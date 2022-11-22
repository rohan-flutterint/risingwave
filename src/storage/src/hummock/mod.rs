// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Hummock is the state store of the streaming system.

use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use risingwave_hummock_sdk::{HummockEpoch, *};
#[cfg(any(test, feature = "test"))]
use risingwave_pb::hummock::HummockVersion;
use risingwave_pb::hummock::{pin_version_response, SstableInfo};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::watch;
use tracing::log::error;

mod block_cache;
pub use block_cache::*;

#[cfg(target_os = "linux")]
pub mod file_cache;

mod tiered_cache;
pub use tiered_cache::*;

pub mod sstable;
pub use sstable::*;

pub mod compaction_group_client;
pub mod compactor;
pub mod conflict_detector;
mod error;
pub mod hummock_meta_client;
pub mod iterator;
pub mod shared_buffer;
pub mod sstable_store;
mod state_store;
mod state_store_v1;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
pub mod utils;
pub use compactor::{CompactorMemoryCollector, CompactorSstableStore};
pub use utils::MemoryLimiter;
pub mod event_handler;
pub mod local_version;
pub mod observer_manager;
pub mod store;
pub mod vacuum;
mod validator;
pub mod value;

pub use error::*;
use local_version::local_version_manager::{LocalVersionManager, LocalVersionManagerRef};
pub use risingwave_common::cache::{CacheableEntry, LookupResult, LruCache};
use risingwave_common_service::observer_manager::{NotificationClient, ObserverManager};
use risingwave_hummock_sdk::filter_key_extractor::{
    FilterKeyExtractorManager, FilterKeyExtractorManagerRef,
};
pub use validator::*;
use value::*;

use self::iterator::{BackwardUserIterator, HummockIterator, UserIterator};
pub use self::sstable_store::*;
use super::monitor::StateStoreMetrics;
use crate::error::StorageResult;
use crate::hummock::compactor::Context;
use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
use crate::hummock::event_handler::{HummockEvent, HummockEventHandler};
use crate::hummock::iterator::{
    Backward, BackwardUserIteratorType, DirectedUserIteratorBuilder, DirectionEnum, Forward,
    ForwardUserIteratorType, HummockIteratorDirection,
};
use crate::hummock::local_version::pinned_version::{start_pinned_version_worker, PinnedVersion};
use crate::hummock::observer_manager::HummockObserverNode;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::shared_buffer::{OrderSortedUncommittedData, UncommittedData};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::{SstableStoreRef, TableHolder};
use crate::hummock::store::version::{HummockReadVersion, HummockVersionReader};
use crate::monitor::StoreLocalStatistic;
use crate::store::ReadOptions;

struct HummockStorageShutdownGuard {
    shutdown_sender: UnboundedSender<HummockEvent>,
}

impl Drop for HummockStorageShutdownGuard {
    fn drop(&mut self) {
        let _ = self
            .shutdown_sender
            .send(HummockEvent::Shutdown)
            .inspect_err(|e| error!("unable to send shutdown: {:?}", e));
    }
}

/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    hummock_event_sender: UnboundedSender<HummockEvent>,

    // TODO: replace it with version mapping
    read_version: Arc<RwLock<HummockReadVersion>>,

    context: Arc<Context>,

    buffer_tracker: BufferTracker,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,

    seal_epoch: Arc<AtomicU64>,

    pinned_version: Arc<ArcSwap<PinnedVersion>>,

    hummock_version_reader: HummockVersionReader,

    _shutdown_guard: Arc<HummockStorageShutdownGuard>,

    #[cfg(not(madsim))]
    tracing: Arc<risingwave_tracing::RwTracingService>,
}

impl HummockStorage {
    /// Creates a [`HummockStorage`].
    pub async fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
    ) -> HummockResult<Self> {
        let sstable_id_manager = Arc::new(SstableIdManager::new(
            hummock_meta_client.clone(),
            options.sstable_id_remote_fetch_number,
        ));

        let filter_key_extractor_manager = Arc::new(FilterKeyExtractorManager::default());
        let (event_tx, mut event_rx) = unbounded_channel();

        let observer_manager = ObserverManager::new(
            notification_client,
            HummockObserverNode::new(filter_key_extractor_manager.clone(), event_tx.clone()),
        )
        .await;
        let _ = observer_manager.start().await;

        let hummock_version = match event_rx.recv().await {
            Some(HummockEvent::VersionUpdate(pin_version_response::Payload::PinnedVersion(version))) => version,
            _ => unreachable!("the hummock observer manager is the first one to take the event tx. Should be full hummock version")
        };

        let (pin_version_tx, pin_version_rx) = unbounded_channel();
        let pinned_version = PinnedVersion::new(hummock_version, pin_version_tx);
        tokio::spawn(start_pinned_version_worker(
            pin_version_rx,
            hummock_meta_client.clone(),
        ));

        let compactor_context = Arc::new(Context::new_local_compact_context(
            options.clone(),
            sstable_store.clone(),
            hummock_meta_client.clone(),
            stats.clone(),
            sstable_id_manager.clone(),
            filter_key_extractor_manager.clone(),
        ));

        let hummock_event_handler =
            HummockEventHandler::new(event_rx, pinned_version, compactor_context.clone());

        let read_version = hummock_event_handler.read_version();

        #[cfg(not(madsim))]
        let tracing = Arc::new(risingwave_tracing::RwTracingService::new());

        let instance = Self {
            read_version,
            context: compactor_context,
            buffer_tracker: hummock_event_handler.buffer_tracker().clone(),
            version_update_notifier_tx: hummock_event_handler.version_update_notifier_tx(),
            seal_epoch: hummock_event_handler.sealed_epoch(),
            hummock_event_sender: event_tx.clone(),
            pinned_version: hummock_event_handler.pinned_version(),
            hummock_version_reader: HummockVersionReader::new(sstable_store, stats.clone()),
            _shutdown_guard: Arc::new(HummockStorageShutdownGuard {
                shutdown_sender: event_tx,
            }),
            #[cfg(not(madsim))]
            tracing,
        };

        tokio::spawn(hummock_event_handler.start_hummock_event_handler_worker());

        Ok(instance)
    }

    pub fn sstable_store(&self) -> SstableStoreRef {
        self.context.sstable_store.clone()
    }

    pub fn sstable_id_manager(&self) -> &SstableIdManagerRef {
        &self.context.sstable_id_manager
    }

    pub fn filter_key_extractor_manager(&self) -> &FilterKeyExtractorManagerRef {
        &self.context.filter_key_extractor_manager
    }

    pub fn get_memory_limiter(&self) -> Arc<MemoryLimiter> {
        self.buffer_tracker.get_memory_limiter().clone()
    }

    pub fn get_pinned_version(&self) -> PinnedVersion {
        self.pinned_version.load().deref().deref().clone()
    }
}

#[cfg(any(test, feature = "test"))]
impl HummockStorage {
    /// Used in the compaction test tool
    pub async fn update_version_and_wait(&self, version: HummockVersion) {
        use tokio::task::yield_now;
        let version_id = version.id;
        self.hummock_event_sender
            .send(HummockEvent::VersionUpdate(
                pin_version_response::Payload::PinnedVersion(version),
            ))
            .unwrap();

        loop {
            if self.pinned_version.load().id() >= version_id {
                break;
            }
            yield_now().await
        }
    }

    pub async fn wait_version(&self, version: HummockVersion) {
        use tokio::task::yield_now;
        loop {
            if self.pinned_version.load().id() >= version.id {
                break;
            }
            yield_now().await
        }
    }

    pub fn get_shared_buffer_size(&self) -> usize {
        self.buffer_tracker.get_buffer_size()
    }

    /// Creates a [`HummockStorage`] with default stats. Should only be used by tests.
    pub async fn for_test(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
    ) -> HummockResult<Self> {
        Self::new(
            options,
            sstable_store,
            hummock_meta_client,
            notification_client,
            Arc::new(StateStoreMetrics::unused()),
        )
        .await
    }

    pub fn options(&self) -> &Arc<StorageConfig> {
        &self.context.options
    }
}

pub async fn get_from_sstable_info(
    sstable_store_ref: SstableStoreRef,
    sstable_info: &SstableInfo,
    full_key: FullKey<&[u8]>,
    read_options: &ReadOptions,
    local_stats: &mut StoreLocalStatistic,
) -> HummockResult<Option<HummockValue<Bytes>>> {
    let sstable = sstable_store_ref.sstable(sstable_info, local_stats).await?;

    let ukey = &full_key.user_key;
    let delete_epoch = if read_options.ignore_range_tombstone {
        None
    } else {
        get_delete_range_epoch_from_sstable(sstable.value().as_ref(), &full_key)
    };
    if read_options.check_bloom_filter
        && !hit_sstable_bloom_filter(sstable.value(), ukey.encode().as_slice(), local_stats)
    {
        if delete_epoch.is_some() {
            return Ok(Some(HummockValue::Delete));
        }
        return Ok(None);
    }

    // TODO: now SstableIterator does not use prefetch through SstableIteratorReadOptions, so we
    // use default before refinement.
    let mut iter = SstableIterator::create(
        sstable,
        sstable_store_ref.clone(),
        Arc::new(SstableIteratorReadOptions::default()),
    );
    iter.seek(full_key).await?;
    // Iterator has sought passed the borders.
    if !iter.is_valid() {
        if delete_epoch.is_some() {
            return Ok(Some(HummockValue::Delete));
        }
        return Ok(None);
    }

    // Iterator gets us the key, we tell if it's the key we want
    // or key next to it.
    let value = if iter.key().user_key == *ukey {
        if delete_epoch
            .map(|epoch| epoch >= iter.key().epoch)
            .unwrap_or(false)
        {
            Some(HummockValue::Delete)
        } else {
            Some(iter.value().to_bytes())
        }
    } else if delete_epoch.is_some() {
        Some(HummockValue::Delete)
    } else {
        None
    };
    iter.collect_local_statistic(local_stats);

    Ok(value)
}

pub fn hit_sstable_bloom_filter(
    sstable_info_ref: &Sstable,
    user_key: &[u8],
    local_stats: &mut StoreLocalStatistic,
) -> bool {
    local_stats.bloom_filter_check_counts += 1;
    let surely_not_have = sstable_info_ref.surely_not_have_user_key(user_key);

    if surely_not_have {
        local_stats.bloom_filter_true_negative_count += 1;
    }

    !surely_not_have
}

/// Get `user_value` from `OrderSortedUncommittedData`. If not get successful, return None.
pub async fn get_from_order_sorted_uncommitted_data(
    sstable_store_ref: SstableStoreRef,
    order_sorted_uncommitted_data: OrderSortedUncommittedData,
    full_key: FullKey<&[u8]>,
    local_stats: &mut StoreLocalStatistic,
    read_options: &ReadOptions,
) -> StorageResult<(Option<HummockValue<Bytes>>, i32)> {
    let mut table_counts = 0;
    let epoch = full_key.epoch;
    for data_list in order_sorted_uncommitted_data {
        for data in data_list {
            match data {
                UncommittedData::Batch(batch) => {
                    assert!(batch.epoch() <= epoch, "batch'epoch greater than epoch");
                    if let Some(data) =
                        get_from_batch(&batch, full_key.user_key.table_key, local_stats)
                    {
                        return Ok((Some(data), table_counts));
                    }
                }

                UncommittedData::Sst(LocalSstableInfo { sst_info, .. }) => {
                    table_counts += 1;

                    if let Some(data) = get_from_sstable_info(
                        sstable_store_ref.clone(),
                        &sst_info,
                        full_key,
                        read_options,
                        local_stats,
                    )
                    .await?
                    {
                        return Ok((Some(data), table_counts));
                    }
                }
            }
        }
    }
    Ok((None, table_counts))
}

/// Get `user_value` from `SharedBufferBatch`
pub fn get_from_batch(
    batch: &SharedBufferBatch,
    table_key: TableKey<&[u8]>,
    local_stats: &mut StoreLocalStatistic,
) -> Option<HummockValue<Bytes>> {
    if batch.check_delete_by_range(table_key) {
        return Some(HummockValue::Delete);
    }
    batch.get(table_key).map(|v| {
        local_stats.get_shared_buffer_hit_counts += 1;
        v
    })
}

#[derive(Clone)]
pub struct HummockStorageV1 {
    options: Arc<StorageConfig>,

    local_version_manager: LocalVersionManagerRef,

    sstable_store: SstableStoreRef,

    /// Statistics
    stats: Arc<StateStoreMetrics>,

    sstable_id_manager: SstableIdManagerRef,

    filter_key_extractor_manager: FilterKeyExtractorManagerRef,

    hummock_event_sender: UnboundedSender<HummockEvent>,

    _shutdown_guard: Arc<HummockStorageShutdownGuard>,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,

    #[cfg(not(madsim))]
    tracing: Arc<risingwave_tracing::RwTracingService>,
}

impl HummockStorageV1 {
    /// Creates a [`HummockStorageV1`].
    pub async fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
    ) -> HummockResult<Self> {
        // For conflict key detection. Enabled by setting `write_conflict_detection_enabled` to
        // true in `StorageConfig`
        let sstable_id_manager = Arc::new(SstableIdManager::new(
            hummock_meta_client.clone(),
            options.sstable_id_remote_fetch_number,
        ));

        let filter_key_extractor_manager = Arc::new(FilterKeyExtractorManager::default());
        let (event_tx, mut event_rx) = unbounded_channel();

        let observer_manager = ObserverManager::new(
            notification_client,
            HummockObserverNode::new(filter_key_extractor_manager.clone(), event_tx.clone()),
        )
        .await;
        let _ = observer_manager.start().await;

        let hummock_version = match event_rx.recv().await {
            Some(HummockEvent::VersionUpdate(pin_version_response::Payload::PinnedVersion(version))) => version,
            _ => unreachable!("the hummock observer manager is the first one to take the event tx. Should be full hummock version")
        };

        let (pin_version_tx, pin_version_rx) = unbounded_channel();
        let pinned_version = PinnedVersion::new(hummock_version, pin_version_tx);
        tokio::spawn(start_pinned_version_worker(
            pin_version_rx,
            hummock_meta_client.clone(),
        ));

        let compactor_context = Arc::new(Context::new_local_compact_context(
            options.clone(),
            sstable_store.clone(),
            hummock_meta_client.clone(),
            stats.clone(),
            sstable_id_manager.clone(),
            filter_key_extractor_manager.clone(),
        ));

        let buffer_tracker = BufferTracker::from_storage_config(&options);

        let local_version_manager =
            LocalVersionManager::new(pinned_version.clone(), compactor_context, buffer_tracker);

        let local_version_manager_clone = local_version_manager.clone();
        let (epoch_update_tx, _) = watch::channel(pinned_version.max_committed_epoch());
        let epoch_update_tx = Arc::new(epoch_update_tx);
        let epoch_update_tx_clone = epoch_update_tx.clone();

        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                match event {
                    HummockEvent::Shutdown => {
                        break;
                    }
                    HummockEvent::VersionUpdate(version_update) => {
                        local_version_manager_clone.try_update_pinned_version(version_update);
                        // TODO: this is
                        epoch_update_tx.send_replace(
                            local_version_manager_clone
                                .get_pinned_version()
                                .max_committed_epoch(),
                        );
                    }
                    _ => {
                        unreachable!("for hummock v1, there should only be shutdown and version update event");
                    }
                }
            }
        });

        let instance = Self {
            options,
            local_version_manager,
            sstable_store,
            stats,
            sstable_id_manager,
            filter_key_extractor_manager,
            _shutdown_guard: Arc::new(HummockStorageShutdownGuard {
                shutdown_sender: event_tx.clone(),
            }),
            version_update_notifier_tx: epoch_update_tx_clone,
            hummock_event_sender: event_tx,
            #[cfg(not(madsim))]
            tracing: Arc::new(risingwave_tracing::RwTracingService::new()),
        };

        Ok(instance)
    }

    pub fn options(&self) -> &Arc<StorageConfig> {
        &self.options
    }

    pub fn sstable_store(&self) -> SstableStoreRef {
        self.sstable_store.clone()
    }

    pub fn sstable_id_manager(&self) -> &SstableIdManagerRef {
        &self.sstable_id_manager
    }

    pub fn filter_key_extractor_manager(&self) -> &FilterKeyExtractorManagerRef {
        &self.filter_key_extractor_manager
    }

    pub fn get_memory_limiter(&self) -> Arc<MemoryLimiter> {
        self.local_version_manager
            .buffer_tracker()
            .get_memory_limiter()
            .clone()
    }

    pub fn get_pinned_version(&self) -> PinnedVersion {
        self.local_version_manager.get_pinned_version()
    }
}

pub(crate) trait HummockIteratorType: 'static {
    type Direction: HummockIteratorDirection;
    type SstableIteratorType: SstableIteratorType<Direction = Self::Direction>;
    type UserIteratorBuilder: DirectedUserIteratorBuilder<
        Direction = Self::Direction,
        SstableIteratorType = Self::SstableIteratorType,
    >;

    fn direction() -> DirectionEnum {
        Self::Direction::direction()
    }
}

pub(crate) struct ForwardIter;
pub(crate) struct BackwardIter;

impl HummockIteratorType for ForwardIter {
    type Direction = Forward;
    type SstableIteratorType = SstableIterator;
    type UserIteratorBuilder = UserIterator<ForwardUserIteratorType>;
}

impl HummockIteratorType for BackwardIter {
    type Direction = Backward;
    type SstableIteratorType = BackwardSstableIterator;
    type UserIteratorBuilder = BackwardUserIterator<BackwardUserIteratorType>;
}
