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

use std::time::Duration;

use risingwave_common::error::Result;
use risingwave_pb::meta::meta_snapshot::SnapshotVersion;
use risingwave_pb::meta::subscribe_response::Info;
use risingwave_pb::meta::{SubscribeResponse, SubscribeType};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::MetaClient;
use tokio::task::JoinHandle;
use tonic::{Status, Streaming};

pub trait SubscribeTypeEnum {
    fn subscribe_type() -> SubscribeType;
}

pub struct SubscribeFrontend {}
impl SubscribeTypeEnum for SubscribeFrontend {
    fn subscribe_type() -> SubscribeType {
        SubscribeType::Frontend
    }
}

pub struct SubscribeHummock {}
impl SubscribeTypeEnum for SubscribeHummock {
    fn subscribe_type() -> SubscribeType {
        SubscribeType::Hummock
    }
}

pub struct SubscribeCompactor {}
impl SubscribeTypeEnum for SubscribeCompactor {
    fn subscribe_type() -> SubscribeType {
        SubscribeType::Compactor
    }
}

/// `ObserverManager` is used to update data based on notification from meta.
/// Call `start` to spawn a new asynchronous task
/// We can write the notification logic by implementing `ObserverNodeImpl`.
pub struct ObserverManager<T: NotificationClient, S: ObserverState> {
    rx: T::Channel,
    client: T,
    observer_states: S,
}

pub trait ObserverState: Send + 'static {
    type SubscribeType: SubscribeTypeEnum;
    /// modify data after receiving notification from meta
    fn handle_notification(&mut self, resp: SubscribeResponse);

    /// Initialize data from the meta. It will be called at start or resubscribe
    fn handle_initialization_notification(&mut self, resp: SubscribeResponse);
}

impl<S: ObserverState> ObserverManager<RpcNotificationClient, S> {
    pub async fn new_with_meta_client(meta_client: MetaClient, observer_states: S) -> Self {
        let client = RpcNotificationClient { meta_client };
        Self::new(client, observer_states).await
    }
}

impl<T, S> ObserverManager<T, S>
where
    T: NotificationClient,
    S: ObserverState,
{
    pub async fn new(client: T, observer_states: S) -> Self {
        let rx = client
            .subscribe(S::SubscribeType::subscribe_type())
            .await
            .unwrap();
        Self {
            rx,
            client,
            observer_states,
        }
    }

    async fn wait_init_notification(&mut self) {
        let mut notification_vec = Vec::new();
        let init_notification = loop {
            // notification before init notification must be received successfully.
            let notification = self.rx.message().await.unwrap().unwrap();
            if !matches!(notification.info.as_ref().unwrap(), &Info::Snapshot(_)) {
                notification_vec.push(notification);
            } else {
                break notification;
            }
        };

        let Info::Snapshot(info) = init_notification.info.as_ref().unwrap() else {
            unreachable!();
        };

        let SnapshotVersion {
            catalog_version,
            parallel_unit_mapping_version,
            worker_node_version,
        } = info.version.clone().unwrap();

        notification_vec.retain_mut(|notification| match notification.info.as_ref().unwrap() {
            Info::Database(_)
            | Info::Schema(_)
            | Info::Table(_)
            | Info::Source(_)
            | Info::Sink(_)
            | Info::Index(_)
            | Info::View(_)
            | Info::User(_) => notification.version > catalog_version,
            Info::ParallelUnitMapping(_) => notification.version > parallel_unit_mapping_version,
            Info::Node(_) => notification.version > worker_node_version,
            Info::HummockVersionDeltas(version_delta) => {
                version_delta.version_deltas[0].id > info.hummock_version.as_ref().unwrap().id
            }
            Info::HummockSnapshot(_) => true,
            Info::Snapshot(_) => unreachable!(),
        });

        self.observer_states
            .handle_initialization_notification(init_notification);

        for notification in notification_vec {
            self.observer_states.handle_notification(notification);
        }
    }

    /// `start` is used to spawn a new asynchronous task which receives meta's notification and
    /// call the `handle_initialization_notification` and `handle_notification` to update node data.
    pub async fn start(mut self) -> JoinHandle<()> {
        self.wait_init_notification().await;

        tokio::spawn(async move {
            loop {
                match self.rx.message().await {
                    Ok(resp) => {
                        if resp.is_none() {
                            tracing::error!("Stream of notification terminated.");
                            self.re_subscribe().await;
                            continue;
                        }
                        self.observer_states.handle_notification(resp.unwrap());
                    }
                    Err(e) => {
                        tracing::error!("Receives meta's notification err {:?}", e);
                        self.re_subscribe().await;
                    }
                }
            }
        })
    }

    /// `re_subscribe` is used to re-subscribe to the meta's notification.
    async fn re_subscribe(&mut self) {
        loop {
            match self
                .client
                .subscribe(S::SubscribeType::subscribe_type())
                .await
            {
                Ok(rx) => {
                    tracing::debug!("re-subscribe success");
                    self.rx = rx;
                    self.wait_init_notification().await;
                    break;
                }
                Err(_) => {
                    tokio::time::sleep(RE_SUBSCRIBE_RETRY_INTERVAL).await;
                }
            }
        }
    }
}
const RE_SUBSCRIBE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

#[async_trait::async_trait]
pub trait Channel: Send + 'static {
    type Item;
    async fn message(&mut self) -> std::result::Result<Option<Self::Item>, Status>;
}

#[async_trait::async_trait]
impl<T: Send + 'static> Channel for Streaming<T> {
    type Item = T;

    async fn message(&mut self) -> std::result::Result<Option<T>, Status> {
        self.message().await
    }
}

#[async_trait::async_trait]
pub trait NotificationClient: Send + Sync + 'static {
    type Channel: Channel<Item = SubscribeResponse>;
    async fn subscribe(&self, subscribe_type: SubscribeType) -> Result<Self::Channel>;
}

pub struct RpcNotificationClient {
    meta_client: MetaClient,
}

impl RpcNotificationClient {
    pub fn new(meta_client: MetaClient) -> Self {
        Self { meta_client }
    }
}

#[async_trait::async_trait]
impl NotificationClient for RpcNotificationClient {
    type Channel = Streaming<SubscribeResponse>;

    async fn subscribe(&self, subscribe_type: SubscribeType) -> Result<Self::Channel> {
        self.meta_client
            .subscribe(subscribe_type)
            .await
            .map_err(RpcError::into)
    }
}
