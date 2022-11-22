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

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::HashKey;
use risingwave_common::types::Datum;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::hash_util::Crc32FastBuilder;
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_storage::StateStore;

use super::top_n_cache::TopNCacheTrait;
use super::utils::*;
use super::TopNCache;
use crate::cache::{cache_may_stale, EvictableHashMap, ExecutorCache, LruManagerRef};
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::top_n::ManagedTopNState;
use crate::executor::{ActorContextRef, Executor, ExecutorInfo, PkIndices, PkIndicesRef};

pub type GroupTopNExecutor<K, S, const WITH_TIES: bool> =
    TopNExecutorWrapper<InnerGroupTopNExecutorNew<K, S, WITH_TIES>>;

impl<K: HashKey, S: StateStore> GroupTopNExecutor<K, S, false> {
    #[allow(clippy::too_many_arguments)]
    pub fn new_without_ties(
        input: Box<dyn Executor>,
        ctx: ActorContextRef,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, usize),
        order_by_len: usize,
        pk_indices: PkIndices,
        executor_id: u64,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        lru_manager: Option<LruManagerRef>,
        cache_size: usize,
    ) -> StreamResult<Self> {
        let info = input.info();
        let schema = input.schema().clone();
        Ok(TopNExecutorWrapper {
            input,
            ctx,
            inner: InnerGroupTopNExecutorNew::new(
                info,
                schema,
                order_pairs,
                offset_and_limit,
                order_by_len,
                pk_indices,
                executor_id,
                group_by,
                state_table,
                lru_manager,
                cache_size,
            )?,
        })
    }
}

impl<K: HashKey, S: StateStore> GroupTopNExecutor<K, S, true> {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_ties(
        input: Box<dyn Executor>,
        ctx: ActorContextRef,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, usize),
        order_by_len: usize,
        pk_indices: PkIndices,
        executor_id: u64,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        lru_manager: Option<LruManagerRef>,
        cache_size: usize,
    ) -> StreamResult<Self> {
        let info = input.info();
        let schema = input.schema().clone();

        Ok(TopNExecutorWrapper {
            input,
            ctx,
            inner: InnerGroupTopNExecutorNew::new(
                info,
                schema,
                order_pairs,
                offset_and_limit,
                order_by_len,
                pk_indices,
                executor_id,
                group_by,
                state_table,
                lru_manager,
                cache_size,
            )?,
        })
    }
}

pub struct InnerGroupTopNExecutorNew<K: HashKey, S: StateStore, const WITH_TIES: bool> {
    info: ExecutorInfo,

    /// Schema of the executor.
    schema: Schema,

    /// `LIMIT XXX`. None means no limit.
    limit: usize,

    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,

    /// The primary key indices of the `GroupTopNExecutor`
    pk_indices: PkIndices,

    /// The internal key indices of the `GroupTopNExecutor`
    internal_key_indices: PkIndices,

    /// We are interested in which element is in the range of [offset, offset+limit).
    managed_state: ManagedTopNState<S>,

    /// which column we used to group the data.
    group_by: Vec<usize>,

    /// group key -> cache for this group
    caches: GroupTopNCache<K, WITH_TIES>,

    /// The number of fields of the ORDER BY clause, and will be used to split key into `CacheKey`.
    order_by_len: usize,

    /// Used for serializing pk into CacheKey.
    cache_key_serde: (OrderedRowSerde, OrderedRowSerde),
}

impl<K: HashKey, S: StateStore, const WITH_TIES: bool> InnerGroupTopNExecutorNew<K, S, WITH_TIES> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        schema: Schema,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, usize),
        order_by_len: usize,
        pk_indices: PkIndices,
        executor_id: u64,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        lru_manager: Option<LruManagerRef>,
        cache_size: usize,
    ) -> StreamResult<Self> {
        // order_pairs is superset of pk
        assert!(order_pairs
            .iter()
            .map(|x| x.column_idx)
            .collect::<HashSet<_>>()
            .is_superset(&pk_indices.iter().copied().collect::<HashSet<_>>()));
        let (internal_key_indices, internal_key_data_types, internal_key_order_types) =
            generate_executor_pk_indices_info(&order_pairs, &schema);

        let managed_state = ManagedTopNState::<S>::new(
            state_table,
            &internal_key_data_types[group_by.len()..],
            &internal_key_order_types[group_by.len()..],
            order_by_len,
        );
        let (first_key_data_types, second_key_data_types) =
            internal_key_data_types[group_by.len()..].split_at(order_by_len);
        let (first_key_order_types, second_key_order_types) =
            internal_key_order_types[group_by.len()..].split_at(order_by_len);
        let first_key_serde = OrderedRowSerde::new(
            first_key_data_types.to_vec(),
            first_key_order_types.to_vec(),
        );
        let second_key_serde = OrderedRowSerde::new(
            second_key_data_types.to_vec(),
            second_key_order_types.to_vec(),
        );

        let cache_key_serde = (first_key_serde, second_key_serde);
        Ok(Self {
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("TopNExecutorNew {:X}", executor_id),
            },
            schema,
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            managed_state,
            pk_indices,
            internal_key_indices,
            group_by,
            caches: GroupTopNCache::new(lru_manager, cache_size),
            order_by_len,
            cache_key_serde,
        })
    }
}

pub struct GroupTopNCache<K: HashKey, const WITH_TIES: bool> {
    data: ExecutorCache<K, TopNCache<WITH_TIES>>,
}

impl<K: HashKey, const WITH_TIES: bool> GroupTopNCache<K, WITH_TIES> {
    pub fn new(lru_manager: Option<LruManagerRef>, cache_size: usize) -> Self {
        let cache = if let Some(lru_manager) = lru_manager {
            ExecutorCache::Managed(lru_manager.create_cache())
        } else {
            ExecutorCache::Local(EvictableHashMap::new(cache_size))
        };
        Self { data: cache }
    }

    fn clear(&mut self) {
        self.data.clear()
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut TopNCache<WITH_TIES>> {
        self.data.get_mut(key)
    }

    fn contains(&mut self, key: &K) -> bool {
        self.data.contains(key)
    }

    fn insert(&mut self, key: K, value: TopNCache<WITH_TIES>) {
        self.data.push(key, value);
    }

    fn evict(&mut self) {
        self.data.evict()
    }
}
#[async_trait]
impl<K: HashKey, S: StateStore, const WITH_TIES: bool> TopNExecutorBase
    for InnerGroupTopNExecutorNew<K, S, WITH_TIES>
where
    TopNCache<WITH_TIES>: TopNCacheTrait,
{
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<StreamChunk> {
        let mut res_ops = Vec::with_capacity(self.limit);
        let mut res_rows = Vec::with_capacity(self.limit);

        println!("这里");
        let hash_codes = chunk
            .data_chunk()
            .get_hash_values(&self.group_by, Crc32FastBuilder);
        let keys = K::build_from_hash_code(&self.group_by, chunk.data_chunk(), hash_codes.clone());
        for ((op, row_ref), group_cache_key) in chunk.rows().zip(keys) {
            // The pk without group by
            let pk_row = row_ref.row_by_indices(&self.internal_key_indices[self.group_by.len()..]);
            let cache_key =
                serialize_pk_to_cache_key(pk_row, self.order_by_len, &self.cache_key_serde);

            let group_key = row_ref.row_by_indices(&self.group_by);
            let pk_prefix = group_key.clone();

            // If 'self.caches' does not already have a cache for the current group, create a new
            // cache for it and insert it into `self.caches`
            if !self.caches.contains(&group_cache_key) {
                let mut topn_cache = TopNCache::new(self.offset, self.limit, self.order_by_len);
                self.managed_state
                    .init_topn_cache(Some(&pk_prefix), &mut topn_cache, self.order_by_len)
                    .await?;
                self.caches.insert(group_cache_key.clone(), topn_cache);
            }
            let cache = self.caches.get_mut(&group_cache_key).unwrap();

            // apply the chunk to state table
            match op {
                Op::Insert | Op::UpdateInsert => {
                    self.managed_state.insert(row_ref.clone());
                    cache.insert(cache_key, row_ref, &mut res_ops, &mut res_rows);
                }

                Op::Delete | Op::UpdateDelete => {
                    self.managed_state.delete(row_ref.clone());
                    cache
                        .delete(
                            Some(&pk_prefix),
                            &mut self.managed_state,
                            cache_key,
                            row_ref,
                            &mut res_ops,
                            &mut res_rows,
                        )
                        .await?;
                }
            }
        }

        generate_output(res_rows, res_ops, &self.schema)
    }

    async fn flush_data(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.flush(epoch).await
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }

    fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) {
        let previous_vnode_bitmap = self
            .managed_state
            .state_table
            .update_vnode_bitmap(vnode_bitmap.clone());

        if cache_may_stale(&previous_vnode_bitmap, &vnode_bitmap) {
            self.caches.clear();
        }
    }

    fn evict(&mut self) {
        self.caches.evict()
    }

    async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.state_table.init_epoch(epoch);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::Field;
    use risingwave_common::hash::SerializedKey;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{ActorContext, Barrier, Message};

    fn create_schema() -> Schema {
        Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        }
    }

    fn create_order_pairs() -> Vec<OrderPair> {
        vec![
            OrderPair::new(1, OrderType::Ascending),
            OrderPair::new(2, OrderType::Ascending),
            OrderPair::new(0, OrderType::Ascending),
        ]
    }

    fn create_stream_chunks() -> Vec<StreamChunk> {
        let chunk0 = StreamChunk::from_pretty(
            "  I I I
            + 10 9 1
            +  8 8 2
            +  7 8 2
            +  9 1 1
            + 10 1 1
            +  8 1 3",
        );
        let chunk1 = StreamChunk::from_pretty(
            "  I I I
            - 10 9 1
            -  8 8 2
            - 10 1 1",
        );
        let chunk2 = StreamChunk::from_pretty(
            " I I I
            - 7 8 2
            - 8 1 3
            - 9 1 1",
        );
        let chunk3 = StreamChunk::from_pretty(
            "  I I I
            +  5 1 1
            +  2 1 1
            +  3 1 2
            +  4 1 3",
        );
        vec![chunk0, chunk1, chunk2, chunk3]
    }

    fn create_source() -> Box<MockSource> {
        let mut chunks = create_stream_chunks();
        let schema = create_schema();
        Box::new(MockSource::with_messages(
            schema,
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Barrier(Barrier::new_test_barrier(2)),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Barrier(Barrier::new_test_barrier(3)),
                Message::Chunk(std::mem::take(&mut chunks[2])),
                Message::Barrier(Barrier::new_test_barrier(4)),
                Message::Chunk(std::mem::take(&mut chunks[3])),
                Message::Barrier(Barrier::new_test_barrier(5)),
            ],
        ))
    }

    #[tokio::test]
    async fn test_without_offset_and_with_limits() {
        let order_types = create_order_pairs();
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::Ascending,
                OrderType::Ascending,
                OrderType::Ascending,
            ],
            &[1, 2, 0],
        )
        .await;
        let top_n_executor = Box::new(
            GroupTopNExecutor::<SerializedKey, MemoryStateStore, false>::new_without_ties(
                source as Box<dyn Executor>,
                ActorContext::create(0),
                order_types,
                (0, 2),
                1,
                vec![1, 2, 0],
                1,
                vec![1],
                state_table,
                None,
                0,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                + 10 9 1
                +  8 8 2
                +  7 8 2
                +  9 1 1
                + 10 1 1
                ",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                - 10 9 1
                -  8 8 2
                - 10 1 1
                +  8 1 3
                ",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                " I I I
                - 7 8 2
                - 8 1 3
                - 9 1 1
                ",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                " I I I
                + 5 1 1
                + 2 1 1
                ",
            ),
        );
    }

    #[tokio::test]
    async fn test_with_offset_and_with_limits() {
        let order_types = create_order_pairs();
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::Ascending,
                OrderType::Ascending,
                OrderType::Ascending,
            ],
            &[1, 2, 0],
        )
        .await;
        let top_n_executor = Box::new(
            GroupTopNExecutor::<SerializedKey, MemoryStateStore, false>::new_without_ties(
                source as Box<dyn Executor>,
                ActorContext::create(0),
                order_types,
                (1, 2),
                1,
                vec![1, 2, 0],
                1,
                vec![1],
                state_table,
                None,
                0,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                +  8 8 2
                + 10 1 1
                +  8 1 3
                ",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                -  8 8 2
                - 10 1 1
                ",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                " I I I
                - 8 1 3",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                " I I I
                + 5 1 1
                + 3 1 2
                ",
            ),
        );
    }
    #[tokio::test]
    async fn test_multi_group_key() {
        let order_types = create_order_pairs();
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::Ascending,
                OrderType::Ascending,
                OrderType::Ascending,
            ],
            &[1, 2, 0],
        )
        .await;
        let top_n_executor = Box::new(
            GroupTopNExecutor::<SerializedKey, MemoryStateStore, false>::new_without_ties(
                source as Box<dyn Executor>,
                ActorContext::create(0),
                order_types,
                (0, 2),
                1,
                vec![1, 2, 0],
                1,
                vec![1, 2],
                state_table,
                None,
                0,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                + 10 9 1
                +  8 8 2
                +  7 8 2
                +  9 1 1
                + 10 1 1
                +  8 1 3",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                - 10 9 1
                -  8 8 2
                - 10 1 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                - 7 8 2
                - 8 1 3
                - 9 1 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                +  5 1 1
                +  2 1 1
                +  3 1 2
                +  4 1 3",
            ),
        );
    }
}
