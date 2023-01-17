// Copyright 2023 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Formatter;

use anyhow::anyhow;
use either::Either;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::{ColumnId, TableId};
use risingwave_connector::source::{ConnectorState, SplitId, SplitMetaData};
use risingwave_source::connector_source::{SourceContext, SourceDescBuilderV2, SourceDescV2};
use risingwave_source::{BoxSourceWithStateStream, StreamChunkWithState};
use risingwave_storage::StateStore;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;

use super::SourceStateTableHandler;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::source::reader::SourceReaderStream;
use crate::executor::*;

/// A constant to multiply when calculating the maximum time to wait for a barrier. This is due to
/// some latencies in network and cost in meta.
const WAIT_BARRIER_MULTIPLE_TIMES: u128 = 5;

/// [`StreamSourceCore`] stores the necessary information for the source executor to execute on the
/// external connector.
pub struct StreamSourceCore<S: StateStore> {
    source_id: TableId,
    source_name: String,

    column_ids: Vec<ColumnId>,

    source_identify: String,

    /// `source_desc_builder` will be taken (`mem::take`) on execution. A `SourceDesc` (currently
    /// named `SourceDescV2`) will be constructed and used for execution.
    source_desc_builder: Option<SourceDescBuilderV2>,

    /// Split info for stream source. A source executor might read data from several splits of
    /// external connector.
    stream_source_splits: HashMap<SplitId, SplitImpl>,

    /// Stores information of the splits.
    split_state_store: SourceStateTableHandler<S>,

    /// In-memory cache for the splits.
    state_cache: HashMap<SplitId, SplitImpl>,
}

impl<S> StreamSourceCore<S>
where
    S: StateStore,
{
    pub fn new(
        source_id: TableId,
        source_name: String,
        column_ids: Vec<ColumnId>,
        source_desc_builder: SourceDescBuilderV2,
        split_state_store: SourceStateTableHandler<S>,
    ) -> Self {
        Self {
            source_id,
            source_name,
            column_ids,
            source_identify: "Table_".to_string() + &source_id.table_id().to_string(),
            source_desc_builder: Some(source_desc_builder),
            stream_source_splits: HashMap::new(),
            split_state_store,
            state_cache: HashMap::new(),
        }
    }
}

pub struct SourceExecutorV2<S: StateStore> {
    ctx: ActorContextRef,

    identity: String,

    schema: Schema,

    pk_indices: PkIndices,

    /// Streaming source  for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// Expected barrier latency.
    expected_barrier_latency_ms: u64,
}

impl<S: StateStore> SourceExecutorV2<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        schema: Schema,
        pk_indices: PkIndices,
        stream_source_core: Option<StreamSourceCore<S>>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        expected_barrier_latency_ms: u64,
        executor_id: u64,
    ) -> Self {
        Self {
            ctx,
            identity: format!("SourceExecutor {:X}", executor_id),
            schema,
            pk_indices,
            stream_source_core,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            expected_barrier_latency_ms,
        }
    }

    async fn build_stream_source_reader(
        &self,
        source_desc: &SourceDescV2,
        state: ConnectorState,
    ) -> StreamExecutorResult<BoxSourceWithStateStream<StreamChunkWithState>> {
        let column_ids = source_desc
            .columns
            .iter()
            .map(|column_desc| column_desc.column_id)
            .collect_vec();
        Ok(source_desc
            .source
            .stream_reader(
                state,
                column_ids,
                source_desc.metrics.clone(),
                SourceContext::new(
                    self.ctx.id,
                    self.stream_source_core.as_ref().unwrap().source_id,
                ),
            )
            .await
            .map_err(StreamExecutorError::connector_error)?
            .into_stream())
    }

    async fn apply_split_change(
        &mut self,
        source_desc: &SourceDescV2,
        stream: &mut SourceReaderStream<StreamChunkWithState>,
        mapping: &HashMap<ActorId, Vec<SplitImpl>>,
    ) -> StreamExecutorResult<()> {
        if let Some(target_splits) = mapping.get(&self.ctx.id).cloned() {
            if let Some(target_state) = self.get_diff(Some(target_splits)).await? {
                tracing::info!(
                    actor_id = self.ctx.id,
                    state = ?target_state,
                    "apply split change"
                );

                self.replace_stream_reader_with_target_state(source_desc, stream, target_state)
                    .await?;
            }
        }

        Ok(())
    }

    // Note: `get_diff` will modify `state_cache`
    // `rhs` can not be None because we do not support split number reduction
    async fn get_diff(&mut self, rhs: ConnectorState) -> StreamExecutorResult<ConnectorState> {
        let core = self.stream_source_core.as_mut().unwrap();

        let split_change = rhs.unwrap();
        let mut target_state: Vec<SplitImpl> = Vec::with_capacity(split_change.len());
        let mut no_change_flag = true;
        for sc in &split_change {
            if let Some(s) = core.state_cache.get(&sc.id()) {
                target_state.push(s.clone())
            } else {
                no_change_flag = false;
                // write new assigned split to state cache. snapshot is base on cache.

                let state = if let Some(recover_state) = core
                    .split_state_store
                    .try_recover_from_state_store(sc)
                    .await?
                {
                    recover_state
                } else {
                    sc.clone()
                };

                core.state_cache
                    .entry(sc.id())
                    .or_insert_with(|| state.clone());
                target_state.push(state);
            }
        }

        Ok((!no_change_flag).then_some(target_state))
    }

    async fn replace_stream_reader_with_target_state(
        &mut self,
        source_desc: &SourceDescV2,
        stream: &mut SourceReaderStream<StreamChunkWithState>,
        target_state: Vec<SplitImpl>,
    ) -> StreamExecutorResult<()> {
        tracing::info!(
            "actor {:?} apply source split change to {:?}",
            self.ctx.id,
            target_state
        );

        // Replace the source reader with a new one of the new state.
        let reader = self
            .build_stream_source_reader(source_desc, Some(target_state.clone()))
            .await?;
        stream.replace_source_stream(reader);

        self.stream_source_core
            .as_mut()
            .unwrap()
            .stream_source_splits = target_state
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        Ok(())
    }

    async fn take_snapshot_and_clear_cache(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<()> {
        let core = self.stream_source_core.as_mut().unwrap();

        let cache = core
            .state_cache
            .values()
            .map(|split_impl| split_impl.to_owned())
            .collect_vec();

        if !cache.is_empty() {
            tracing::debug!(actor_id = self.ctx.id, state = ?cache, "take snapshot");
            core.split_state_store.take_snapshot(cache).await?
        }
        // commit anyway, even if no message saved
        core.split_state_store.state_store.commit(epoch).await?;

        core.state_cache.clear();

        Ok(())
    }

    /// A source executor with a stream source receives:
    /// 1. Barrier messages
    /// 2. Data from external source
    /// and acts accordingly.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_with_stream_source(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .stack_trace("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                StreamExecutorError::from(anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.ctx.id,
                    self.stream_source_core.as_ref().unwrap().source_id
                ))
            })?;

        let mut core = self.stream_source_core.unwrap();

        // Build source description from the builder.
        let source_desc = core
            .source_desc_builder
            .take()
            .unwrap()
            .build()
            .await
            .map_err(StreamExecutorError::connector_error)?;

        let mut boot_state = Vec::default();
        if let Some(mutation) = barrier.mutation.as_ref() {
            match mutation.as_ref() {
                Mutation::Add { splits, .. }
                | Mutation::Update {
                    actor_splits: splits,
                    ..
                } => {
                    if let Some(splits) = splits.get(&self.ctx.id) {
                        boot_state = splits.clone();
                    }
                }
                _ => {}
            }
        }

        core.split_state_store.init_epoch(barrier.epoch);

        core.stream_source_splits = boot_state
            .clone()
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        for ele in &mut boot_state {
            if let Some(recover_state) = core
                .split_state_store
                .try_recover_from_state_store(ele)
                .await?
            {
                *ele = recover_state;
            }
        }

        // Return the ownership of `stream_source_core` to the source executor.
        self.stream_source_core = Some(core);

        let recover_state: ConnectorState = (!boot_state.is_empty()).then_some(boot_state);
        tracing::info!(actor_id = self.ctx.id, state = ?recover_state, "start with state");

        let source_chunk_reader = self
            .build_stream_source_reader(&source_desc, recover_state)
            .stack_trace("source_build_reader")
            .await?;

        // Merge the chunks from source and the barriers into a single stream.
        let mut stream = SourceReaderStream::new(barrier_receiver, source_chunk_reader);

        // If the first barrier is configuration change, then the source executor must be newly
        // created, and we should start with the paused state.
        if barrier.is_update() {
            stream.pause_source();
        }

        yield Message::Barrier(barrier);

        // We allow data to flow for `WAIT_BARRIER_MULTIPLE_TIMES` * `expected_barrier_latency_ms`
        // milliseconds, considering some other latencies like network and cost in Meta.
        let max_wait_barrier_time_ms =
            self.expected_barrier_latency_ms as u128 * WAIT_BARRIER_MULTIPLE_TIMES;
        let mut last_barrier_time = Instant::now();
        let mut self_paused = false;
        while let Some(msg) = stream.next().await {
            match msg? {
                // This branch will be preferred.
                Either::Left(barrier) => {
                    last_barrier_time = Instant::now();
                    if self_paused {
                        stream.resume_source();
                        self_paused = false;
                    }
                    let epoch = barrier.epoch;

                    if let Some(ref mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::SourceChangeSplit(actor_splits) => {
                                self.apply_split_change(&source_desc, &mut stream, actor_splits)
                                    .await?
                            }
                            Mutation::Pause => stream.pause_source(),
                            Mutation::Resume => stream.resume_source(),
                            Mutation::Update { actor_splits, .. } => {
                                self.apply_split_change(&source_desc, &mut stream, actor_splits)
                                    .await?;
                            }
                            _ => {}
                        }
                    }

                    self.take_snapshot_and_clear_cache(epoch).await?;

                    yield Message::Barrier(barrier);
                }

                Either::Right(StreamChunkWithState {
                    chunk,
                    split_offset_mapping,
                }) => {
                    if last_barrier_time.elapsed().as_millis() > max_wait_barrier_time_ms {
                        // Exceeds the max wait barrier time, the source will be paused. Currently
                        // we can guarantee the source is not paused since it received stream
                        // chunks.
                        self_paused = true;
                        stream.pause_source();
                    }
                    if let Some(mapping) = split_offset_mapping {
                        let state: HashMap<_, _> = mapping
                            .iter()
                            .flat_map(|(split_id, offset)| {
                                let origin_split_impl = self
                                    .stream_source_core
                                    .as_ref()
                                    .unwrap()
                                    .stream_source_splits
                                    .get(split_id);

                                origin_split_impl.map(|split_impl| {
                                    (split_id.clone(), split_impl.update(offset.clone()))
                                })
                            })
                            .collect();

                        self.stream_source_core
                            .as_mut()
                            .unwrap()
                            .state_cache
                            .extend(state);
                    }

                    self.metrics
                        .source_output_row_count
                        .with_label_values(&[
                            self.stream_source_core
                                .as_ref()
                                .unwrap()
                                .source_identify
                                .as_str(),
                            self.stream_source_core
                                .as_ref()
                                .unwrap()
                                .source_name
                                .as_ref(),
                        ])
                        .inc_by(chunk.cardinality() as u64);
                    yield Message::Chunk(chunk);
                }
            }
        }

        // The source executor should only be stopped by the actor when finding a `Stop` mutation.
        tracing::error!(
            actor_id = self.ctx.id,
            "source executor exited unexpectedly"
        )
    }

    /// A source executor without stream source only receives barrier messages and sends them to
    /// the downstream executor.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_without_stream_source(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .stack_trace("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                StreamExecutorError::from(anyhow!(
                    "failed to receive the first barrier, actor_id: {:?} with no stream source",
                    self.ctx.id
                ))
            })?;
        yield Message::Barrier(barrier);

        while let Some(barrier) = barrier_receiver.recv().await {
            yield Message::Barrier(barrier);
        }
    }
}

impl<S: StateStore> Executor for SourceExecutorV2<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        if self.stream_source_core.is_some() {
            self.execute_with_stream_source().boxed()
        } else {
            self.execute_without_stream_source().boxed()
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}

impl<S: StateStore> Debug for SourceExecutorV2<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("SourceExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .field("pk_indices", &self.pk_indices)
                .finish()
        } else {
            f.debug_struct("SourceExecutor").finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use maplit::{convert_args, hashmap};
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_connector::source::datagen::DatagenSplit;
    use risingwave_pb::catalog::StreamSourceInfo;
    use risingwave_pb::plan_common::RowFormatType as ProstRowFormatType;
    use risingwave_source::connector_test_utils::create_source_desc_builder;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::executor::ActorContext;

    const MOCK_SOURCE_NAME: &str = "mock_source";

    #[tokio::test]
    async fn test_source_executor() {
        let table_id = TableId::default();
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::with_name(DataType::Int32, "sequence_int"),
            ],
        };
        let row_id_index = Some(0);
        let pk_column_ids = vec![0];
        let pk_indices = vec![0];
        let source_info = StreamSourceInfo {
            row_format: ProstRowFormatType::Json as i32,
            ..Default::default()
        };
        let (barrier_tx, barrier_rx) = unbounded_channel::<Barrier>();
        let column_ids = vec![0, 1].into_iter().map(ColumnId::from).collect();

        // This datagen will generate 3 rows at one time.
        let properties: HashMap<String, String> = convert_args!(hashmap!(
            "connector" => "datagen",
            "datagen.rows.per.second" => "3",
            "fields.sequence_int.kind" => "sequence",
            "fields.sequence_int.start" => "11",
            "fields.sequence_int.end" => "11111",
        ));
        let source_desc_builder = create_source_desc_builder(
            &schema,
            pk_column_ids,
            row_id_index,
            source_info,
            properties,
        );
        let split_state_store = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            MemoryStateStore::new(),
        )
        .await;
        let core = StreamSourceCore::<MemoryStateStore> {
            source_id: table_id,
            column_ids,
            source_identify: "Table_".to_string() + &table_id.table_id().to_string(),
            source_desc_builder: Some(source_desc_builder),
            stream_source_splits: HashMap::new(),
            split_state_store,
            state_cache: HashMap::new(),
            source_name: MOCK_SOURCE_NAME.to_string(),
        };

        let executor = SourceExecutorV2::new(
            ActorContext::create(0),
            schema,
            pk_indices,
            Some(core),
            Arc::new(StreamingMetrics::unused()),
            barrier_rx,
            u64::MAX,
            1,
        );
        let mut executor = Box::new(executor).execute();

        let init_barrier = Barrier::new_test_barrier(1).with_mutation(Mutation::Add {
            adds: HashMap::new(),
            splits: hashmap! {
                ActorId::default() => vec![
                    SplitImpl::Datagen(DatagenSplit {
                        split_index: 0,
                        split_num: 1,
                        start_offset: None,
                    }),
                ],
            },
        });
        barrier_tx.send(init_barrier).unwrap();

        // Consume barrier.
        executor.next().await.unwrap().unwrap();

        // Consume data chunk.
        let msg = executor.next().await.unwrap().unwrap();

        // Row id will not be filled here.
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I i
                + . 11
                + . 12
                + . 13"
            )
        );
    }

    #[tokio::test]
    async fn test_split_change_mutation() {
        let table_id = TableId::default();
        let schema = Schema {
            fields: vec![Field::with_name(DataType::Int32, "v1")],
        };
        let row_id_index = None;
        let pk_column_ids = vec![0];
        let pk_indices = vec![0_usize];
        let source_info = StreamSourceInfo {
            row_format: ProstRowFormatType::Json as i32,
            ..Default::default()
        };
        let properties = convert_args!(hashmap!(
            "connector" => "datagen",
            "fields.v1.min" => "1",
            "fields.v1.max" => "1000",
            "fields.v1.seed" => "12345",
        ));

        let source_desc_builder = create_source_desc_builder(
            &schema,
            pk_column_ids,
            row_id_index,
            source_info,
            properties,
        );
        let mem_state_store = MemoryStateStore::new();

        let column_ids = vec![ColumnId::from(0)];
        let (barrier_tx, barrier_rx) = unbounded_channel::<Barrier>();
        let split_state_store = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            mem_state_store.clone(),
        )
        .await;

        let core = StreamSourceCore::<MemoryStateStore> {
            source_id: table_id,
            column_ids: column_ids.clone(),
            source_identify: "Table_".to_string() + &table_id.table_id().to_string(),
            source_desc_builder: Some(source_desc_builder),
            stream_source_splits: HashMap::new(),
            split_state_store,
            state_cache: HashMap::new(),
            source_name: MOCK_SOURCE_NAME.to_string(),
        };

        let executor = SourceExecutorV2::new(
            ActorContext::create(0),
            schema,
            pk_indices,
            Some(core),
            Arc::new(StreamingMetrics::unused()),
            barrier_rx,
            u64::MAX,
            1,
        );

        let mut materialize = MaterializeExecutor::for_test(
            Box::new(executor),
            mem_state_store.clone(),
            TableId::from(0x2333),
            vec![OrderPair::new(0, OrderType::Ascending)],
            column_ids,
            2,
            Arc::new(AtomicU64::new(0)),
            false,
        )
        .await
        .boxed()
        .execute();

        let init_barrier = Barrier::new_test_barrier(1).with_mutation(Mutation::Add {
            adds: HashMap::new(),
            splits: hashmap! {
                ActorId::default() => vec![
                    SplitImpl::Datagen(DatagenSplit {
                        split_index: 0,
                        split_num: 3,
                        start_offset: None,
                    }),
                ],
            },
        });
        barrier_tx.send(init_barrier).unwrap();

        (materialize.next().await.unwrap().unwrap())
            .into_barrier()
            .unwrap();

        let mut ready_chunks = materialize.ready_chunks(10);
        let chunks = (ready_chunks.next().await.unwrap())
            .into_iter()
            .map(|msg| msg.unwrap().into_chunk().unwrap())
            .collect();
        let chunk_1 = StreamChunk::concat(chunks);
        assert_eq!(
            chunk_1,
            StreamChunk::from_pretty(
                " i
                + 533
                + 833
                + 738
                + 344",
            )
        );

        let new_assignments = vec![
            SplitImpl::Datagen(DatagenSplit {
                split_index: 0,
                split_num: 3,
                start_offset: None,
            }),
            SplitImpl::Datagen(DatagenSplit {
                split_index: 1,
                split_num: 3,
                start_offset: None,
            }),
        ];

        let change_split_mutation =
            Barrier::new_test_barrier(2).with_mutation(Mutation::SourceChangeSplit(hashmap! {
                ActorId::default() => new_assignments.clone()
            }));

        barrier_tx.send(change_split_mutation).unwrap();

        let _ = ready_chunks.next().await.unwrap(); // barrier

        let mut source_state_handler = SourceStateTableHandler::from_table_catalog(
            &default_source_internal_table(0x2333),
            mem_state_store.clone(),
        )
        .await;
        // there must exist state for new add partition
        source_state_handler.init_epoch(EpochPair::new_test_epoch(2));
        source_state_handler
            .get(new_assignments[1].id())
            .await
            .unwrap()
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        let chunks = (ready_chunks.next().await.unwrap())
            .into_iter()
            .map(|msg| msg.unwrap().into_chunk().unwrap())
            .collect();
        let chunk_2 = StreamChunk::concat(chunks).sort_rows();
        assert_eq!(
            chunk_2,
            // mixed from datagen split 0 and 1
            StreamChunk::from_pretty(
                " i
                + 29
                + 201
                + 344
                + 425
                + 525
                + 533
                + 833",
            )
        );

        let barrier = Barrier::new_test_barrier(3).with_mutation(Mutation::Pause);
        barrier_tx.send(barrier).unwrap();

        let barrier = Barrier::new_test_barrier(4).with_mutation(Mutation::Resume);
        barrier_tx.send(barrier).unwrap();
    }
}
