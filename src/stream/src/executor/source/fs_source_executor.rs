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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use anyhow::anyhow;
use either::Either;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_connector::source::{ConnectorState, SplitId, SplitImpl, SplitMetaData};
use risingwave_connector::{BoxSourceWithStateStream, StreamChunkWithState};
use risingwave_source::connector_source::{SourceContext, SourceDescBuilderV2};
use risingwave_source::fs_connector_source::FsConnectorSource;
use risingwave_source::monitor::SourceMetrics;
use risingwave_storage::StateStore;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;

use crate::error::StreamResult;
use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::source::reader::SourceReaderStream;
use crate::executor::source::state_table_handler::SourceStateTableHandler;
use crate::executor::*;
/// [`FsSourceExecutor`] is a streaming source, fir external file systems
/// such as s3.
pub struct FsSourceExecutor<S: StateStore> {
    ctx: ActorContextRef,

    source_id: TableId,
    source_desc_builder: SourceDescBuilderV2,

    column_ids: Vec<ColumnId>,
    schema: Schema,
    pk_indices: PkIndices,

    /// Identity string
    identity: String,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,

    source_identify: String,
    source_name: String,

    split_state_store: SourceStateTableHandler<S>,

    // state_cache of current epoch
    state_cache: HashMap<SplitId, SplitImpl>,

    /// just store information about the split that is currently being read
    /// because state_cache will is cleared every epoch
    stream_source_splits: HashMap<SplitId, SplitImpl>,

    /// Expected barrier latency
    expected_barrier_latency_ms: u64,
}

// epoch 1: actor 1: A, B, C; actor 2: D, E
// epoch 2: actor 1: A, B; actor 2: C, D, actor 3: E, F
// actor needs to know if C has been read

impl<S: StateStore> FsSourceExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        source_desc_builder: SourceDescBuilderV2,
        source_id: TableId,
        source_name: String,
        state_table: SourceStateTableHandler<S>,
        column_ids: Vec<ColumnId>,
        schema: Schema,
        pk_indices: PkIndices,
        barrier_receiver: UnboundedReceiver<Barrier>,
        executor_id: u64,
        _operator_id: u64,
        _op_info: String,
        streaming_metrics: Arc<StreamingMetrics>,
        expected_barrier_latency_ms: u64,
    ) -> StreamResult<Self> {
        Ok(Self {
            ctx,
            source_id,
            source_name,
            source_desc_builder,
            column_ids,
            schema,
            pk_indices,
            barrier_receiver: Some(barrier_receiver),
            identity: format!("SourceExecutor {:X}", executor_id),
            metrics: streaming_metrics,
            source_identify: "Table_".to_string() + &source_id.table_id().to_string(),
            split_state_store: state_table,
            stream_source_splits: HashMap::new(),
            state_cache: HashMap::new(),
            expected_barrier_latency_ms,
        })
    }
}

impl<S: StateStore> FsSourceExecutor<S> {
    // Note: get_diff will modify the state_cache
    async fn get_diff(&mut self, rhs: Vec<SplitImpl>) -> StreamExecutorResult<ConnectorState> {
        // rhs can not be None because we do not support split number reduction

        let all_completed: HashSet<SplitId> = self.split_state_store.get_all_completed().await?;

        tracing::debug!(actor = self.ctx.id, all_completed = ?all_completed , "get diff");

        let mut target_state: Vec<SplitImpl> = Vec::new();
        let mut no_change_flag = true;
        for sc in rhs {
            if let Some(s) = self.state_cache.get(&sc.id()) {
                let fs = s
                    .as_fs()
                    .unwrap_or_else(|| panic!("split {:?} is not fs", s));
                // unfinished this epoch
                if fs.offset < fs.size {
                    target_state.push(s.clone())
                }
            } else if all_completed.contains(&sc.id()) {
                // finish in prev epoch
                continue;
            } else {
                no_change_flag = false;
                // write new assigned split to state cache. snapshot is base on cache.
                let state = if let Some(recover_state) = self
                    .split_state_store
                    .try_recover_from_state_store(&sc)
                    .await?
                {
                    recover_state
                } else {
                    sc
                };

                self.state_cache
                    .entry(state.id())
                    .or_insert_with(|| state.clone());
                target_state.push(state);
            }
        }
        Ok((!no_change_flag).then_some(target_state))
    }

    async fn take_snapshot(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        let incompleted = self
            .state_cache
            .values()
            .filter(|split| {
                let fs = split
                    .as_fs()
                    .unwrap_or_else(|| panic!("split {:?} is not fs", split));
                fs.offset < fs.size
            })
            .cloned()
            .collect_vec();

        let completed = self
            .state_cache
            .values()
            .filter(|split| {
                let fs = split
                    .as_fs()
                    .unwrap_or_else(|| panic!("split {:?} is not fs", split));
                fs.offset == fs.size
            })
            .cloned()
            .collect_vec();

        if !incompleted.is_empty() {
            tracing::debug!(actor_id = self.ctx.id, incompleted = ?incompleted, "take snapshot");
            self.split_state_store.take_snapshot(incompleted).await?
        }

        if !completed.is_empty() {
            tracing::debug!(actor_id = self.ctx.id, completed = ?completed, "take snapshot");
            self.split_state_store.set_all_complete(completed).await?
        }
        // commit anyway, even if no message saved
        self.split_state_store.state_store.commit(epoch).await?;

        Ok(())
    }

    async fn build_stream_source_reader(
        &mut self,
        fs_source: &FsConnectorSource,
        source_metrics: Arc<SourceMetrics>,
        state: ConnectorState,
    ) -> StreamExecutorResult<BoxSourceWithStateStream> {
        let steam_reader = fs_source
            .stream_reader(
                state,
                self.column_ids.clone(),
                source_metrics.clone(),
                SourceContext::new(self.ctx.id, self.source_id),
            )
            .await
            .map_err(StreamExecutorError::connector_error)?;
        Ok(steam_reader.into_stream())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .stack_trace("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                StreamExecutorError::from(anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.ctx.id,
                    self.source_id
                ))
            })?;

        let fs_source = self
            .source_desc_builder
            .build_fs_stream_source()
            .map_err(StreamExecutorError::connector_error)?;

        let source_metrics = self.source_desc_builder.metrics();

        // If the first barrier is configuration change, then the source executor must be newly
        // created, and we should start with the paused state.
        let start_with_paused = barrier.is_update();

        let mut boot_state = Vec::default();
        if let Some(mutation) = barrier.mutation.as_deref() {
            match mutation {
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

        self.split_state_store.init_epoch(barrier.epoch);

        let all_completed: HashSet<SplitId> = self.split_state_store.get_all_completed().await?;
        tracing::debug!(actor = self.ctx.id, all_completed = ?all_completed , "get diff");

        let mut boot_state = boot_state
            .into_iter()
            .filter(|split| !all_completed.contains(&split.id()))
            .collect_vec();

        // restore the newest split info
        for ele in &mut boot_state {
            if let Some(recover_state) = self
                .split_state_store
                .try_recover_from_state_store(ele)
                .await?
            {
                *ele = recover_state;
            }
        }

        self.stream_source_splits = boot_state
            .clone()
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        let recover_state: ConnectorState = (!boot_state.is_empty()).then_some(boot_state);
        tracing::info!(actor_id = self.ctx.id, state = ?recover_state, "start with state");

        let source_chunk_reader = self
            .build_stream_source_reader(&fs_source, source_metrics.clone(), recover_state)
            .stack_trace("fs_source_start_reader")
            .await?;

        // Merge the chunks from source and the barriers into a single stream.
        let mut stream = SourceReaderStream::new(barrier_receiver, source_chunk_reader);
        if start_with_paused {
            stream.pause_source();
        }

        yield Message::Barrier(barrier);

        // We allow data to flow for 5 * `expected_barrier_latency_ms` milliseconds, considering
        // some other latencies like network and cost in Meta.
        let max_wait_barrier_time_ms = self.expected_barrier_latency_ms as u128 * 5;
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
                                self.apply_split_change(
                                    &fs_source,
                                    source_metrics.clone(),
                                    &mut stream,
                                    actor_splits,
                                )
                                .await?
                            }
                            Mutation::Pause => stream.pause_source(),
                            Mutation::Resume => stream.resume_source(),
                            Mutation::Update { actor_splits, .. } => {
                                self.apply_split_change(
                                    &fs_source,
                                    source_metrics.clone(),
                                    &mut stream,
                                    actor_splits,
                                )
                                .await?;
                            }
                            _ => {}
                        }
                    }
                    self.take_snapshot(epoch).await?;
                    self.state_cache.clear();
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
                    // update split offset
                    if let Some(mapping) = split_offset_mapping {
                        let state: Vec<(SplitId, SplitImpl)> = mapping
                            .iter()
                            .flat_map(|(id, offset)| {
                                let origin_split = self.stream_source_splits.get(id);

                                origin_split.map(|split| (id.clone(), split.update(offset.clone())))
                            })
                            .collect_vec();

                        self.state_cache.extend(state)
                    }

                    self.metrics
                        .source_output_row_count
                        .with_label_values(&[
                            self.source_identify.as_str(),
                            self.source_name.as_str(),
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

    async fn apply_split_change(
        &mut self,
        fs_source: &FsConnectorSource,
        source_metrics: Arc<SourceMetrics>,
        stream: &mut SourceReaderStream,
        mapping: &HashMap<ActorId, Vec<SplitImpl>>,
    ) -> StreamExecutorResult<()> {
        if let Some(target_splits) = mapping.get(&self.ctx.id).cloned() {
            if let Some(target_state) = self.get_diff(target_splits).await? {
                tracing::info!(
                    actor_id = self.ctx.id,
                    state = ?target_state,
                    "apply split change"
                );

                self.replace_stream_reader_with_target_state(
                    fs_source,
                    source_metrics,
                    stream,
                    target_state,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn replace_stream_reader_with_target_state(
        &mut self,
        fs_source: &FsConnectorSource,
        source_metrics: Arc<SourceMetrics>,
        stream: &mut SourceReaderStream,
        target_state: Vec<SplitImpl>,
    ) -> StreamExecutorResult<()> {
        tracing::info!(
            "actor {:?} apply source split change to {:?}",
            self.ctx.id,
            target_state
        );

        // Replace the source reader with a new one of the new state.
        let reader = self
            .build_stream_source_reader(fs_source, source_metrics, Some(target_state.clone()))
            .await?;
        stream.replace_source_stream(reader);

        self.stream_source_splits = target_state
            .into_iter()
            .map(|split| (split.id(), split))
            .collect();

        Ok(())
    }
}

impl<S: StateStore> Executor for FsSourceExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
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

impl<S: StateStore> Debug for FsSourceExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceExecutor")
            .field("source_id", &self.source_id)
            .field("column_ids", &self.column_ids)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}
