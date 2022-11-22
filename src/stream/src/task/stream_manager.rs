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

use core::time::Duration;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_recursion::async_recursion;
use async_stack_trace::{StackTraceManager, StackTraceReport, TraceConfig};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::config::StreamingConfig;
use risingwave_common::util::addr::HostAddr;
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_pb::{stream_plan, stream_service};
use risingwave_storage::{dispatch_state_store, StateStore, StateStoreImpl};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use super::{unique_executor_id, unique_operator_id, CollectResult};
use crate::error::{StreamError, StreamResult};
use crate::executor::exchange::permit::Receiver;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::subtask::SubtaskHandle;
use crate::executor::*;
use crate::from_proto::create_executor;
use crate::task::{ActorId, FragmentId, SharedContext, StreamEnvironment, UpDownActorIds};

#[cfg(test)]
pub static LOCAL_TEST_ADDR: std::sync::LazyLock<HostAddr> =
    std::sync::LazyLock::new(|| "127.0.0.1:2333".parse().unwrap());

pub type ActorHandle = JoinHandle<()>;

pub struct LocalStreamManagerCore {
    /// Runtime for the streaming actors.
    runtime: &'static tokio::runtime::Runtime,

    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    handles: HashMap<ActorId, ActorHandle>,

    pub(crate) context: Arc<SharedContext>,

    /// Stores all actor information, taken after actor built.
    actors: HashMap<ActorId, stream_plan::StreamActor>,

    /// Stores all actor tokio runtime monitoring tasks.
    actor_monitor_tasks: HashMap<ActorId, ActorHandle>,

    /// The state store implement
    state_store: StateStoreImpl,

    /// Metrics of the stream manager
    pub(crate) streaming_metrics: Arc<StreamingMetrics>,

    /// Config of streaming engine
    pub(crate) config: StreamingConfig,

    /// Manages the stack traces of all actors.
    stack_trace_manager: Option<(StackTraceManager<ActorId>, TraceConfig)>,
}

/// `LocalStreamManager` manages all stream executors in this project.
pub struct LocalStreamManager {
    core: Mutex<LocalStreamManagerCore>,

    // Maintain a copy of the core to reduce async locks
    state_store: StateStoreImpl,
    context: Arc<SharedContext>,
    streaming_metrics: Arc<StreamingMetrics>,
}

pub struct ExecutorParams {
    pub env: StreamEnvironment,

    /// Indices of primary keys
    pub pk_indices: PkIndices,

    /// Executor id, unique across all actors.
    pub executor_id: u64,

    /// Operator id, unique for each operator in fragment.
    pub operator_id: u64,

    /// Information of the operator from plan node.
    pub op_info: String,

    /// The output schema of the executor.
    pub schema: Schema,

    /// The input executor.
    pub input: Vec<BoxedExecutor>,

    /// FragmentId of the actor
    pub fragment_id: FragmentId,

    /// Metrics
    pub executor_stats: Arc<StreamingMetrics>,

    /// Actor context
    pub actor_context: ActorContextRef,

    /// Vnodes owned by this executor. Represented in bitmap.
    pub vnode_bitmap: Option<Bitmap>,
}

impl Debug for ExecutorParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorParams")
            .field("pk_indices", &self.pk_indices)
            .field("executor_id", &self.executor_id)
            .field("operator_id", &self.operator_id)
            .field("op_info", &self.op_info)
            .field("schema", &self.schema)
            .field("input", &self.input.len())
            .field("actor_id", &self.actor_context.id)
            .finish_non_exhaustive()
    }
}

impl LocalStreamManager {
    fn with_core(core: LocalStreamManagerCore) -> Self {
        Self {
            state_store: core.state_store.clone(),
            context: core.context.clone(),
            streaming_metrics: core.streaming_metrics.clone(),
            core: Mutex::new(core),
        }
    }

    pub fn new(
        addr: HostAddr,
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
        config: StreamingConfig,
        async_stack_trace_config: Option<TraceConfig>,
        enable_managed_cache: bool,
    ) -> Self {
        Self::with_core(LocalStreamManagerCore::new(
            addr,
            state_store,
            streaming_metrics,
            config,
            async_stack_trace_config,
            enable_managed_cache,
        ))
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::with_core(LocalStreamManagerCore::for_test())
    }

    /// Print the traces of all actors periodically, used for debugging only.
    pub fn spawn_print_trace(self: Arc<Self>) -> JoinHandle<!> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
                let mut core = self.core.lock().await;

                for (k, trace) in core
                    .stack_trace_manager
                    .as_mut()
                    .expect("async stack trace not enabled")
                    .0
                    .get_all()
                {
                    println!(">> Actor {}\n\n{}", k, &*trace);
                }
            }
        })
    }

    /// Get stack trace reports for all actors.
    pub async fn get_actor_traces(&self) -> HashMap<ActorId, StackTraceReport> {
        let mut core = self.core.lock().await;
        match &mut core.stack_trace_manager {
            Some((mgr, _)) => mgr.get_all().map(|(k, v)| (*k, v.clone())).collect(),
            None => Default::default(),
        }
    }

    /// Broadcast a barrier to all senders. Save a receiver in barrier manager
    pub fn send_barrier(
        &self,
        barrier: &Barrier,
        actor_ids_to_send: impl IntoIterator<Item = ActorId>,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
    ) -> StreamResult<()> {
        let timer = self
            .streaming_metrics
            .barrier_inflight_latency
            .start_timer();
        let mut barrier_manager = self.context.lock_barrier_manager();
        barrier_manager.send_barrier(
            barrier,
            actor_ids_to_send,
            actor_ids_to_collect,
            Some(timer),
        )?;
        Ok(())
    }

    /// Clear all senders and collect rx in barrier manager.
    pub fn clear_all_senders_and_collect_rx(&self) {
        let mut barrier_manager = self.context.lock_barrier_manager();
        barrier_manager.clear_senders();
        barrier_manager.clear_collect_rx();
    }

    /// Use `epoch` to find collect rx. And wait for all actor to be collected before
    /// returning.
    pub async fn collect_barrier(&self, epoch: u64) -> StreamResult<(CollectResult, bool)> {
        let complete_receiver = {
            let mut barrier_manager = self.context.lock_barrier_manager();
            barrier_manager.remove_collect_rx(epoch)
        };
        // Wait for all actors finishing this barrier.
        let result = complete_receiver
            .complete_receiver
            .expect("no rx for local mode")
            .await
            .context("failed to collect barrier")??;
        complete_receiver
            .barrier_inflight_timer
            .expect("no timer for test")
            .observe_duration();
        Ok((result, complete_receiver.checkpoint))
    }

    pub async fn sync_epoch(&self, epoch: u64) -> StreamResult<Vec<LocalSstableInfo>> {
        let timer = self
            .core
            .lock()
            .await
            .streaming_metrics
            .barrier_sync_latency
            .start_timer();
        let res = dispatch_state_store!(self.state_store.clone(), store, {
            match store.sync(epoch).await {
                Ok(sync_result) => Ok(sync_result.uncommitted_ssts),
                Err(e) => {
                    tracing::error!(
                        "Failed to sync state store after receiving barrier prev_epoch {:?} due to {}",
                        epoch, e);
                    Err(e.into())
                }
            }
        });
        timer.observe_duration();
        res
    }

    pub async fn clear_storage_buffer(&self) {
        dispatch_state_store!(self.state_store.clone(), store, {
            store.clear_shared_buffer().await.unwrap();
        });
    }

    /// Broadcast a barrier to all senders. Returns immediately, and caller won't be notified when
    /// this barrier is finished.
    #[cfg(test)]
    pub fn send_barrier_for_test(&self, barrier: &Barrier) -> StreamResult<()> {
        use std::iter::empty;

        let mut barrier_manager = self.context.lock_barrier_manager();
        assert!(barrier_manager.is_local_mode());
        let timer = self
            .streaming_metrics
            .barrier_inflight_latency
            .start_timer();
        barrier_manager.send_barrier(barrier, empty(), empty(), Some(timer))?;
        barrier_manager.remove_collect_rx(barrier.epoch.prev);
        Ok(())
    }

    pub async fn drop_actor(&self, actors: &[ActorId]) -> StreamResult<()> {
        let mut core = self.core.lock().await;
        for id in actors {
            core.drop_actor(*id);
        }
        tracing::debug!(actors = ?actors, "drop actors");
        Ok(())
    }

    /// Force stop all actors on this worker.
    pub async fn stop_all_actors(&self) -> StreamResult<()> {
        // Clear shared buffer in storage to release memory
        self.clear_storage_buffer().await;
        self.clear_all_senders_and_collect_rx();
        self.core.lock().await.drop_all_actors();

        Ok(())
    }

    pub async fn take_receiver(&self, ids: UpDownActorIds) -> StreamResult<Receiver> {
        let core = self.core.lock().await;
        core.context.take_receiver(&ids)
    }

    pub async fn update_actors(
        &self,
        actors: &[stream_plan::StreamActor],
        hanging_channels: &[stream_service::HangingChannel],
    ) -> StreamResult<()> {
        let mut core = self.core.lock().await;
        core.update_actors(actors, hanging_channels)
    }

    /// This function was called while [`LocalStreamManager`] exited.
    pub async fn wait_all(self) -> StreamResult<()> {
        let handles = self.core.lock().await.take_all_handles()?;
        for (_id, handle) in handles {
            handle.await.unwrap();
        }
        Ok(())
    }

    /// This function could only be called once during the lifecycle of `LocalStreamManager` for
    /// now.
    pub async fn update_actor_info(&self, actor_infos: &[ActorInfo]) -> StreamResult<()> {
        let mut core = self.core.lock().await;
        core.update_actor_info(actor_infos)
    }

    /// This function could only be called once during the lifecycle of `LocalStreamManager` for
    /// now.
    pub async fn build_actors(
        &self,
        actors: &[ActorId],
        env: StreamEnvironment,
    ) -> StreamResult<()> {
        let mut core = self.core.lock().await;
        core.build_actors(actors, env).await
    }

    pub async fn config(&self) -> StreamingConfig {
        let core = self.core.lock().await;
        core.config.clone()
    }
}

fn update_upstreams(context: &SharedContext, ids: &[UpDownActorIds]) {
    ids.iter()
        .map(|&id| {
            context.add_channel_pairs(id);
        })
        .count();
}

impl LocalStreamManagerCore {
    fn new(
        addr: HostAddr,
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
        config: StreamingConfig,
        async_stack_trace_config: Option<TraceConfig>,
        enable_managed_cache: bool,
    ) -> Self {
        let context = SharedContext::new(addr, state_store.clone(), &config, enable_managed_cache);
        Self::new_inner(
            state_store,
            context,
            streaming_metrics,
            config,
            async_stack_trace_config,
        )
    }

    fn new_inner(
        state_store: StateStoreImpl,
        context: SharedContext,
        streaming_metrics: Arc<StreamingMetrics>,
        config: StreamingConfig,
        async_stack_trace_config: Option<TraceConfig>,
    ) -> Self {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(worker_threads_num) = config.actor_runtime_worker_threads_num {
            builder.worker_threads(worker_threads_num);
        }
        let runtime = builder
            .thread_name("risingwave-streaming-actor")
            .enable_all()
            .build()
            .unwrap();

        Self {
            // Leak the runtime to avoid runtime shutting-down in the main async context.
            // TODO: may manually shutdown the runtime after we implement graceful shutdown for
            // stream manager.
            runtime: Box::leak(Box::new(runtime)),
            handles: HashMap::new(),
            context: Arc::new(context),
            actors: HashMap::new(),
            actor_monitor_tasks: HashMap::new(),
            state_store,
            streaming_metrics,
            config,
            stack_trace_manager: async_stack_trace_config
                .map(|c| (StackTraceManager::default(), c)),
        }
    }

    #[cfg(test)]
    fn for_test() -> Self {
        use risingwave_storage::monitor::StateStoreMetrics;

        let register = prometheus::Registry::new();
        let streaming_metrics = Arc::new(StreamingMetrics::new(register));
        Self::new_inner(
            StateStoreImpl::shared_in_memory_store(Arc::new(StateStoreMetrics::unused())),
            SharedContext::for_test(),
            streaming_metrics,
            StreamingConfig::default(),
            None,
        )
    }

    /// Create dispatchers with downstream information registered before
    fn create_dispatcher(
        &mut self,
        input: BoxedExecutor,
        dispatchers: &[stream_plan::Dispatcher],
        actor_id: ActorId,
    ) -> StreamResult<DispatchExecutor> {
        let dispatcher_impls = dispatchers
            .iter()
            .map(|dispatcher| DispatcherImpl::new(&self.context, actor_id, dispatcher))
            .try_collect()?;

        Ok(DispatchExecutor::new(
            input,
            dispatcher_impls,
            actor_id,
            self.context.clone(),
            self.streaming_metrics.clone(),
        ))
    }

    /// Create a chain(tree) of nodes, with given `store`.
    #[allow(clippy::too_many_arguments)]
    #[async_recursion]
    async fn create_nodes_inner(
        &mut self,
        fragment_id: FragmentId,
        node: &stream_plan::StreamNode,
        input_pos: usize,
        env: StreamEnvironment,
        store: impl StateStore,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
        has_stateful: bool,
        subtasks: &mut Vec<SubtaskHandle>,
    ) -> StreamResult<BoxedExecutor> {
        // The "stateful" here means that the executor may issue read operations to the state store
        // massively and continuously. Used to decide whether to apply the optimization of subtasks.
        fn is_stateful_executor(stream_node: &StreamNode) -> bool {
            matches!(
                stream_node.get_node_body().unwrap(),
                NodeBody::HashAgg(_)
                    | NodeBody::HashJoin(_)
                    | NodeBody::DeltaIndexJoin(_)
                    | NodeBody::Lookup(_)
                    | NodeBody::Chain(_)
                    | NodeBody::DynamicFilter(_)
                    | NodeBody::GroupTopN(_)
            )
        }
        let is_stateful = is_stateful_executor(node);

        // Create the input executor before creating itself
        let mut input = Vec::with_capacity(node.input.iter().len());
        for (input_pos, input_stream_node) in node.input.iter().enumerate() {
            input.push(
                self.create_nodes_inner(
                    fragment_id,
                    input_stream_node,
                    input_pos,
                    env.clone(),
                    store.clone(),
                    actor_context,
                    vnode_bitmap.clone(),
                    has_stateful || is_stateful,
                    subtasks,
                )
                .await?,
            );
        }

        let op_info = node.get_identity().clone();
        let pk_indices = node
            .get_stream_key()
            .iter()
            .map(|idx| *idx as usize)
            .collect::<Vec<_>>();

        // We assume that the operator_id of different instances from the same RelNode will be the
        // same.
        let executor_id = unique_executor_id(actor_context.id, node.operator_id);
        let operator_id = unique_operator_id(fragment_id, node.operator_id);
        let schema = node.fields.iter().map(Field::from).collect();

        // Build the executor with params.
        let executor_params = ExecutorParams {
            env: env.clone(),
            pk_indices,
            executor_id,
            operator_id,
            op_info,
            schema,
            input,
            fragment_id,
            executor_stats: self.streaming_metrics.clone(),
            actor_context: actor_context.clone(),
            vnode_bitmap,
        };

        let executor = create_executor(executor_params, self, node, store).await?;

        // Wrap the executor for debug purpose.
        let executor = WrapperExecutor::new(
            executor,
            input_pos,
            actor_context.id,
            executor_id,
            self.streaming_metrics.clone(),
            self.config.developer.stream_enable_executor_row_count,
        )
        .boxed();

        // If there're multiple stateful executors in this actor, we will wrap it into a subtask.
        let executor = if has_stateful && is_stateful {
            let (subtask, executor) = subtask::wrap(executor);
            subtasks.push(subtask);
            executor.boxed()
        } else {
            executor
        };

        Ok(executor)
    }

    /// Create a chain(tree) of nodes and return the head executor.
    async fn create_nodes(
        &mut self,
        fragment_id: FragmentId,
        node: &stream_plan::StreamNode,
        env: StreamEnvironment,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
    ) -> StreamResult<(BoxedExecutor, Vec<SubtaskHandle>)> {
        let mut subtasks = vec![];

        let executor = dispatch_state_store!(self.state_store.clone(), store, {
            self.create_nodes_inner(
                fragment_id,
                node,
                0,
                env,
                store,
                actor_context,
                vnode_bitmap,
                false,
                &mut subtasks,
            )
            .await
        })?;

        Ok((executor, subtasks))
    }

    async fn build_actors(
        &mut self,
        actors: &[ActorId],
        env: StreamEnvironment,
    ) -> StreamResult<()> {
        for &actor_id in actors {
            let actor = self.actors.remove(&actor_id).ok_or_else(|| {
                StreamError::from(anyhow!("No such actor with actor id:{}", actor_id))
            })?;
            let mview_definition = &actor.mview_definition;
            let actor_context = ActorContext::create(actor_id);
            let vnode_bitmap = actor
                .vnode_bitmap
                .as_ref()
                .map(|b| b.try_into())
                .transpose()
                .context("failed to decode vnode bitmap")?;

            let (executor, subtasks) = self
                .create_nodes(
                    actor.fragment_id,
                    actor.get_nodes()?,
                    env.clone(),
                    &actor_context,
                    vnode_bitmap,
                )
                .await?;

            let dispatcher = self.create_dispatcher(executor, &actor.dispatcher, actor_id)?;
            let actor = Actor::new(
                dispatcher,
                subtasks,
                self.context.clone(),
                self.streaming_metrics.clone(),
                actor_context,
            );

            let monitor = tokio_metrics::TaskMonitor::new();
            let trace_reporter = self
                .stack_trace_manager
                .as_mut()
                .map(|(m, _)| m.register(actor_id));

            let handle = {
                let context = self.context.clone();
                let actor = async move {
                    if let Err(err) = actor.run().await {
                        // TODO: check error type and panic if it's unexpected.
                        tracing::error!(actor=%actor_id, error=%err, "actor exit");
                        context.lock_barrier_manager().notify_failure(actor_id, err);
                    }
                };
                #[auto_enums::auto_enum(Future)]
                let traced = match trace_reporter {
                    Some(trace_reporter) => trace_reporter.trace(
                        actor,
                        format!("Actor {actor_id}: `{}`", mview_definition),
                        TraceConfig {
                            report_detached: true,
                            verbose: true,
                            interval: Duration::from_secs(1),
                        },
                    ),
                    None => actor,
                };
                let instrumented = monitor.instrument(traced);
                self.runtime.spawn(instrumented)
            };
            self.handles.insert(actor_id, handle);

            let actor_id_str = actor_id.to_string();

            let metrics = self.streaming_metrics.clone();
            let actor_monitor_task = self.runtime.spawn(async move {
                loop {
                    let task_metrics = monitor.cumulative();
                    metrics
                        .actor_execution_time
                        .with_label_values(&[&actor_id_str])
                        .set(task_metrics.total_poll_duration.as_secs_f64());
                    metrics
                        .actor_fast_poll_duration
                        .with_label_values(&[&actor_id_str])
                        .set(task_metrics.total_fast_poll_duration.as_secs_f64());
                    metrics
                        .actor_fast_poll_cnt
                        .with_label_values(&[&actor_id_str])
                        .set(task_metrics.total_fast_poll_count as i64);
                    metrics
                        .actor_slow_poll_duration
                        .with_label_values(&[&actor_id_str])
                        .set(task_metrics.total_slow_poll_duration.as_secs_f64());
                    metrics
                        .actor_slow_poll_cnt
                        .with_label_values(&[&actor_id_str])
                        .set(task_metrics.total_slow_poll_count as i64);
                    metrics
                        .actor_poll_duration
                        .with_label_values(&[&actor_id_str])
                        .set(task_metrics.total_poll_duration.as_secs_f64());
                    metrics
                        .actor_poll_cnt
                        .with_label_values(&[&actor_id_str])
                        .set(task_metrics.total_poll_count as i64);
                    metrics
                        .actor_idle_duration
                        .with_label_values(&[&actor_id_str])
                        .set(task_metrics.total_idle_duration.as_secs_f64());
                    metrics
                        .actor_idle_cnt
                        .with_label_values(&[&actor_id_str])
                        .set(task_metrics.total_idled_count as i64);
                    metrics
                        .actor_scheduled_duration
                        .with_label_values(&[&actor_id_str])
                        .set(task_metrics.total_scheduled_duration.as_secs_f64());
                    metrics
                        .actor_scheduled_cnt
                        .with_label_values(&[&actor_id_str])
                        .set(task_metrics.total_scheduled_count as i64);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
            self.actor_monitor_tasks
                .insert(actor_id, actor_monitor_task);
        }

        Ok(())
    }

    pub fn take_all_handles(&mut self) -> StreamResult<HashMap<ActorId, ActorHandle>> {
        Ok(std::mem::take(&mut self.handles))
    }

    pub fn remove_actor_handles(
        &mut self,
        actor_ids: &[ActorId],
    ) -> StreamResult<Vec<ActorHandle>> {
        actor_ids
            .iter()
            .map(|actor_id| {
                self.handles
                    .remove(actor_id)
                    .ok_or_else(|| anyhow!("No such actor with actor id:{}", actor_id).into())
            })
            .try_collect()
    }

    fn update_actor_info(&mut self, new_actor_infos: &[ActorInfo]) -> StreamResult<()> {
        let mut actor_infos = self.context.actor_infos.write();
        for actor in new_actor_infos {
            let ret = actor_infos.insert(actor.get_actor_id(), actor.clone());
            if let Some(prev_actor) = ret && actor != &prev_actor{
                bail!(
                    "actor info mismatch when broadcasting {}",
                    actor.get_actor_id()
                );
            }
        }
        Ok(())
    }

    /// `drop_actor` is invoked by meta node via RPC once the stop barrier arrives at the
    /// sink. All the actors in the actors should stop themselves before this method is invoked.
    fn drop_actor(&mut self, actor_id: ActorId) {
        self.context.retain_channel(|&(up_id, _)| up_id != actor_id);
        self.actor_monitor_tasks
            .remove(&actor_id)
            .inspect(|handle| handle.abort());
        self.context.actor_infos.write().remove(&actor_id);
        self.actors.remove(&actor_id);
        // Task should have already stopped when this method is invoked.
        self.handles
            .remove(&actor_id)
            .inspect(|handle| handle.abort());
    }

    /// `drop_all_actors` is invoked by meta node via RPC for recovery purpose.
    fn drop_all_actors(&mut self) {
        for (actor_id, handle) in self.handles.drain() {
            tracing::debug!("force stopping actor {}", actor_id);
            handle.abort();
        }
        self.actors.clear();
        self.context.clear_channels();
        if let Some((stack_trace_manager, _)) = self.stack_trace_manager.as_mut() {
            std::mem::take(stack_trace_manager);
        }
        self.actor_monitor_tasks.clear();
        self.context.actor_infos.write().clear();
    }

    fn update_actors(
        &mut self,
        actors: &[stream_plan::StreamActor],
        hanging_channels: &[stream_service::HangingChannel],
    ) -> StreamResult<()> {
        for actor in actors {
            self.actors
                .try_insert(actor.get_actor_id(), actor.clone())
                .map_err(|_| anyhow!("duplicated actor {}", actor.get_actor_id()))?;
        }

        for actor in actors {
            // At this time, the graph might not be complete, so we do not check if downstream
            // has `current_id` as upstream.
            let down_id = actor
                .dispatcher
                .iter()
                .flat_map(|x| x.downstream_actor_id.iter())
                .map(|id| (actor.actor_id, *id))
                .collect_vec();
            update_upstreams(&self.context, &down_id);
        }

        for hanging_channel in hanging_channels {
            match (&hanging_channel.upstream, &hanging_channel.downstream) {
                (
                    Some(ActorInfo {
                        actor_id: up_id,
                        host: None, // local
                    }),
                    Some(ActorInfo {
                        actor_id: down_id,
                        host: Some(_), // remote
                    }),
                ) => {
                    let up_down_ids = (*up_id, *down_id);
                    self.context.add_channel_pairs(up_down_ids);
                }
                _ => bail!("hanging channel must be from local to remote: {hanging_channel:?}"),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod test_utils {
    use risingwave_pb::common::HostAddress;

    use super::*;

    pub fn add_local_channels(ctx: Arc<SharedContext>, up_down_ids: Vec<(u32, u32)>) {
        for up_down_id in up_down_ids {
            ctx.add_channel_pairs(up_down_id);
        }
    }

    pub fn helper_make_local_actor(actor_id: u32) -> ActorInfo {
        ActorInfo {
            actor_id,
            host: Some(HostAddress {
                host: LOCAL_TEST_ADDR.host.clone(),
                port: LOCAL_TEST_ADDR.port as i32,
            }),
        }
    }
}
