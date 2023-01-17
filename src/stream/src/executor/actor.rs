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

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_stack_trace::{SpanValue, StackTrace};
use futures::future::join_all;
use futures::pin_mut;
use hytra::TrAdder;
use minitrace::prelude::*;
use parking_lot::Mutex;
use risingwave_common::util::epoch::EpochPair;
use risingwave_expr::ExprError;
use tokio_stream::StreamExt;

use super::monitor::StreamingMetrics;
use super::subtask::SubtaskHandle;
use super::StreamConsumer;
use crate::error::StreamResult;
use crate::task::{ActorId, SharedContext};

/// Shared by all operators of an actor.
pub struct ActorContext {
    pub id: ActorId,

    // TODO: report errors and prompt the user.
    pub errors: Mutex<HashMap<String, Vec<ExprError>>>,

    last_mem_val: Arc<AtomicUsize>,
    cur_mem_val: Arc<AtomicUsize>,
    total_mem_val: Arc<TrAdder<i64>>,
}

pub type ActorContextRef = Arc<ActorContext>;

impl ActorContext {
    pub fn create(id: ActorId) -> ActorContextRef {
        Arc::new(Self {
            id,
            errors: Default::default(),
            cur_mem_val: Arc::new(0.into()),
            last_mem_val: Arc::new(0.into()),
            total_mem_val: Arc::new(TrAdder::new()),
        })
    }

    pub fn create_with_counter(id: ActorId, total_mem_val: Arc<TrAdder<i64>>) -> ActorContextRef {
        Arc::new(Self {
            id,
            errors: Default::default(),
            cur_mem_val: Arc::new(0.into()),
            last_mem_val: Arc::new(0.into()),
            total_mem_val,
        })
    }

    pub fn on_compute_error(&self, err: ExprError, identity: &str) {
        tracing::error!("Compute error: {}, executor: {identity}", err);
        self.errors
            .lock()
            .entry(identity.to_owned())
            .or_default()
            .push(err);
    }

    pub fn store_mem_usage(&self, val: usize) {
        // Record the last mem val.
        // Calculate the difference between old val and new value, and apply the diff to total
        // memory usage value.
        let old_value = self.cur_mem_val.load(Ordering::Relaxed);
        self.last_mem_val.store(old_value, Ordering::Relaxed);
        let diff = val as i64 - old_value as i64;

        self.total_mem_val.inc(diff);

        self.cur_mem_val.store(val, Ordering::Relaxed);
    }

    pub fn mem_usage(&self) -> usize {
        self.cur_mem_val.load(Ordering::Relaxed)
    }
}

/// `Actor` is the basic execution unit in the streaming framework.
pub struct Actor<C> {
    /// The [`StreamConsumer`] of the actor.
    consumer: C,
    /// The subtasks to execute concurrently.
    subtasks: Vec<SubtaskHandle>,

    context: Arc<SharedContext>,
    _metrics: Arc<StreamingMetrics>,
    actor_context: ActorContextRef,
}

impl<C> Actor<C>
where
    C: StreamConsumer,
{
    pub fn new(
        consumer: C,
        subtasks: Vec<SubtaskHandle>,
        context: Arc<SharedContext>,
        metrics: Arc<StreamingMetrics>,
        actor_context: ActorContextRef,
    ) -> Self {
        Self {
            consumer,
            subtasks,
            context,
            _metrics: metrics,
            actor_context,
        }
    }

    #[inline(always)]
    pub async fn run(mut self) -> StreamResult<()> {
        tokio::join!(
            // Drive the subtasks concurrently.
            join_all(std::mem::take(&mut self.subtasks)),
            self.run_consumer(),
        )
        .1
    }

    async fn run_consumer(self) -> StreamResult<()> {
        let id = self.actor_context.id;

        let span_name = format!("actor_poll_{:03}", id);
        let mut span = {
            let mut span = Span::enter_with_local_parent("actor_poll");
            span.add_property(|| ("otel.name", span_name.to_string()));
            span.add_property(|| ("next", id.to_string()));
            span.add_property(|| ("next", "Outbound".to_string()));
            span.add_property(|| ("epoch", (-1).to_string()));
            span
        };

        let mut last_epoch: Option<EpochPair> = None;

        let stream = Box::new(self.consumer).execute();
        pin_mut!(stream);

        // Drive the streaming task with an infinite loop
        while let Some(barrier) = stream
            .next()
            .in_span(span)
            .stack_trace(last_epoch.map_or(SpanValue::Slice("Epoch <initial>"), |e| {
                format!("Epoch {}", e.curr).into()
            }))
            .await
            .transpose()?
        {
            last_epoch = Some(barrier.epoch);

            // Collect barriers to local barrier manager
            self.context.lock_barrier_manager().collect(id, &barrier);

            // Then stop this actor if asked
            let to_stop = barrier.is_stop_or_update_drop_actor(id);
            if to_stop {
                tracing::trace!(actor_id = id, "actor exit");
                return Ok(());
            }

            // Tracing related work
            span = {
                let mut span = Span::enter_with_local_parent("actor_poll");
                span.add_property(|| ("otel.name", span_name.to_string()));
                span.add_property(|| ("next", id.to_string()));
                span.add_property(|| ("next", "Outbound".to_string()));
                span.add_property(|| ("epoch", barrier.epoch.curr.to_string()));
                span
            };
        }

        Ok(())
    }
}
