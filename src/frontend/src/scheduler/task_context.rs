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

use std::sync::Arc;

use risingwave_batch::executor::BatchTaskMetricsWithTaskLabels;
use risingwave_batch::task::{BatchTaskContext, TaskOutput, TaskOutputId};
use risingwave_common::catalog::SysCatalogReaderRef;
use risingwave_common::config::BatchConfig;
use risingwave_common::error::Result;
use risingwave_common::util::addr::{is_local_address, HostAddr};
use risingwave_rpc_client::ComputeClientPoolRef;
use risingwave_source::monitor::SourceMetrics;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::session::{AuthContext, FrontendEnv};

/// Batch task execution context in frontend.
#[derive(Clone)]
pub struct FrontendBatchTaskContext {
    env: FrontendEnv,
    auth_context: Arc<AuthContext>,
}

impl FrontendBatchTaskContext {
    pub fn new(env: FrontendEnv, auth_context: Arc<AuthContext>) -> Self {
        Self { env, auth_context }
    }
}

impl BatchTaskContext for FrontendBatchTaskContext {
    fn get_task_output(&self, _task_output_id: TaskOutputId) -> Result<TaskOutput> {
        unimplemented!("not supported in local mode")
    }

    fn catalog_reader(&self) -> SysCatalogReaderRef {
        Arc::new(SysCatalogReaderImpl::new(
            self.env.catalog_reader().clone(),
            self.env.user_info_reader().clone(),
            self.env.worker_node_manager_ref(),
            self.env.meta_client_ref(),
            self.auth_context.clone(),
        ))
    }

    fn is_local_addr(&self, peer_addr: &HostAddr) -> bool {
        is_local_address(self.env.server_address(), peer_addr)
    }

    fn state_store(&self) -> risingwave_storage::store_impl::StateStoreImpl {
        unimplemented!("not supported in local mode")
    }

    fn task_metrics(&self) -> Option<BatchTaskMetricsWithTaskLabels> {
        None
    }

    fn client_pool(&self) -> ComputeClientPoolRef {
        self.env.client_pool()
    }

    fn get_config(&self) -> &BatchConfig {
        self.env.batch_config()
    }

    fn dml_manager(&self) -> risingwave_source::dml_manager::DmlManagerRef {
        unimplemented!("not supported in local mode")
    }

    fn source_metrics(&self) -> Arc<SourceMetrics> {
        self.env.source_metrics()
    }

    fn store_mem_usage(&self, _val: usize) {
        todo!()
    }

    fn mem_usage(&self) -> usize {
        todo!()
    }
}
