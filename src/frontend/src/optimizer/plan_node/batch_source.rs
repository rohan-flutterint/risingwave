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

use std::fmt;

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::SourceNode;
use risingwave_pb::catalog::SourceInfo;

use super::{LogicalSource, PlanBase, PlanRef, ToBatchProst, ToDistributedBatch, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order};

/// [`BatchSource`] represents a table/connector source at the very beginning of the graph.
#[derive(Debug, Clone)]
pub struct BatchSource {
    pub base: PlanBase,
    logical: LogicalSource,
}

impl BatchSource {
    pub fn new(logical: LogicalSource) -> Self {
        let base = PlanBase::new_batch(
            logical.ctx(),
            logical.schema().clone(),
            // Use `Single` by default, will be updated later with `clone_with_dist`.
            Distribution::Single,
            Order::any(),
        );
        Self { base, logical }
    }

    pub fn column_names(&self) -> Vec<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    pub fn logical(&self) -> &LogicalSource {
        &self.logical
    }

    pub fn clone_with_dist(&self) -> Self {
        let mut base = self.base.clone();
        base.dist = Distribution::SomeShard;
        Self {
            base,
            logical: self.logical.clone(),
        }
    }
}

impl_plan_tree_node_for_leaf! { BatchSource }

impl fmt::Display for BatchSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("BatchSource");
        builder
            .field("source", &self.logical.source_catalog().name)
            .field(
                "columns",
                &format_args!("[{}]", &self.column_names().join(", ")),
            )
            .finish()
    }
}

impl ToLocalBatch for BatchSource {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToDistributedBatch for BatchSource {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchProst for BatchSource {
    fn to_batch_prost_body(&self) -> NodeBody {
        let source_catalog = self.logical.source_catalog();
        NodeBody::Source(SourceNode {
            source_id: source_catalog.id,
            info: Some(SourceInfo {
                source_info: Some(source_catalog.info.clone()),
            }),
            columns: source_catalog
                .columns
                .iter()
                .map(|c| c.to_protobuf())
                .collect_vec(),
            properties: source_catalog.properties.clone(),
            split: vec![],
        })
    }
}
