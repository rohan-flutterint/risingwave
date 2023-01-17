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

use std::fmt::Debug;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilderImpl, Op};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::must_match;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_storage::StateStore;

use super::agg_state::{AggState, AggStateStorage};
use super::AggCall;
use crate::common::table::state_table::StateTable;
use crate::executor::error::StreamExecutorResult;
use crate::executor::PkIndices;

/// [`AggGroup`] manages agg states of all agg calls for one `group_key`.
pub struct AggGroup<S: StateStore> {
    /// Group key.
    group_key: Option<OwnedRow>,

    /// Current managed states for all [`AggCall`]s.
    states: Vec<AggState<S>>,

    /// Previous outputs of managed states. Initializing with `None`.
    prev_outputs: Option<OwnedRow>,
}

impl<S: StateStore> Debug for AggGroup<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggGroup")
            .field("group_key", &self.group_key)
            .field("prev_outputs", &self.prev_outputs)
            .finish()
    }
}

/// We assume the first state of aggregation is always `StreamingRowCountAgg`.
const ROW_COUNT_COLUMN: usize = 0;

/// Information about the changes built by `AggState::build_changes`.
pub struct AggChangesInfo {
    /// The number of rows and corresponding ops in the changes.
    pub n_appended_ops: usize,
    /// The result row containing group key prefix. To be inserted into result table.
    pub result_row: OwnedRow,
    /// The previous outputs of all agg calls recorded in the `AggState`.
    pub prev_outputs: Option<OwnedRow>,
}

impl<S: StateStore> AggGroup<S> {
    /// Create [`AggGroup`] for the given [`AggCall`]s and `group_key`.
    /// For [`crate::executor::GlobalSimpleAggExecutor`], the `group_key` should be `None`.
    pub async fn create(
        group_key: Option<OwnedRow>,
        agg_calls: &[AggCall],
        storages: &[AggStateStorage<S>],
        result_table: &StateTable<S>,
        pk_indices: &PkIndices,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<AggGroup<S>> {
        let prev_outputs: Option<OwnedRow> = result_table.get_row(&group_key).await?;
        if let Some(prev_outputs) = &prev_outputs {
            assert_eq!(prev_outputs.len(), agg_calls.len());
        }

        let states =
            futures::future::try_join_all(agg_calls.iter().enumerate().map(|(idx, agg_call)| {
                AggState::create(
                    agg_call,
                    &storages[idx],
                    prev_outputs.as_ref().map(|outputs| &outputs[idx]),
                    pk_indices,
                    group_key.as_ref(),
                    extreme_cache_size,
                    input_schema,
                )
            }))
            .await?;

        Ok(Self {
            group_key,
            states,
            prev_outputs,
        })
    }

    pub fn group_key(&self) -> Option<&OwnedRow> {
        self.group_key.as_ref()
    }

    fn prev_row_count(&self) -> usize {
        match &self.prev_outputs {
            Some(states) => states[ROW_COUNT_COLUMN]
                .as_ref()
                .map(|x| *x.as_int64() as usize)
                .unwrap_or(0),
            None => 0,
        }
    }

    /// Apply input chunk to all managed agg states.
    /// `visibilities` contains the row visibility of the input chunk for each agg call.
    pub fn apply_chunk(
        &mut self,
        storages: &mut [AggStateStorage<S>],
        ops: &[Op],
        columns: &[Column],
        visibilities: Vec<Option<Bitmap>>,
    ) -> StreamExecutorResult<()> {
        let columns = columns.iter().map(|col| col.array_ref()).collect_vec();
        for ((state, storage), visibility) in
            self.states.iter_mut().zip_eq(storages).zip_eq(visibilities)
        {
            state.apply_chunk(ops, visibility.as_ref(), &columns, storage)?;
        }
        Ok(())
    }

    /// Flush in-memory state into state table if needed.
    /// The calling order of this method and `get_outputs` doesn't matter, but this method
    /// must be called before committing state tables.
    pub async fn flush_state_if_needed(
        &self,
        storages: &mut [AggStateStorage<S>],
    ) -> StreamExecutorResult<()> {
        futures::future::try_join_all(self.states.iter().zip_eq(storages).filter_map(
            |(state, storage)| match state {
                AggState::Table(state) => Some(state.flush_state_if_needed(
                    must_match!(storage, AggStateStorage::Table { table } => table),
                    self.group_key(),
                )),
                _ => None,
            },
        ))
        .await?;
        Ok(())
    }

    /// Reset all in-memory states to their initial state, i.e. to reset all agg state structs to
    /// the status as if they are just created, no input applied and no row in state table.
    fn reset(&mut self) {
        self.states.iter_mut().for_each(|state| state.reset());
    }

    /// Get the outputs of all managed agg states.
    /// Possibly need to read/sync from state table if the state not cached in memory.
    pub async fn get_outputs(
        &mut self,
        storages: &[AggStateStorage<S>],
    ) -> StreamExecutorResult<OwnedRow> {
        // Row count doesn't need I/O, so the following statement is supposed to be fast.
        let row_count = self.states[ROW_COUNT_COLUMN]
            .get_output(&storages[ROW_COUNT_COLUMN], self.group_key.as_ref())
            .await?
            .as_ref()
            .map(|x| *x.as_int64() as usize)
            .expect("row count should not be None");
        if row_count == 0 {
            // Reset all states (in fact only value states will be reset).
            // This is important because for some agg calls (e.g. `sum`), if no row is applied,
            // they should output NULL, for some other calls (e.g. `sum0`), they should output 0.
            self.reset();
        }
        futures::future::try_join_all(
            self.states
                .iter_mut()
                .zip_eq(storages)
                .map(|(state, storage)| state.get_output(storage, self.group_key.as_ref())),
        )
        .await
        .map(OwnedRow::new)
    }

    /// Build changes into `builders` and `new_ops`, according to previous and current agg outputs.
    /// Returns [`AggChangesInfo`] contains information about changes built.
    /// The saved previous outputs will be updated to the latest outputs after building changes.
    pub fn build_changes(
        &mut self,
        curr_outputs: OwnedRow,
        builders: &mut [ArrayBuilderImpl],
        new_ops: &mut Vec<Op>,
    ) -> AggChangesInfo {
        let row_count = curr_outputs[ROW_COUNT_COLUMN]
            .as_ref()
            .map(|x| *x.as_int64())
            .expect("row count should not be None");
        let prev_row_count = self.prev_row_count();

        trace!(
            "prev_row_count = {}, row_count = {}",
            prev_row_count,
            row_count
        );

        let n_appended_ops = match (
            prev_row_count,
            row_count,
            self.group_key().is_some(),
            self.prev_outputs.is_some(),
        ) {
            (0, 0, _, _) => {
                // Previous state is empty, current state is also empty.
                // FIXME: for `SimpleAgg`, should we still build some changes when `row_count` is 0
                // While other aggs may not be `0`?

                0
            }

            (0, _, true, _) | (0, _, _, false) => {
                // Previous state is empty, current state is not empty, insert one `Insert` op.
                new_ops.push(Op::Insert);

                for (builder, new_value) in builders.iter_mut().zip_eq(curr_outputs.iter()) {
                    trace!("append_datum (0 -> N): {:?}", new_value);
                    builder.append_datum(new_value);
                }

                1
            }

            (_, 0, true, _) => {
                // Previous state is not empty, current state is empty, insert one `Delete` op.
                new_ops.push(Op::Delete);

                for (builder, old_value) in builders
                    .iter_mut()
                    .zip_eq(self.prev_outputs.as_ref().unwrap().iter())
                {
                    trace!("append_datum (N -> 0): {:?}", old_value);
                    builder.append_datum(old_value);
                }

                1
            }

            _ => {
                // 1. Previous state is not empty and current state is not empty.
                //
                // 2. Previous state is not empty and current state is empty and there is no group
                // by keys.
                //
                // 3. Previous state is empty and current state is not empty and there is no group
                // by keys and prev_outputs is not none.
                //
                // Insert two `Update` op.
                new_ops.push(Op::UpdateDelete);
                new_ops.push(Op::UpdateInsert);

                for (builder, old_value, new_value) in itertools::multizip((
                    builders.iter_mut(),
                    self.prev_outputs.as_ref().unwrap().iter(),
                    curr_outputs.iter(),
                )) {
                    trace!(
                        "append_datum (N -> N): prev = {:?}, cur = {:?}",
                        old_value,
                        new_value
                    );

                    builder.append_datum(old_value);
                    builder.append_datum(new_value);
                }

                2
            }
        };

        let result_row = self.group_key().chain(&curr_outputs).into_owned_row();

        let prev_outputs = if n_appended_ops == 0 {
            self.prev_outputs.clone()
        } else {
            std::mem::replace(&mut self.prev_outputs, Some(curr_outputs))
        };

        AggChangesInfo {
            n_appended_ops,
            result_row,
            prev_outputs,
        }
    }
}
