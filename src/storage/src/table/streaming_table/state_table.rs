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

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Bound;
use std::ops::Bound::*;
use std::sync::Arc;

use async_stack_trace::StackTrace;
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::{izip, Itertools};
use risingwave_common::array::{Op, StreamChunk, Vis};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, TableId, TableOption};
use risingwave_common::row::{CompactedRow, Row, RowDeserializer};
use risingwave_common::types::VirtualNode;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::{
    end_bound_of_prefix, prefixed_range, range_of_prefix, start_bound_of_excluded_prefix,
};
use risingwave_pb::catalog::Table;
use tracing::trace;

use super::mem_table::{MemTable, MemTableIter, RowOp};
use crate::error::{StorageError, StorageResult};
use crate::keyspace::ExtractTableKeyIterator;
use crate::row_serde::row_serde_util::{
    deserialize_pk_with_vnode, serialize_pk, serialize_pk_with_vnode,
};
use crate::storage_value::StorageValue;
use crate::store::{LocalStateStore, ReadOptions, WriteOptions};
use crate::table::streaming_table::mem_table::MemTableError;
use crate::table::{compute_chunk_vnode, compute_vnode, Distribution};
use crate::{Keyspace, StateStore, StateStoreIter};

/// `StateTable` is the interface accessing relational data in KV(`StateStore`) with
/// row-based encoding.
#[derive(Clone)]
pub struct StateTable<S: StateStore> {
    /// buffer row operations.
    mem_table: MemTable,

    /// write into state store.
    keyspace: Keyspace<S::Local>,

    /// Used for serializing and deserializing the primary key.
    pk_serde: OrderedRowSerde,

    /// Row deserializer with value encoding
    row_deserializer: RowDeserializer,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    pk_indices: Vec<usize>,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    dist_key_indices: Vec<usize>,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the primary key columns by `pk_indices`.
    dist_key_in_pk_indices: Vec<usize>,

    /// Virtual nodes that the table is partitioned into.
    ///
    /// Only the rows whose vnode of the primary key is in this set will be visible to the
    /// executor. The table will also check whether the written rows
    /// conform to this partition.
    vnodes: Arc<Bitmap>,

    /// Used for catalog table_properties
    table_option: TableOption,

    /// If true, sanity check is disabled on this table.
    disable_sanity_check: bool,

    /// an optional column index which is the vnode of each row computed by the table's consistent
    /// hash distribution
    vnode_col_idx_in_pk: Option<usize>,

    value_indices: Option<Vec<usize>>,

    /// the epoch flush to the state store last time
    epoch: Option<EpochPair>,
}

// initialize
impl<S: StateStore> StateTable<S> {
    /// Create state table from table catalog and store.
    pub async fn from_table_catalog(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
    ) -> Self {
        let table_id = TableId::new(table_catalog.id);
        let table_columns: Vec<ColumnDesc> = table_catalog
            .columns
            .iter()
            .map(|col| col.column_desc.as_ref().unwrap().into())
            .collect();
        let order_types: Vec<OrderType> = table_catalog
            .pk
            .iter()
            .map(|col_order| {
                OrderType::from_prost(
                    &risingwave_pb::plan_common::OrderType::from_i32(col_order.order_type).unwrap(),
                )
            })
            .collect();
        let dist_key_indices: Vec<usize> = table_catalog
            .distribution_key
            .iter()
            .map(|dist_index| *dist_index as usize)
            .collect();

        let pk_indices = table_catalog
            .pk
            .iter()
            .map(|col_order| col_order.index as usize)
            .collect_vec();

        let dist_key_in_pk_indices = dist_key_indices
            .iter()
            .map(|&di| {
                pk_indices
                    .iter()
                    .position(|&pi| di == pi)
                    .unwrap_or_else(|| {
                        panic!(
                            "distribution key {:?} must be a subset of primary key {:?}",
                            dist_key_indices, pk_indices
                        )
                    })
            })
            .collect_vec();

        let local_state_store = store.new_local(table_id).await;
        let keyspace = Keyspace::table_root(local_state_store, table_id);

        let pk_data_types = pk_indices
            .iter()
            .map(|i| table_columns[*i].data_type.clone())
            .collect();
        let pk_serde = OrderedRowSerde::new(pk_data_types, order_types);

        let Distribution {
            dist_key_indices,
            vnodes,
        } = match vnodes {
            Some(vnodes) => Distribution {
                dist_key_indices,
                vnodes,
            },
            None => Distribution::fallback(),
        };

        let vnode_col_idx_in_pk = table_catalog
            .vnode_col_idx
            .as_ref()
            .and_then(|vnode_col_idx| {
                let vnode_col_idx = vnode_col_idx.index as usize;
                pk_indices.iter().position(|&i| vnode_col_idx == i)
            });
        let input_value_indices = table_catalog
            .value_indices
            .iter()
            .map(|val| *val as usize)
            .collect_vec();

        let data_types = input_value_indices
            .iter()
            .map(|idx| table_columns[*idx].data_type.clone())
            .collect();

        let no_shuffle_value_indices = (0..table_columns.len()).collect_vec();

        // if value_indices is the no shuffle full columns and
        let value_indices = match input_value_indices.len() == table_columns.len()
            && input_value_indices == no_shuffle_value_indices
        {
            true => None,
            false => Some(input_value_indices),
        };
        Self {
            mem_table: MemTable::new(),
            keyspace,
            pk_serde,
            row_deserializer: RowDeserializer::new(data_types),
            pk_indices: pk_indices.to_vec(),
            dist_key_indices,
            dist_key_in_pk_indices,
            vnodes,
            table_option: TableOption::build_table_option(table_catalog.get_properties()),
            disable_sanity_check: false,
            vnode_col_idx_in_pk,
            value_indices,
            epoch: None,
        }
    }

    /// Create a state table without distribution, used for unit tests.
    pub async fn new_without_distribution(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
    ) -> Self {
        Self::new_with_distribution(
            store,
            table_id,
            columns,
            order_types,
            pk_indices,
            Distribution::fallback(),
            None,
        )
        .await
    }

    /// Create a state table with given `value_indices`, used for unit tests.
    pub async fn new_without_distribution_partial(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        value_indices: Vec<usize>,
    ) -> Self {
        Self::new_with_distribution(
            store,
            table_id,
            columns,
            order_types,
            pk_indices,
            Distribution::fallback(),
            Some(value_indices),
        )
        .await
    }

    /// Create a state table with distribution specified with `distribution`. Should use
    /// `Distribution::fallback()` for tests.
    pub async fn new_with_distribution(
        store: S,
        table_id: TableId,
        table_columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        Distribution {
            dist_key_indices,
            vnodes,
        }: Distribution,
        value_indices: Option<Vec<usize>>,
    ) -> Self {
        let local_state_store = store.new_local(table_id).await;
        let keyspace = Keyspace::table_root(local_state_store, table_id);

        let pk_data_types = pk_indices
            .iter()
            .map(|i| table_columns[*i].data_type.clone())
            .collect();
        let pk_serde = OrderedRowSerde::new(pk_data_types, order_types);

        let data_types = match &value_indices {
            Some(value_indices) => value_indices
                .iter()
                .map(|idx| table_columns[*idx].data_type.clone())
                .collect(),
            None => table_columns.iter().map(|c| c.data_type.clone()).collect(),
        };
        let dist_key_in_pk_indices = dist_key_indices
            .iter()
            .map(|&di| {
                pk_indices
                    .iter()
                    .position(|&pi| di == pi)
                    .unwrap_or_else(|| {
                        panic!(
                            "distribution key {:?} must be a subset of primary key {:?}",
                            dist_key_indices, pk_indices
                        )
                    })
            })
            .collect_vec();
        Self {
            mem_table: MemTable::new(),
            keyspace,
            pk_serde,
            row_deserializer: RowDeserializer::new(data_types),
            pk_indices,
            dist_key_indices,
            dist_key_in_pk_indices,
            vnodes,
            table_option: Default::default(),
            disable_sanity_check: false,
            vnode_col_idx_in_pk: None,
            value_indices,
            epoch: None,
        }
    }

    /// Disable sanity check on this storage table.
    pub fn disable_sanity_check(&mut self) {
        self.disable_sanity_check = true;
    }

    fn table_id(&self) -> TableId {
        self.keyspace.table_id()
    }

    /// get the newest epoch of the state store and panic if the `init_epoch()` has never be called
    pub fn init_epoch(&mut self, epoch: EpochPair) {
        match self.epoch {
            Some(prev_epoch) => {
                panic!(
                    "init the state table's epoch twice, table_id: {}, prev_epoch: {}, new_epoch: {}",
                    self.table_id(),
                    prev_epoch.curr,
                    epoch.curr
                )
            }
            None => {
                self.epoch = Some(epoch);
            }
        }
    }

    /// get the newest epoch of the state store and panic if the `init_epoch()` has never be called
    pub fn epoch(&self) -> u64 {
        self.epoch.unwrap_or_else(|| panic!("try to use state table's epoch, but the init_epoch() has not been called, table_id: {}", self.table_id())).curr
    }

    /// get the previous epoch of the state store and panic if the `init_epoch()` has never be
    /// called
    pub fn prev_epoch(&self) -> u64 {
        self.epoch.unwrap_or_else(|| panic!("try to use state table's epoch, but the init_epoch() has not been called, table_id: {}", self.table_id())).prev
    }

    /// Get the vnode value with given (prefix of) primary key
    fn compute_prefix_vnode(&self, pk_prefix: &Row) -> VirtualNode {
        let prefix_len = pk_prefix.0.len();
        if let Some(vnode_col_idx_in_pk) = self.vnode_col_idx_in_pk {
            let vnode = pk_prefix.0.get(vnode_col_idx_in_pk).unwrap();
            vnode.clone().unwrap().into_int16() as _
        } else {
            // For streaming, the given prefix must be enough to calculate the vnode
            assert!(self.dist_key_in_pk_indices.iter().all(|&d| d < prefix_len));
            compute_vnode(pk_prefix, &self.dist_key_in_pk_indices, &self.vnodes)
        }
    }

    /// Get the vnode value of the given row
    pub fn compute_vnode(&self, row: &Row) -> VirtualNode {
        compute_vnode(row, &self.dist_key_indices, &self.vnodes)
    }

    // TODO: remove, should not be exposed to user
    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    pub fn pk_serde(&self) -> &OrderedRowSerde {
        &self.pk_serde
    }

    pub fn dist_key_indices(&self) -> &[usize] {
        &self.dist_key_indices
    }

    pub fn vnodes(&self) -> &Arc<Bitmap> {
        &self.vnodes
    }

    pub fn value_indices(&self) -> &Option<Vec<usize>> {
        &self.value_indices
    }

    pub fn is_dirty(&self) -> bool {
        self.mem_table.is_dirty()
    }

    pub fn vnode_bitmap(&self) -> &Bitmap {
        &self.vnodes
    }
}

const ENABLE_SANITY_CHECK: bool = cfg!(debug_assertions);

// point get
impl<S: StateStore> StateTable<S> {
    /// Get a single row from state table.
    pub async fn get_row<'a>(&'a self, pk: &'a Row) -> StorageResult<Option<Row>> {
        let compacted_row: Option<CompactedRow> = self.get_compacted_row(pk).await?;
        match compacted_row {
            Some(compacted_row) => {
                let row = self
                    .row_deserializer
                    .deserialize(compacted_row.row.as_ref())?;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    /// Get a compacted row from state table.
    pub async fn get_compacted_row<'a>(
        &'a self,
        pk: &'a Row,
    ) -> StorageResult<Option<CompactedRow>> {
        let serialized_pk =
            serialize_pk_with_vnode(pk, &self.pk_serde, self.compute_prefix_vnode(pk));
        let mem_table_res = self.mem_table.get_row_op(&serialized_pk);

        match mem_table_res {
            Some(row_op) => match row_op {
                RowOp::Insert(row_bytes) => Ok(Some(CompactedRow {
                    row: row_bytes.to_vec(),
                })),
                RowOp::Delete(_) => Ok(None),
                RowOp::Update((_, row_bytes)) => Ok(Some(CompactedRow {
                    row: row_bytes.to_vec(),
                })),
            },
            None => {
                assert!(pk.size() <= self.pk_indices.len());
                let key_indices = (0..pk.size())
                    .into_iter()
                    .map(|index| self.pk_indices[index])
                    .collect_vec();
                let read_options = ReadOptions {
                    prefix_hint: None,
                    check_bloom_filter: self.dist_key_indices == key_indices,
                    retention_seconds: self.table_option.retention_seconds,
                    table_id: self.keyspace.table_id(),
                    ignore_range_tombstone: false,
                };
                if let Some(storage_row_bytes) = self
                    .keyspace
                    .get(&serialized_pk, self.epoch(), read_options)
                    .await?
                {
                    Ok(Some(CompactedRow {
                        row: storage_row_bytes.to_vec(),
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Update the vnode bitmap of the state table, returns the previous vnode bitmap.
    #[must_use = "the executor should decide whether to manipulate the cache based on the previous vnode bitmap"]
    pub fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
        assert!(
            !self.is_dirty(),
            "vnode bitmap should only be updated when state table is clean"
        );
        if self.dist_key_indices.is_empty() {
            assert_eq!(
                new_vnodes, self.vnodes,
                "should not update vnode bitmap for singleton table"
            );
        }
        assert_eq!(self.vnodes.len(), new_vnodes.len());

        std::mem::replace(&mut self.vnodes, new_vnodes)
    }
}

// write
impl<S: StateStore> StateTable<S> {
    fn handle_mem_table_error(&self, e: MemTableError) {
        match e {
            MemTableError::Conflict { key, prev, new } => {
                let (vnode, key) = deserialize_pk_with_vnode(&key, &self.pk_serde).unwrap();
                panic!(
                    "mem-table operation conflicts! table_id: {}, vnode: {}, key: {:?}, prev: {}, new: {}",
                    self.table_id(),
                    vnode,
                    &key,
                    prev.debug_fmt(&self.row_deserializer),
                    new.debug_fmt(&self.row_deserializer),
                )
            }
        }
    }

    /// Insert a row into state table. Must provide a full row corresponding to the column desc of
    /// the table.
    pub fn insert(&mut self, value: Row) {
        let pk = value.by_indices(self.pk_indices());

        let key_bytes =
            serialize_pk_with_vnode(&pk, &self.pk_serde, self.compute_prefix_vnode(&pk));
        let value_bytes = value.serialize(&self.value_indices);
        self.mem_table
            .insert(key_bytes, value_bytes)
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }

    /// Delete a row from state table. Must provide a full row of old value corresponding to the
    /// column desc of the table.
    pub fn delete(&mut self, old_value: Row) {
        let pk = old_value.by_indices(self.pk_indices());
        let key_bytes =
            serialize_pk_with_vnode(&pk, &self.pk_serde, self.compute_prefix_vnode(&pk));
        let value_bytes = old_value.serialize(&self.value_indices);
        self.mem_table
            .delete(key_bytes, value_bytes)
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }

    /// Update a row. The old and new value should have the same pk.
    pub fn update(&mut self, old_value: Row, new_value: Row) {
        let old_pk = old_value.by_indices(self.pk_indices());
        let new_pk = new_value.by_indices(self.pk_indices());
        debug_assert_eq!(old_pk, new_pk);

        let new_key_bytes =
            serialize_pk_with_vnode(&new_pk, &self.pk_serde, self.compute_prefix_vnode(&new_pk));

        self.mem_table
            .update(
                new_key_bytes,
                old_value.serialize(&self.value_indices),
                new_value.serialize(&self.value_indices),
            )
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }

    /// Write batch with a `StreamChunk` which should have the same schema with the table.
    // allow(izip, which use zip instead of zip_eq)
    #[allow(clippy::disallowed_methods)]
    pub fn write_chunk(&mut self, chunk: StreamChunk) {
        let (chunk, op) = chunk.into_parts();

        let mut vnode_and_pks = vec![vec![]; chunk.capacity()];

        compute_chunk_vnode(&chunk, &self.dist_key_indices, &self.vnodes)
            .into_iter()
            .zip_eq(vnode_and_pks.iter_mut())
            .for_each(|(vnode, vnode_and_pk)| vnode_and_pk.extend(vnode.to_be_bytes()));

        let value_chunk = if let Some(ref value_indices) = self.value_indices {
            chunk.clone().reorder_columns(value_indices)
        } else {
            chunk.clone()
        };
        let values = value_chunk.serialize();

        let key_chunk = chunk.reorder_columns(self.pk_indices());
        key_chunk
            .rows_with_holes()
            .zip_eq(vnode_and_pks.iter_mut())
            .for_each(|(r, vnode_and_pk)| {
                if let Some(r) = r {
                    self.pk_serde.serialize_ref(r, vnode_and_pk);
                }
            });

        let (_, vis) = key_chunk.into_parts();
        match vis {
            Vis::Bitmap(vis) => {
                for ((op, key, value), vis) in izip!(op, vnode_and_pks, values).zip_eq(vis.iter()) {
                    if vis {
                        match op {
                            Op::Insert | Op::UpdateInsert => self.mem_table.insert(key, value),
                            Op::Delete | Op::UpdateDelete => self.mem_table.delete(key, value),
                        }
                        .unwrap_or_else(|e| self.handle_mem_table_error(e))
                    }
                }
            }
            Vis::Compact(_) => {
                for (op, key, value) in izip!(op, vnode_and_pks, values) {
                    match op {
                        Op::Insert | Op::UpdateInsert => self.mem_table.insert(key, value),
                        Op::Delete | Op::UpdateDelete => self.mem_table.delete(key, value),
                    }
                    .unwrap_or_else(|e| self.handle_mem_table_error(e))
                }
            }
        }
    }

    fn update_epoch(&mut self, new_epoch: EpochPair) {
        assert!(
            self.epoch() <= new_epoch.curr,
            "state table commit a committed epoch, table_id: {}, prev_epoch: {}, new_epoch: {}",
            self.table_id(),
            self.epoch(),
            new_epoch.curr
        );
        self.epoch = Some(new_epoch);
    }

    pub async fn commit(&mut self, new_epoch: EpochPair) -> StorageResult<()> {
        assert_eq!(self.epoch(), new_epoch.prev);
        let mem_table = std::mem::take(&mut self.mem_table).into_parts();
        self.batch_write_rows(mem_table, new_epoch.prev).await?;
        self.update_epoch(new_epoch);
        Ok(())
    }

    /// used for unit test, and do not need to assert epoch.
    pub async fn commit_for_test(&mut self, new_epoch: EpochPair) -> StorageResult<()> {
        let mem_table = std::mem::take(&mut self.mem_table).into_parts();
        self.batch_write_rows(mem_table, new_epoch.prev).await?;
        self.update_epoch(new_epoch);
        Ok(())
    }

    // TODO(st1page): maybe we should extract a pub struct to do it
    /// just specially used by those state table read-only and after the call the data
    /// in the epoch will be visible
    pub fn commit_no_data_expected(&mut self, new_epoch: EpochPair) {
        assert_eq!(self.epoch(), new_epoch.prev);
        assert!(!self.is_dirty());
        self.update_epoch(new_epoch);
    }

    /// Write to state store.
    async fn batch_write_rows(
        &mut self,
        buffer: BTreeMap<Vec<u8>, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        let mut write_batch = self.keyspace.start_write_batch(WriteOptions {
            epoch,
            table_id: self.table_id(),
        });
        for (pk, row_op) in buffer {
            match row_op {
                // Currently, some executors do not strictly comply with these semantics. As a
                // workaround you may call disable the check by calling `.disable_sanity_check()` on
                // state table.
                RowOp::Insert(row) => {
                    if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
                        self.do_insert_sanity_check(&pk, &row, epoch).await?;
                    }
                    write_batch.put(pk, StorageValue::new_put(row));
                }
                RowOp::Delete(row) => {
                    if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
                        self.do_delete_sanity_check(&pk, &row, epoch).await?;
                    }
                    write_batch.delete(pk);
                }
                RowOp::Update((old_row, new_row)) => {
                    if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
                        self.do_update_sanity_check(&pk, &old_row, &new_row, epoch)
                            .await?;
                    }
                    write_batch.put(pk, StorageValue::new_put(new_row));
                }
            }
        }
        write_batch.ingest().await?;
        Ok(())
    }

    /// Make sure the key to insert should not exist in storage.
    async fn do_insert_sanity_check(
        &self,
        key: &[u8],
        value: &[u8],
        epoch: u64,
    ) -> StorageResult<()> {
        let read_options = ReadOptions {
            prefix_hint: None,
            check_bloom_filter: false,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.keyspace.table_id(),
            ignore_range_tombstone: false,
        };
        let stored_value = self.keyspace.get(key, epoch, read_options).await?;

        if let Some(stored_value) = stored_value {
            let (vnode, key) = deserialize_pk_with_vnode(key, &self.pk_serde).unwrap();
            let in_storage = self.row_deserializer.deserialize(stored_value).unwrap();
            let to_write = self.row_deserializer.deserialize(value).unwrap();
            panic!(
                "overwrites an existing key!\ntable_id: {}, vnode: {}, key: {:?}\nvalue in storage: {:?}\nvalue to write: {:?}",
                self.table_id(),
                vnode,
                key,
                in_storage,
                to_write,
            );
        }
        Ok(())
    }

    /// Make sure that the key to delete should exist in storage and the value should be matched.
    async fn do_delete_sanity_check(
        &self,
        key: &[u8],
        old_row: &[u8],
        epoch: u64,
    ) -> StorageResult<()> {
        let read_options = ReadOptions {
            prefix_hint: None,
            check_bloom_filter: false,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.keyspace.table_id(),
            ignore_range_tombstone: false,
        };
        let stored_value = self.keyspace.get(key, epoch, read_options).await?;

        if stored_value.is_none() || stored_value.as_ref().unwrap() != old_row {
            let (vnode, key) = deserialize_pk_with_vnode(key, &self.pk_serde).unwrap();
            let stored_row =
                stored_value.map(|bytes| self.row_deserializer.deserialize(bytes).unwrap());
            let to_delete = self.row_deserializer.deserialize(old_row).unwrap();
            panic!(
                "inconsistent delete!\ntable_id: {}, vnode: {}, key: {:?}\nstored value: {:?}\nexpected value: {:?}",
                self.table_id(),
                vnode,
                key,
                stored_row,
                to_delete,
            );
        }
        Ok(())
    }

    /// Make sure that the key to update should exist in storage and the value should be matched
    async fn do_update_sanity_check(
        &self,
        key: &[u8],
        old_row: &[u8],
        new_row: &[u8],
        epoch: u64,
    ) -> StorageResult<()> {
        let read_options = ReadOptions {
            prefix_hint: None,
            ignore_range_tombstone: false,
            check_bloom_filter: false,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.keyspace.table_id(),
        };
        let stored_value = self.keyspace.get(key, epoch, read_options).await?;

        if stored_value.is_none() || stored_value.as_ref().unwrap() != old_row {
            let (vnode, key) = deserialize_pk_with_vnode(key, &self.pk_serde).unwrap();
            let expected_row = self.row_deserializer.deserialize(old_row).unwrap();
            let stored_row =
                stored_value.map(|bytes| self.row_deserializer.deserialize(bytes).unwrap());
            let new_row = self.row_deserializer.deserialize(new_row).unwrap();
            panic!(
                "inconsistent update!\ntable_id: {}, vnode: {}, key: {:?}\nstored value: {:?}\nexpected value: {:?}\nnew value: {:?}",
                self.table_id(),
                vnode,
                key,
                stored_row,
                expected_row,
                new_row,
            );
        }

        Ok(())
    }
}

fn get_second<T, U>(arg: StorageResult<(T, U)>) -> StorageResult<U> {
    arg.map(|x| x.1)
}

// Iterator functions
impl<S: StateStore> StateTable<S> {
    /// This function scans rows from the relational table.
    pub async fn iter(&self) -> StorageResult<RowStream<'_, S>> {
        self.iter_with_pk_prefix(Row::empty()).await
    }

    /// This function scans rows from the relational table with specific `pk_prefix`.
    pub async fn iter_with_pk_prefix<'a>(
        &'a self,
        pk_prefix: &'a Row,
    ) -> StorageResult<RowStream<'a, S>> {
        let (mem_table_iter, storage_iter_stream) = self
            .iter_with_pk_prefix_inner(pk_prefix, self.epoch())
            .await?;

        let storage_iter = storage_iter_stream.into_stream();
        Ok(
            StateTableRowIter::new(mem_table_iter, storage_iter, self.row_deserializer.clone())
                .into_stream()
                .map(get_second),
        )
    }

    /// This function scans rows from the relational table with specific `pk_prefix`.
    async fn iter_with_pk_range_inner<'a>(
        &'a self,
        pk_range: &'a (Bound<Row>, Bound<Row>),
        // Optional vnode that returns an iterator only over the given range under that vnode.
        // For now, we require this parameter, and will panic. In the future, when `None`, we can
        // iterate over each vnode that the `StateTable` owns.
        vnode: u8,
    ) -> StorageResult<(MemTableIter<'_>, StorageIterInner<S::Local>)> {
        let memcomparable_range = prefix_range_to_memcomparable(&self.pk_serde, pk_range);

        let memcomparable_range_with_vnode = prefixed_range(memcomparable_range, &[vnode]);

        // TODO: provide a trace of useful params.

        let (mem_table_iter, storage_iter_stream) = self
            .iter_inner(memcomparable_range_with_vnode, None, self.epoch())
            .await?;

        Ok((mem_table_iter, storage_iter_stream))
    }

    pub async fn iter_with_pk_range<'a>(
        &'a self,
        pk_range: &'a (Bound<Row>, Bound<Row>),
        // Optional vnode that returns an iterator only over the given range under that vnode.
        // For now, we require this parameter, and will panic. In the future, when `None`, we can
        // iterate over each vnode that the `StateTable` owns.
        vnode: u8,
    ) -> StorageResult<RowStream<'a, S>> {
        let (mem_table_iter, storage_iter_stream) =
            self.iter_with_pk_range_inner(pk_range, vnode).await?;
        let storage_iter = storage_iter_stream.into_stream();
        Ok(
            StateTableRowIter::new(mem_table_iter, storage_iter, self.row_deserializer.clone())
                .into_stream()
                .map(get_second),
        )
    }

    pub async fn iter_key_and_val_with_pk_range<'a>(
        &'a self,
        pk_range: &'a (Bound<Row>, Bound<Row>),
        // Optional vnode that returns an iterator only over the given range under that vnode.
        // For now, we require this parameter, and will panic. In the future, when `None`, we can
        // iterate over each vnode that the `StateTable` owns.
        vnode: u8,
    ) -> StorageResult<RowStreamWithPk<'a, S>> {
        let (mem_table_iter, storage_iter_stream) =
            self.iter_with_pk_range_inner(pk_range, vnode).await?;
        let storage_iter = storage_iter_stream.into_stream();
        Ok(
            StateTableRowIter::new(mem_table_iter, storage_iter, self.row_deserializer.clone())
                .into_stream(),
        )
    }

    /// This function scans rows from the relational table with specific `pk_prefix`.
    pub async fn iter_prev_epoch_with_pk_prefix<'a>(
        &'a self,
        pk_prefix: &'a Row,
    ) -> StorageResult<RowStream<'a, S>> {
        let (mem_table_iter, storage_iter_stream) = self
            .iter_with_pk_prefix_inner(pk_prefix, self.prev_epoch())
            .await?;

        let storage_iter = storage_iter_stream.into_stream();
        Ok(
            StateTableRowIter::new(mem_table_iter, storage_iter, self.row_deserializer.clone())
                .into_stream()
                .map(get_second),
        )
    }

    /// This function scans rows from the relational table with specific `pk_prefix`, return both
    /// key and value.
    pub async fn iter_key_and_val<'a>(
        &'a self,
        pk_prefix: &'a Row,
    ) -> StorageResult<RowStreamWithPk<'a, S>> {
        let (mem_table_iter, storage_iter_stream) = self
            .iter_with_pk_prefix_inner(pk_prefix, self.epoch())
            .await?;
        let storage_iter = storage_iter_stream.into_stream();

        Ok(
            StateTableRowIter::new(mem_table_iter, storage_iter, self.row_deserializer.clone())
                .into_stream(),
        )
    }

    async fn iter_with_pk_prefix_inner<'a>(
        &'a self,
        pk_prefix: &'a Row,
        epoch: u64,
    ) -> StorageResult<(MemTableIter<'_>, StorageIterInner<S::Local>)> {
        let prefix_serializer = self.pk_serde.prefix(pk_prefix.size());
        let encoded_prefix = serialize_pk(pk_prefix, &prefix_serializer);
        let encoded_key_range = range_of_prefix(&encoded_prefix);

        // We assume that all usages of iterating the state table only access a single vnode.
        // If this assertion fails, then something must be wrong with the operator implementation or
        // the distribution derivation from the optimizer.
        let vnode = self.compute_prefix_vnode(pk_prefix).to_be_bytes();
        let encoded_key_range_with_vnode = prefixed_range(encoded_key_range, &vnode);

        // Construct prefix hint for prefix bloom filter.
        let pk_prefix_indices = &self.pk_indices[..pk_prefix.size()];
        let prefix_hint = {
            if self.dist_key_indices.is_empty() || self.dist_key_indices != pk_prefix_indices {
                None
            } else {
                Some([&vnode, &encoded_prefix[..]].concat())
            }
        };

        trace!(
            table_id = ?self.table_id(),
            ?prefix_hint, ?encoded_key_range_with_vnode, ?pk_prefix,
            dist_key_indices = ?self.dist_key_indices, ?pk_prefix_indices,
            "storage_iter_with_prefix"
        );

        self.iter_inner(encoded_key_range_with_vnode, prefix_hint, epoch)
            .await
    }

    async fn iter_inner(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        prefix_hint: Option<Vec<u8>>,
        epoch: u64,
    ) -> StorageResult<(MemTableIter<'_>, StorageIterInner<S::Local>)> {
        // Mem table iterator.
        let mem_table_iter = self.mem_table.iter(key_range.clone());

        let check_bloom_filter = prefix_hint.is_some();
        let read_options = ReadOptions {
            prefix_hint,
            check_bloom_filter,
            ignore_range_tombstone: false,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.keyspace.table_id(),
        };

        // Storage iterator.
        let storage_iter = StorageIterInner::<S::Local>::new(
            &self.keyspace,
            epoch,
            key_range,
            read_options,
            self.row_deserializer.clone(),
        )
        .await?;

        Ok((mem_table_iter, storage_iter))
    }

    pub fn get_vnodes(&self) -> Arc<Bitmap> {
        self.vnodes.clone()
    }
}

pub type RowStream<'a, S: StateStore> = impl Stream<Item = StorageResult<Cow<'a, Row>>>;
pub type RowStreamWithPk<'a, S: StateStore> =
    impl Stream<Item = StorageResult<(Cow<'a, Vec<u8>>, Cow<'a, Row>)>>;

/// `StateTableRowIter` is able to read the just written data (uncommitted data).
/// It will merge the result of `mem_table_iter` and `state_store_iter`.
struct StateTableRowIter<'a, M, C> {
    mem_table_iter: M,
    storage_iter: C,
    _phantom: PhantomData<&'a ()>,
    deserializer: RowDeserializer,
}

impl<'a, M, C> StateTableRowIter<'a, M, C>
where
    M: Iterator<Item = (&'a Vec<u8>, &'a RowOp)>,
    C: Stream<Item = StorageResult<(Vec<u8>, Row)>>,
{
    fn new(mem_table_iter: M, storage_iter: C, deserializer: RowDeserializer) -> Self {
        Self {
            mem_table_iter,
            storage_iter,
            _phantom: PhantomData,
            deserializer,
        }
    }

    /// This function scans kv pairs from the `shared_storage` and
    /// memory(`mem_table`) with optional pk_bounds. If a record exist in both `shared_storage` and
    /// `mem_table`, result `mem_table` is returned according to the operation(RowOp) on it.
    #[try_stream(ok = (Cow<'a, Vec<u8>>, Cow<'a, Row>), error = StorageError)]
    async fn into_stream(self) {
        let storage_iter = self.storage_iter.peekable();
        pin_mut!(storage_iter);

        let mut mem_table_iter = self.mem_table_iter.fuse().peekable();

        loop {
            match (storage_iter.as_mut().peek().await, mem_table_iter.peek()) {
                (None, None) => break,
                // The mem table side has come to an end, return data from the shared storage.
                (Some(_), None) => {
                    let (pk, row) = storage_iter.next().await.unwrap()?;
                    yield (Cow::Owned(pk), Cow::Owned(row))
                }
                // The stream side has come to an end, return data from the mem table.
                (None, Some(_)) => {
                    let (pk, row_op) = mem_table_iter.next().unwrap();
                    match row_op {
                        RowOp::Insert(row_bytes) | RowOp::Update((_, row_bytes)) => {
                            let row = self.deserializer.deserialize(row_bytes.as_ref())?;

                            yield (Cow::Borrowed(pk), Cow::Owned(row))
                        }
                        _ => {}
                    }
                }
                (Some(Ok((storage_pk, _))), Some((mem_table_pk, _))) => {
                    match storage_pk.cmp(mem_table_pk) {
                        Ordering::Less => {
                            // yield data from storage
                            let (pk, row) = storage_iter.next().await.unwrap()?;
                            yield (Cow::Owned(pk), Cow::Owned(row));
                        }
                        Ordering::Equal => {
                            // both memtable and storage contain the key, so we advance both
                            // iterators and return the data in memory.

                            let (pk, row_op) = mem_table_iter.next().unwrap();
                            let (_, old_row_in_storage) = storage_iter.next().await.unwrap()?;
                            match row_op {
                                RowOp::Insert(row_bytes) => {
                                    let row = self.deserializer.deserialize(row_bytes.as_ref())?;

                                    yield (Cow::Borrowed(pk), Cow::Owned(row));
                                }
                                RowOp::Delete(_) => {}
                                RowOp::Update((old_row_bytes, new_row_bytes)) => {
                                    let old_row =
                                        self.deserializer.deserialize(old_row_bytes.as_ref())?;
                                    let new_row =
                                        self.deserializer.deserialize(new_row_bytes.as_ref())?;

                                    debug_assert!(old_row == old_row_in_storage);

                                    yield (Cow::Borrowed(pk), Cow::Owned(new_row));
                                }
                            }
                        }
                        Ordering::Greater => {
                            // yield data from mem table
                            let (pk, row_op) = mem_table_iter.next().unwrap();

                            match row_op {
                                RowOp::Insert(row_bytes) => {
                                    let row = self.deserializer.deserialize(row_bytes.as_ref())?;

                                    yield (Cow::Borrowed(pk), Cow::Owned(row));
                                }
                                RowOp::Delete(_) => {}
                                RowOp::Update(_) => unreachable!(
                                    "memtable update should always be paired with a storage key"
                                ),
                            }
                        }
                    }
                }
                (Some(Err(_)), Some(_)) => {
                    // Throw the error.
                    return Err(storage_iter.next().await.unwrap().unwrap_err());
                }
            }
        }
    }
}

struct StorageIterInner<S: LocalStateStore> {
    /// An iterator that returns raw bytes from storage.
    iter: ExtractTableKeyIterator<S::Iter>,

    deserializer: RowDeserializer,
}

impl<S: LocalStateStore> StorageIterInner<S> {
    async fn new(
        keyspace: &Keyspace<S>,
        epoch: u64,
        raw_key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_options: ReadOptions,
        deserializer: RowDeserializer,
    ) -> StorageResult<Self> {
        let iter = keyspace
            .iter_with_range(raw_key_range, epoch, read_options)
            .await?;
        let iter = Self { iter, deserializer };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    async fn into_stream(mut self) {
        while let Some((key, value)) = self
            .iter
            .next()
            .verbose_stack_trace("storage_table_iter_next")
            .await?
        {
            let row = self.deserializer.deserialize(value.as_ref())?;
            yield (key.to_vec(), row);
        }
    }
}

pub fn prefix_range_to_memcomparable(
    pk_serde: &OrderedRowSerde,
    range: &(Bound<Row>, Bound<Row>),
) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    (
        to_memcomparable(pk_serde, &range.0, false),
        to_memcomparable(pk_serde, &range.1, true),
    )
}

fn to_memcomparable(
    pk_serde: &OrderedRowSerde,
    bound: &Bound<Row>,
    is_upper: bool,
) -> Bound<Vec<u8>> {
    let serialize_pk_prefix = |pk_prefix: &Row| {
        let prefix_serializer = pk_serde.prefix(pk_prefix.size());
        serialize_pk(pk_prefix, &prefix_serializer)
    };
    match &bound {
        Unbounded => Unbounded,
        Included(r) => {
            let serialized = serialize_pk_prefix(r);
            if is_upper {
                end_bound_of_prefix(&serialized)
            } else {
                Included(serialized)
            }
        }
        Excluded(r) => {
            let serialized = serialize_pk_prefix(r);
            if !is_upper {
                // if lower
                start_bound_of_excluded_prefix(&serialized)
            } else {
                Excluded(serialized)
            }
        }
    }
}
