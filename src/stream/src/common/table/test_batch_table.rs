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

use futures::pin_mut;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId, TableOption};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::{Distribution, TableIter};

use crate::common::table::state_table::StateTable;

/// There are three struct in relational layer, StateTable, MemTable and CellBasedTable.
/// `StateTable` provides read/write interfaces to the upper layer streaming operator.
/// `MemTable` is an in-memory buffer used to cache operator operations.

// test storage table
#[tokio::test]
async fn test_storage_table_get_row() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
    )
    .await;
    let table: StorageTable<MemoryStateStore> = StorageTable::for_test(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices,
        vec![0, 1, 2],
    );
    let mut epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    state.insert(OwnedRow::new(vec![Some(1_i32.into()), None, None]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(222_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![Some(3_i32.into()), None, None]));

    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(222_i32.into()),
    ]));

    epoch.inc();
    state.commit(epoch).await.unwrap();

    let get_row1_res = table
        .get_row(
            &OwnedRow::new(vec![Some(1_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(OwnedRow::new(vec![Some(1_i32.into()), None, None,]))
    );

    let get_row2_res = table
        .get_row(
            &OwnedRow::new(vec![Some(2_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(
            &OwnedRow::new(vec![Some(3_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(OwnedRow::new(vec![Some(3_i32.into()), None, None]))
    );

    let get_no_exist_res = table
        .get_row(
            &OwnedRow::new(vec![Some(0_i32.into()), Some(00_i32.into())]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

#[tokio::test]
async fn test_storage_table_value_indices() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![
        ColumnId::from(0),
        ColumnId::from(1),
        ColumnId::from(2),
        ColumnId::from(3),
        ColumnId::from(4),
    ];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
        ColumnDesc::unnamed(column_ids[3], DataType::Int32),
        ColumnDesc::unnamed(column_ids[4], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 2_usize];
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let value_indices = vec![1, 3, 4];
    let mut state = StateTable::new_with_value_indices_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
        value_indices.clone(),
    )
    .await;

    let table: StorageTable<MemoryStateStore> = StorageTable::for_test(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices,
        value_indices,
    );
    let mut epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        None,
        Some(11_i32.into()),
        Some(111_i32.into()),
        Some(1111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(22_i32.into()),
        Some(222_i32.into()),
        Some(2222_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        None,
        Some(33_i32.into()),
        Some(333_i32.into()),
        Some(3333_i32.into()),
    ]));

    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(22_i32.into()),
        Some(222_i32.into()),
        Some(2222_i32.into()),
    ]));

    epoch.inc();
    state.commit(epoch).await.unwrap();

    let get_row1_res = table
        .get_row(
            &OwnedRow::new(vec![Some(1_i32.into()), Some(11_i32.into())]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(OwnedRow::new(vec![
            Some(1_i32.into()),
            None,
            Some(11_i32.into()),
            Some(111_i32.into()),
            Some(1111_i32.into())
        ]))
    );

    let get_row2_res = table
        .get_row(
            &OwnedRow::new(vec![Some(2_i32.into()), Some(22_i32.into())]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(
            &OwnedRow::new(vec![Some(3_i32.into()), Some(33_i32.into())]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(OwnedRow::new(vec![
            Some(3_i32.into()),
            None,
            Some(33_i32.into()),
            Some(333_i32.into()),
            Some(3333_i32.into())
        ]))
    );

    let get_no_exist_res = table
        .get_row(
            &OwnedRow::new(vec![Some(0_i32.into()), Some(00_i32.into())]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

#[tokio::test]
async fn test_storage_get_row_for_string() {
    let state_store = MemoryStateStore::new();
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let column_ids = vec![ColumnId::from(1), ColumnId::from(4), ColumnId::from(7)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[1], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[2], DataType::Varchar),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
    )
    .await;
    let table: StorageTable<MemoryStateStore> = StorageTable::for_test(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices,
        vec![0, 1, 2],
    );
    let mut epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    state.insert(OwnedRow::new(vec![
        Some("1".to_string().into()),
        Some("11".to_string().into()),
        Some("111".to_string().into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some("4".to_string().into()),
        Some("44".to_string().into()),
        Some("444".to_string().into()),
    ]));
    state.delete(OwnedRow::new(vec![
        Some("4".to_string().into()),
        Some("44".to_string().into()),
        Some("444".to_string().into()),
    ]));

    epoch.inc();
    state.commit(epoch).await.unwrap();

    let get_row1_res = table
        .get_row(
            &OwnedRow::new(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
            ]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(OwnedRow::new(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into()),
        ]))
    );

    let get_row2_res = table
        .get_row(
            &OwnedRow::new(vec![
                Some("4".to_string().into()),
                Some("44".to_string().into()),
            ]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);
}

#[tokio::test]
async fn test_shuffled_column_id_for_storage_table_get_row() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(3), ColumnId::from(2), ColumnId::from(1)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let pk_indices = vec![0_usize, 1_usize];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
    )
    .await;
    let mut epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    let table: StorageTable<MemoryStateStore> = StorageTable::for_test(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
        vec![0, 1, 2],
    );

    state.insert(OwnedRow::new(vec![Some(1_i32.into()), None, None]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(222_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![Some(3_i32.into()), None, None]));

    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(222_i32.into()),
    ]));

    epoch.inc();
    state.commit(epoch).await.unwrap();

    let get_row1_res = table
        .get_row(
            &OwnedRow::new(vec![Some(1_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(OwnedRow::new(vec![Some(1_i32.into()), None, None,]))
    );

    let get_row2_res = table
        .get_row(
            &OwnedRow::new(vec![Some(2_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(
            &OwnedRow::new(vec![Some(3_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(OwnedRow::new(vec![Some(3_i32.into()), None, None]))
    );

    let get_no_exist_res = table
        .get_row(
            &OwnedRow::new(vec![Some(0_i32.into()), Some(00_i32.into())]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

// test row-based encoding in batch mode
#[tokio::test]
async fn test_row_based_storage_table_point_get_in_batch_mode() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
    )
    .await;
    let column_ids_partial = vec![ColumnId::from(1), ColumnId::from(2)];
    let value_indices: Vec<usize> = vec![0, 1, 2];
    let table = StorageTable::new_partial(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        column_ids_partial,
        order_types.clone(),
        pk_indices,
        Distribution::fallback(),
        TableOption::default(),
        value_indices,
        0,
    );
    let epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    state.insert(OwnedRow::new(vec![Some(1_i32.into()), None, None]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(222_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![Some(3_i32.into()), None, None]));

    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(222_i32.into()),
    ]));
    let next_epoch = EpochPair::new_test_epoch(2);
    state.commit(next_epoch).await.unwrap();

    let get_row1_res = table
        .get_row(
            &OwnedRow::new(vec![Some(1_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();

    // Here only column_ids_partial will be get
    assert_eq!(get_row1_res, Some(OwnedRow::new(vec![None, None,])));

    let get_row2_res = table
        .get_row(
            &OwnedRow::new(vec![Some(2_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(
            &OwnedRow::new(vec![Some(3_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(get_row3_res, Some(OwnedRow::new(vec![None, None])));

    let get_no_exist_res = table
        .get_row(
            &OwnedRow::new(vec![Some(0_i32.into()), Some(00_i32.into())]),
            HummockReadEpoch::Committed(epoch.curr),
        )
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

#[tokio::test]
async fn test_row_based_storage_table_scan_in_batch_mode() {
    let state_store = MemoryStateStore::new();
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let mut state = StateTable::new_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
    )
    .await;
    let column_ids_partial = vec![ColumnId::from(1), ColumnId::from(2)];
    let value_indices: Vec<usize> = vec![0, 1, 2];
    let table = StorageTable::new_partial(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        column_ids_partial,
        order_types.clone(),
        pk_indices,
        Distribution::fallback(),
        TableOption::default(),
        value_indices,
        0,
    );
    let mut epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    epoch.inc();
    state.commit(epoch).await.unwrap();

    let iter = table
        .batch_iter(HummockReadEpoch::Committed(epoch.curr), false)
        .await
        .unwrap();
    pin_mut!(iter);

    let res = iter.next_row().await.unwrap();

    // only scan two columns
    assert_eq!(
        OwnedRow::new(vec![Some(11_i32.into()), Some(111_i32.into())]),
        res.unwrap()
    );

    let res = iter.next_row().await.unwrap();
    assert!(res.is_none());
}

#[tokio::test]
async fn test_batch_scan_with_value_indices() {
    let state_store = MemoryStateStore::new();
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let column_ids = vec![
        ColumnId::from(0),
        ColumnId::from(1),
        ColumnId::from(2),
        ColumnId::from(3),
    ];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
        ColumnDesc::unnamed(column_ids[3], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 2_usize];
    let value_indices: Vec<usize> = vec![1, 3];
    let mut state = StateTable::new_with_value_indices_without_distribution(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
        value_indices.clone(),
    )
    .await;
    let column_ids_partial = vec![ColumnId::from(1), ColumnId::from(2)];

    let table = StorageTable::new_partial(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        column_ids_partial,
        order_types.clone(),
        pk_indices,
        Distribution::fallback(),
        TableOption::default(),
        value_indices,
        0,
    );
    let mut epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
        Some(1111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
        Some(2222_i32.into()),
    ]));
    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
        Some(2222_i32.into()),
    ]));

    epoch.inc();
    state.commit(epoch).await.unwrap();

    let iter = table
        .batch_iter(HummockReadEpoch::Committed(epoch.curr), false)
        .await
        .unwrap();
    pin_mut!(iter);

    let res = iter.next_row().await.unwrap();

    // only scan two columns
    assert_eq!(
        OwnedRow::new(vec![Some(11_i32.into()), Some(111_i32.into())]),
        res.unwrap()
    );

    let res = iter.next_row().await.unwrap();
    assert!(res.is_none());
}
