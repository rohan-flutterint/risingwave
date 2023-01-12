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

//! Provides validation logic for expected errors.
use risingwave_expr::ExprError;

fn is_division_by_zero_err(db_error: &str) -> bool {
    db_error.contains(&ExprError::DivisionByZero.to_string())
}

fn is_numeric_out_of_range_err(db_error: &str) -> bool {
    db_error.contains(&ExprError::NumericOutOfRange.to_string())
}

/// Skip queries with unimplemented features
fn is_unimplemented_error(db_error: &str) -> bool {
    db_error.contains("Feature is not yet implemented")
}

/// This error occurs because we test `implicit` casts as well,
/// generated expressions may be ambiguous as a result,
/// if there are multiple candidates signatures.
/// Additionally.
fn not_unique_error(db_error: &str) -> bool {
    db_error.contains("Bind error") && db_error.contains("is not unique")
}

// Do not support streaming nested-loop join, it is expensive.
fn is_nested_loop_join_error(db_error: &str) -> bool {
    db_error.contains("Not supported: streaming nested-loop join")
}

/// Certain errors are permitted to occur. This is because:
/// 1. It is more complex to generate queries without these errors.
/// 2. These errors seldom occur, skipping them won't affect overall effectiveness of sqlsmith.
pub fn is_permissible_error(db_error: &str) -> bool {
    is_numeric_out_of_range_err(db_error)
        || is_division_by_zero_err(db_error)
        || is_unimplemented_error(db_error)
        || not_unique_error(db_error)
        || is_nested_loop_join_error(db_error)
}
