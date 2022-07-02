//
//
// test_base.rs
// Copyright (C) 2022 db3.network Author imotai <codego.me@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

use arrow::array::ArrayRef;
use arrow::array::FixedSizeListArray;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::dataframe::DataFrame;
use datafusion::execution::{context::SessionContext, options::NdJsonReadOptions};
use datafusion::physical_plan::collect;
use std::sync::Arc;
use std::vec::Vec;

pub async fn run_sql_on_json(json_path: &str, table: &str, sql: &str) -> Vec<Vec<String>> {
    let ctx = SessionContext::new();
    ctx.register_json(table, json_path, NdJsonReadOptions::default())
        .await
        .unwrap();
    execute(&ctx, sql).await
}
fn col_str(column: &ArrayRef, row_index: usize) -> String {
    if column.is_null(row_index) {
        return "NULL".to_string();
    }

    // Special case ListArray as there is no pretty print support for it yet
    if let DataType::FixedSizeList(_, n) = column.data_type() {
        let array = column
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .value(row_index);

        let mut r = Vec::with_capacity(*n as usize);
        for i in 0..*n {
            r.push(col_str(&array, i as usize));
        }
        return format!("[{}]", r.join(","));
    }

    array_value_to_string(column, row_index)
        .ok()
        .unwrap_or_else(|| "???".to_string())
}
fn result_vec(results: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut result = vec![];
    for batch in results {
        for row_index in 0..batch.num_rows() {
            let row_vec = batch
                .columns()
                .iter()
                .map(|column| col_str(column, row_index))
                .collect();
            result.push(row_vec);
        }
    }
    result
}

pub async fn execute(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    result_vec(&execute_to_batches(ctx, sql).await)
}

pub async fn execute_to_batches(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let msg = format!("Creating logical plan for '{}'", sql);
    let plan = ctx
        .create_logical_plan(sql)
        .map_err(|e| format!("{:?} at {}", e, msg))
        .unwrap();
    let logical_schema = plan.schema();
    let msg = format!(
        "Optimizing logical plan for '{}': {:?}, schema {:?}",
        sql, plan, logical_schema
    );
    println!("{}", msg);
    let plan = ctx
        .optimize(&plan)
        .map_err(|e| format!("{:?} at {}", e, msg))
        .unwrap();
    let ret = Arc::new(DataFrame::new(ctx.state.clone(), &plan));
    // use streaming resultset
    let batches = ret.collect().await.unwrap();
    batches
}
