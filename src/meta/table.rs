//
//
// table.rs
// Copyright (C) 2022 rtstore.ai Author imotai <codego.me@gmail.com>
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

use crate::base::arrow_parquet_utils::*;
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::RtStoreTableDesc;
use arrow::datatypes::SchemaRef;
use std::ops::Range;
use std::sync::Arc;
uselog!(info, warn, debug);

pub struct Cell {
    range: Range<u64>,
    partition_index: usize,
    num_rows: u64,
}

/// the smallest data unit for table store
pub struct Partition {
    partition_index: usize,
    num_rows: u64,
    cells: Vec<Cell>,
}

pub struct Table {
    // name of table like db1.user
    id: String,
    // schema for table
    // more go to https://github.com/apache/arrow-rs/blob/master/arrow/src/datatypes/schema.rs
    schema: SchemaRef,
    // rtstore table description
    table_desc: Arc<RtStoreTableDesc>,
    partitions: Vec<Partition>,
}

impl Table {
    pub fn gen_id(table_desc: &RtStoreTableDesc) -> Result<String> {
        // validate table name and join names with dot
        if table_desc.names.len() <= 0 {
            return Err(RTStoreError::TableInvalidNamesError {
                error: "empty name".to_string(),
            });
        }
        Ok(table_desc.names.join("."))
    }

    pub fn new(table_desc: &RtStoreTableDesc) -> Result<Self> {
        let id = Self::gen_id(table_desc)?;
        info!("gen a new table id {}", id);
        let schema = match &table_desc.schema {
            Some(s) => Ok(s),
            _ => Err(RTStoreError::TableSchemaInvalidError {
                name: id.to_string(),
            }),
        }?;
        let arrow_schema_ref = table_desc_to_arrow_schema(&schema)?;
        Ok(Self {
            id,
            schema: arrow_schema_ref,
            table_desc: Arc::new(table_desc.clone()),
            partitions: Vec::new(),
        })
    }
}
