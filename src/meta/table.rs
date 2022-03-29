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

use crate::proto::rtstore_base_proto::RtStoreTableDesc;
use arrow::datatypes::{Schema, SchemaRef};
use std::ops::Range;
uselog!(info, warn, debug);

pub struct Cell {
    pub range: Range<u64>,
    pub partition_index: usize,
    pub num_rows: u64,
}

/// the smallest data unit for table store
pub struct Partition {
    pub partition_index: usize,
    pub num_rows: u64,
    pub cells: Vec<Cell>,
}

pub struct Table {
    // name of table like db1.user
    pub name: String,
    // schema for table
    // more go to https://github.com/apache/arrow-rs/blob/master/arrow/src/datatypes/schema.rs
    pub schema: SchemaRef,
    pub table_desc: RtStoreTableDesc,
    pub partitions: Vec<Partition>,
}
