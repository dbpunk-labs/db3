//
//
// table.rs
// Copyright (C) 2022 rtstore.io Author imotai <codego.me@gmail.com>
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

use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::{RtStoreNode, RtStoreTableDesc, StorageRegion};
use arrow::datatypes::{Schema, SchemaRef};
use crossbeam_skiplist_piedb::SkipMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct Table {
    desc: RtStoreTableDesc,
    parquet_schema: SchemaRef,
    partition_to_nodes: Arc<SkipMap<i32, RtStoreNode>>,
}

unsafe impl Send for Table {}
unsafe impl Sync for Table {}

impl Table {
    pub fn new(desc: &RtStoreTableDesc, schema: SchemaRef) -> Self {
        Self {
            desc: desc.clone(),
            parquet_schema: schema,
            partition_to_nodes: Arc::new(SkipMap::new()),
        }
    }
    pub fn assign_partition_to_node(&self, pid: i32, node: RtStoreNode) -> Result<()> {
        self.partition_to_nodes.remove(&pid);
        self.partition_to_nodes.get_or_insert_with(pid, || node);
        Ok(())
    }
    #[inline]
    pub fn get_table_desc(&self) -> &RtStoreTableDesc {
        &self.desc
    }

    #[inline]
    pub fn get_db(&self) -> &str {
        &self.desc.db
    }

    #[inline]
    pub fn get_schema(&self) -> &SchemaRef {
        &self.parquet_schema
    }

    #[inline]
    pub fn get_ctime(&self) -> i64 {
        self.desc.ctime
    }

    #[inline]
    pub fn get_name(&self) -> &str {
        &self.desc.name
    }

    #[inline]
    pub fn get_node_by_partition(&self, pid: i32) -> Option<RtStoreNode> {
        let node_entry = self.partition_to_nodes.get(&pid);
        match node_entry {
            Some(entry) => {
                let node = entry.value();
                Some(node.clone())
            }
            _ => None,
        }
    }
}
