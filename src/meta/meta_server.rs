//
//
// meta_server.rs
// Copyright (C) 2022 rtstore.io Author imrtstore <rtstore_dev@outlook.com>
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
use super::table::Table;
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::RtStoreTableDesc;
use crate::proto::rtstore_meta_proto::meta_server::{Meta, MetaServer};
use crate::proto::rtstore_meta_proto::{CreateTableRequest, CreateTableResponse};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};
uselog!(debug, info, warn);

struct MetaServerState {
    tables: HashMap<String, Table>,
}
impl MetaServerState {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, table_desc: &RtStoreTableDesc) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
