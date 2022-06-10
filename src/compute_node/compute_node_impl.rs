//
//
// compute_node_impl.rs
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
use crate::proto::rtstore_base_proto::{
    RtStoreNode, RtStoreNodeType, RtStoreTableDesc, StorageBackendConfig, StorageRegion,
};

use crate::proto::rtstore_compute_proto::compute_node_server::ComputeNode;
use crate::proto::rtstore_compute_proto::{
    QueryRequest, QueryResponse
};

use crate::catalog::catalog::Catalog;
use super::sql_engine::SQLEngine;

pub struct ComputeNodeConfig {
    pub etcd_cluster: String,
    pub etcd_root_path: String,
    pub node: RtStoreNode,
}

pub struct ComputeNodeImpl {
    catalog:Arc<Catalog>,
    sql_engine:Arc<SQLEngine>,
}
