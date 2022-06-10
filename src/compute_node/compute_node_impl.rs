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
use crate::store::meta_store::MetaStore;
use crate::proto::rtstore_compute_proto::compute_node_server::ComputeNode;
use crate::proto::rtstore_compute_proto::{
    QueryRequest, QueryResponse
};
use crate::store::object_store::{S3FileSystem, build_credentials};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use crate::catalog::catalog::Catalog;
use super::sql_engine::SQLEngine;
use s3::region::Region;
use std::sync::Arc;

pub struct ComputeNodeConfig {
    pub etcd_cluster: String,
    pub etcd_root_path: String,
    pub node: RtStoreNode,
    pub region: String,
}

pub struct ComputeNodeImpl {
    catalog:Arc<Catalog>,
    sql_engine:Arc<SQLEngine>,
    runtime: Arc<RuntimeEnv>,
    meta_store:Arc<MetaStore>
}

unsafe impl Send for ComputeNodeImpl {}

unsafe impl Sync for ComputeNodeImpl {}

impl ComputeNodeImpl {
    pub fn new(region:Region,
               config:ComputeNodeConfig, 
               meta_store:Arc<MetaStore>) -> Result<ComputeNodeImpl> {
        let credentials = build_credentials(None, None)?;
        let s3 = S3FileSystem::new(region, credentials);
        let catalog = Arc::new(Catalog::new(meta_store.clone()));
        let runtime_config = RuntimeConfig::new();
        let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
        runtime.register_object_store("s3", Arc::new(s3));
        let sql_engine = Arc::new(SQLEngine::new(&catalog));
        Ok(Self {
            catalog,
            sql_engine,
            runtime,
            meta_store
        })
    }
}

#[tonic::async_trait]
impl ComputeNode for ComputeNodeImpl {
    async fn query(&self,
        request: Request<QueryRequest>) -> std::result::Result<Result<QueryResponse>, Status> {
        let query_request = request.into_inner();
        let mut db:Option<String> = None;
        if !query_request.db.is_empty() {
            db = Some(query_request.db);
        }
        sql_engine.execute(query_request.sql, db).await?;
        Ok(Response::new(QueryResponse {}))
    }
}

