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

use super::sql_engine::SQLEngine;
use crate::catalog::catalog::Catalog;
use crate::codec::flight_codec::{flight_data_from_arrow_batch, SchemaAsIpc};
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::{
    FlightData, RtStoreNode, RtStoreNodeType, RtStoreTableDesc, StorageBackendConfig, StorageRegion,
};
use crate::proto::rtstore_compute_proto::compute_node_server::ComputeNode;
use crate::proto::rtstore_compute_proto::QueryRequest;
use crate::store::meta_store::MetaStore;
use crate::store::object_store::{build_credentials, S3FileSystem};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use futures::Stream;
use s3::region::Region;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status};
uselog!(info);

pub struct ComputeNodeConfig {
    pub etcd_cluster: String,
    pub etcd_root_path: String,
    pub node: RtStoreNode,
}

pub struct ComputeNodeImpl {
    catalog: Arc<Catalog>,
    sql_engine: Arc<SQLEngine>,
    meta_store: Arc<MetaStore>,
    config: ComputeNodeConfig,
}

unsafe impl Send for ComputeNodeImpl {}

unsafe impl Sync for ComputeNodeImpl {}

impl ComputeNodeImpl {
    pub fn new(
        region: Region,
        config: ComputeNodeConfig,
        meta_store: Arc<MetaStore>,
    ) -> Result<ComputeNodeImpl> {
        let credentials = build_credentials(None, None)?;
        let s3 = S3FileSystem::new(region, credentials);
        let catalog = Arc::new(Catalog::new(meta_store.clone()));
        let runtime_config = RuntimeConfig::new();
        let runtime = Arc::new(RuntimeEnv::new(runtime_config)?);
        runtime.register_object_store("s3", Arc::new(s3));
        let sql_engine = Arc::new(SQLEngine::new(&catalog, &runtime));
        Ok(Self {
            catalog,
            sql_engine,
            meta_store,
            config,
        })
    }

    pub async fn init(&self) -> Result<()> {
        self.catalog.recover().await?;
        Catalog::subscribe_changes(&self.catalog).await;
        self.meta_store.add_node(&self.config.node).await?;
        Ok(())
    }
}
#[tonic::async_trait]
impl ComputeNode for ComputeNodeImpl {
    type QueryStream = Pin<
        Box<dyn Stream<Item = std::result::Result<FlightData, Status>> + Send + Sync + 'static>,
    >;
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> std::result::Result<Response<Self::QueryStream>, Status> {
        let query_request = request.into_inner();
        let mut db: Option<String> = None;
        if !query_request.default_db.is_empty() {
            db = Some(query_request.default_db);
        }
        let result = self.sql_engine.execute(&query_request.sql, db).await?;
        if result.batch.is_none() {
            let flights: Vec<std::result::Result<FlightData, Status>> = Vec::new();
            let output = futures::stream::iter(flights);
            return Ok(Response::new(Box::pin(output) as Self::QueryStream));
        }
        let batches = result.batch.unwrap();
        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
        let schema_flight_data = SchemaAsIpc::new(batches[0].schema().as_ref(), &options).into();
        let mut flights: Vec<std::result::Result<FlightData, Status>> =
            vec![Ok(schema_flight_data)];
        let mut batches: Vec<std::result::Result<FlightData, Status>> = batches
            .iter()
            .flat_map(|batch| {
                let (flight_dictionaries, flight_batch) =
                    flight_data_from_arrow_batch(batch, &options);
                flight_dictionaries
                    .into_iter()
                    .chain(std::iter::once(flight_batch))
                    .map(Ok)
            })
            .collect();
        // append batch vector to schema vector, so that the first message sent is the schema
        flights.append(&mut batches);
        let output = futures::stream::iter(flights);
        Ok(Response::new(Box::pin(output) as Self::QueryStream))
    }
}
