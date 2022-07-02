//
//
// meta_server.rs
// Copyright (C) 2022 db3.network Author imrtstore <rtstore_dev@outlook.com>
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
use crate::catalog::catalog::Catalog;
use crate::error::{DB3Error, Result};
use crate::proto::db3_base_proto::{
    Db3Node, Db3NodeType, PartitionToNode, StorageBackendConfig, StorageRegion,
};
use crate::proto::db3_meta_proto::meta_server::Meta;
use crate::proto::db3_meta_proto::{
    CreateDbRequest, CreateDbResponse, CreateTableRequest, CreateTableResponse,
};
use crate::sdk::memory_node_sdk::MemoryNodeSDK;
use crate::store::meta_store::MetaStore;
use bytes::Bytes;
use etcd_client::EventType;
use prost::Message;
use rand::prelude::*;
use s3::region::Region;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
uselog!(debug, info, warn);

pub struct MetaConfig {
    pub node: DB3Node,
    pub etcd_cluster: String,
    pub etcd_root_path: String,
    // region for s3 bucket
    pub region: Region,
}

pub struct MetaServiceState {
    // key is the id of table
    memory_nodes: HashMap<String, Arc<MemoryNodeSDK>>,
}

impl MetaServiceState {
    pub fn new() -> Self {
        Self {
            memory_nodes: HashMap::new(),
        }
    }

    pub fn add_memory_node(&mut self, endpoint: &str, node: &Arc<MemoryNodeSDK>) -> Result<()> {
        match self.memory_nodes.get(endpoint) {
            Some(_) => Err(DB3Error::MemoryNodeExistError(endpoint.to_string())),
            _ => {
                self.memory_nodes.insert(endpoint.to_string(), node.clone());
                info!("add a new memory node {}", endpoint);
                Ok(())
            }
        }
    }
}

impl Default for MetaServiceState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MetaServiceImpl {
    state: Arc<Mutex<MetaServiceState>>,
    config: MetaConfig,
    meta_store: Arc<MetaStore>,
    catalog: Arc<Catalog>,
}

unsafe impl Send for MetaServiceImpl {}

unsafe impl Sync for MetaServiceImpl {}

impl MetaServiceImpl {
    pub fn new(config: MetaConfig, meta_store: Arc<MetaStore>) -> Self {
        Self {
            state: Arc::new(Mutex::new(MetaServiceState::new())),
            config,
            meta_store: meta_store.clone(),
            catalog: Arc::new(Catalog::new(meta_store)),
        }
    }

    pub async fn assign_partitions(
        &self,
        table_id: &str,
        db: &str,
        partition_range: &[i32],
    ) -> Result<()> {
        let memory_node_sdk = self.random_choose_a_memory_node()?;
        let sregion = match self.config.region {
            Region::Custom { .. } => StorageRegion {
                region: "".to_string(),
                endpoint: self.config.region.endpoint(),
            },
            _ => StorageRegion {
                region: format!("{}", self.config.region),
                endpoint: "".to_string(),
            },
        };
        let sconfig = StorageBackendConfig {
            bucket: format!("/{}", db),
            region: Some(sregion),
            l1_rows_limit: 10 * 1024,
            l2_rows_limit: 5 * 10 * 1024,
        };
        let database = self.catalog.get_db(db)?;
        let table = database.get_table(table_id)?;
        if memory_node_sdk
            .assign_partition(partition_range, table.get_table_desc(), &sconfig)
            .await
            .is_ok()
        {
            let mut mappings: Vec<PartitionToNode> = Vec::new();
            for pid in partition_range {
                let node_list = vec![memory_node_sdk.endpoint().to_string()];
                let mapping = PartitionToNode {
                    partition_id: *pid,
                    node_list,
                };
                mappings.push(mapping);
            }
            let mut new_table_desc = table.get_table_desc().clone();
            new_table_desc.mappings = mappings;
            // update meta of table
            self.meta_store.add_table(&new_table_desc).await?;
            info!("assign table {} to memory node ok", table_id);
        } else {
            todo!("handle error condition");
        }
        Ok(())
    }

    fn random_choose_a_memory_node(&self) -> Result<Arc<MemoryNodeSDK>> {
        if let Ok(local_state) = self.state.lock() {
            if local_state.memory_nodes.is_empty() {
                return Err(DB3Error::MemoryNodeNotEnoughError);
            }
            let mut rng = rand::thread_rng();
            let rand_num: f64 = rng.gen();
            let index: usize = (local_state.memory_nodes.len() as f64 * rand_num) as usize;
            for (current_index, sdk) in local_state.memory_nodes.values().enumerate() {
                if current_index == index {
                    return Ok(sdk.clone());
                }
            }
        }
        Err(DB3Error::MemoryNodeNotEnoughError)
    }

    pub async fn init(&self) -> Result<()> {
        self.catalog.recover().await?;
        let local_meta_store = self.meta_store.clone();
        self.meta_store.add_node(&self.config.node).await?;
        let local_state = self.state.clone();
        tokio::task::spawn(async move {
            if let Ok(mut stream) = local_meta_store
                .subscribe_node_events(&Db3NodeType::KMemoryNode)
                .await
            {
                while let Ok(Some(resp)) = stream.message().await {
                    if resp.canceled() {
                        break;
                    }
                    let mut new_add_nodes: Vec<Db3Node> = Vec::new();
                    let mut deleted_nodes: Vec<Db3Node> = Vec::new();
                    for event in resp.events() {
                        match (event.event_type(), event.kv()) {
                            (EventType::Put, Some(kv)) => {
                                let buf = Bytes::from(kv.value().to_vec());
                                match Db3Node::decode(buf) {
                                    Ok(node) => {
                                        if Db3NodeType::KMemoryNode as i32 == node.node_type {
                                            new_add_nodes.push(node);
                                        }
                                    }
                                    Err(e) => {
                                        warn!("fail to decode data value for err {} ", e);
                                    }
                                }
                            }
                            (EventType::Delete, Some(kv)) => {
                                let buf = Bytes::from(kv.value().to_vec());
                                match Db3Node::decode(buf) {
                                    Ok(node) => {
                                        if Db3NodeType::KMemoryNode as i32 == node.node_type {
                                            deleted_nodes.push(node);
                                        }
                                    }
                                    Err(e) => {
                                        warn!("fail to decode data value for err {} ", e);
                                    }
                                }
                            }
                            _ => {
                                warn!("null kv data");
                            }
                        }
                    }
                    for node in new_add_nodes {
                        match (
                            MemoryNodeSDK::connect(&node.endpoint).await,
                            local_state.lock(),
                        ) {
                            (Ok(sdk), Ok(mut state)) => {
                                let arc_sdk = Arc::new(sdk);
                                if state.add_memory_node(&node.endpoint, &arc_sdk).is_err() {
                                    warn!("fail to connect memory node {}", &node.endpoint);
                                }
                            }
                            (_, _) => warn!("fail to connect memory node {}", &node.endpoint),
                        }
                    }
                    for node in deleted_nodes {
                        info!("delete node {}", node.endpoint);
                    }
                }
            }
        });
        Ok(())
    }
}

#[tonic::async_trait]
impl Meta for MetaServiceImpl {
    async fn create_db(
        &self,
        request: Request<CreateDbRequest>,
    ) -> std::result::Result<Response<CreateDbResponse>, Status> {
        let create_db_request = request.into_inner();
        self.catalog
            .create_db(&create_db_request.db, self.config.region.clone())
            .await?;
        Ok(Response::new(CreateDbResponse {}))
    }
    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> std::result::Result<Response<CreateTableResponse>, Status> {
        let create_request = request.into_inner();
        let table_desc = match &create_request.table_desc {
            Some(t) => Ok(t),
            _ => Err(DB3Error::MetaRpcCreateTableError {
                err: "input is invalid for empty table description".to_string(),
            }),
        }?;
        let database = self.catalog.get_db(&table_desc.db)?;
        database.create_table(table_desc, false).await?;
        let partitions = vec![0];
        if let Err(e) = self
            .assign_partitions(&table_desc.name, &table_desc.db, &partitions)
            .await
        {
            warn!(
                "fail to assign partition for table {} with error {}",
                table_desc.name, e
            );
        }
        Ok(Response::new(CreateTableResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::db3_base_proto::{Db3ColumnDesc, Db3SchemaDesc, Db3TableDesc};
    use crate::store::build_meta_store;
    use crate::store::meta_store::MetaStoreType;
    use crate::store::object_store::build_region;

    fn build_config() -> MetaConfig {
        let node = Db3Node {
            endpoint: "http://127.0.0.1:9191".to_string(),
            node_type: Db3NodeType::KMetaNode as i32,
            ns: "127.0.0.1".to_string(),
            port: 9191,
        };

        MetaConfig {
            node,
            etcd_cluster: "127.0.0.1:9191".to_string(),
            etcd_root_path: "/rtstore".to_string(),
            region: build_region("http://127.0.0.1:9000"),
        }
    }

    async fn build_meta_service() -> MetaServiceImpl {
        let config = build_config();
        let meta_store = build_meta_store(
            &config.etcd_cluster,
            &config.etcd_root_path,
            MetaStoreType::MutableMetaStore,
        )
        .await
        .unwrap();
        let meta = MetaServiceImpl::new(config, Arc::new(meta_store));
        meta
    }

    #[tokio::test]
    async fn test_create_table_empty_desc() {
        let meta = build_meta_service().await;
        let req = Request::new(CreateTableRequest { table_desc: None });
        let result = meta.create_table(req).await;
        if result.is_ok() {
            panic!("should go error");
        }
    }

    fn create_simple_table_desc(db: &str, tname: &str) -> Db3TableDesc {
        let col1 = Db3ColumnDesc {
            name: "col1".to_string(),
            ctype: 0,
            null_allowed: true,
        };
        let schema = Db3SchemaDesc {
            columns: vec![col1],
            version: 1,
        };
        Db3TableDesc {
            name: tname.to_string(),
            schema: Some(schema),
            partition_desc: None,
            db: db.to_string(),
            ctime: 0,
            mappings: Vec::new(),
        }
    }
}
