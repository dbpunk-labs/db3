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
use crate::proto::rtstore_base_proto::{RtStoreNode, RtStoreNodeType, RtStoreTableDesc};
use crate::proto::rtstore_meta_proto::meta_server::Meta;
use crate::proto::rtstore_meta_proto::{
    CreateTableRequest, CreateTableResponse, PingRequest, PingResponse,
};
use crate::sdk::memory_node_sdk::MemoryNodeSDK;
use crate::store::meta_store::{MetaStore, MetaStoreConfig, MetaStoreType};
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use etcd_client::{Client, ConnectOptions, EventType, GetOptions};
use prost::Message;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
uselog!(debug, info, warn);

pub struct MetaConfig {
    pub node: RtStoreNode,
    pub etcd_cluster: String,
    pub etcd_root_path: String,
}

pub struct MetaServiceState {
    // key is the id of table
    tables: HashMap<String, Table>,
    memory_nodes: HashMap<String, Arc<MemoryNodeSDK>>,
    table_to_nodes: HashMap<String, HashMap<i32, String>>,
}

impl MetaServiceState {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            memory_nodes: HashMap::new(),
            table_to_nodes: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, table_desc: &RtStoreTableDesc) -> Result<()> {
        // join the names of table desc
        let id = Table::gen_id(table_desc)?;
        debug!("create table with id {}", id);
        match self.tables.get(&id) {
            Some(_) => Err(RTStoreError::TableNamesExistError { name: id }),
            _ => {
                let table = Table::new(table_desc)?;
                info!("create a new table with id {} successfully", id);
                self.tables.insert(id, table);
                Ok(())
            }
        }
    }

    pub fn add_memory_node(&mut self, endpoint: &str, node: &Arc<MemoryNodeSDK>) -> Result<()> {
        match self.memory_nodes.get(endpoint) {
            Some(_) => Err(RTStoreError::MemoryNodeExistError(endpoint.to_string())),
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
    meta_store: ArcSwapOption<MetaStore>,
}

unsafe impl Send for MetaServiceImpl {}

unsafe impl Sync for MetaServiceImpl {}

impl MetaServiceImpl {
    pub fn new(config: MetaConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(MetaServiceState::new())),
            config,
            meta_store: ArcSwapOption::from(None),
        }
    }

    pub async fn connect_to_meta(&self) -> Result<()> {
        info!("connect to meta");
        if self.config.etcd_cluster.is_empty() {
            return Err(RTStoreError::NodeRPCInvalidEndpointError {
                name: "etcd cluster".to_string(),
            });
        }
        // connect to etcd
        let endpoints: Vec<&str> = self.config.etcd_cluster.split(",").collect();
        let etcd_client = match Client::connect(endpoints, None).await {
            Ok(client) => Ok(client),
            Err(e) => {
                warn!("fail to connect etcd for err {}", e);
                Err(RTStoreError::NodeRPCInvalidEndpointError {
                    name: "etcd".to_string(),
                })
            }
        }?;

        let meta_store_config = MetaStoreConfig {
            root_path: self.config.etcd_root_path.to_string(),
            store_type: MetaStoreType::MutableMetaStore,
        };
        info!("register self {} to etcd", self.config.node.ns);
        let meta_store = MetaStore::new(etcd_client, meta_store_config);
        // register self to etcd
        meta_store.add_node(&self.config.node).await?;
        self.meta_store.store(Some(Arc::new(meta_store)));
        let local_meta_store = self.meta_store.load().clone();
        let local_state = self.state.clone();
        if let Some(local_meta_store) = local_meta_store {
            tokio::task::spawn(async move {
                if let Ok(mut stream) = local_meta_store
                    .subscribe_node_events(&RtStoreNodeType::KMemoryNode)
                    .await
                {
                    while let Ok(Some(resp)) = stream.message().await {
                        if resp.canceled() {
                            break;
                        }
                        let mut new_add_nodes: Vec<RtStoreNode> = Vec::new();
                        let mut deleted_nodes: Vec<RtStoreNode> = Vec::new();
                        for event in resp.events() {
                            match (event.event_type(), event.kv()) {
                                (EventType::Put, Some(kv)) => {
                                    let buf = Bytes::from(kv.value().to_vec());
                                    match RtStoreNode::decode(buf) {
                                        Ok(node) => {
                                            if RtStoreNodeType::KMemoryNode as i32 == node.node_type
                                            {
                                                new_add_nodes.push(node);
                                            }
                                        }
                                        Err(e) => {
                                            warn!("fail to decode data value ");
                                        }
                                    }
                                }
                                (EventType::Delete, Some(kv)) => {
                                    let buf = Bytes::from(kv.value().to_vec());
                                    match RtStoreNode::decode(buf) {
                                        Ok(node) => {
                                            if RtStoreNodeType::KMemoryNode as i32 == node.node_type
                                            {
                                                deleted_nodes.push(node);
                                            }
                                        }
                                        Err(e) => {
                                            warn!("fail to decode data value ");
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
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl Meta for MetaServiceImpl {
    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> std::result::Result<Response<CreateTableResponse>, Status> {
        let create_request = request.into_inner();
        let table_desc = match &create_request.table_desc {
            Some(t) => Ok(t),
            _ => Err(RTStoreError::MetaRpcCreateTableError {
                err: "input is invalid for empty table description".to_string(),
            }),
        }?;
        let mut local_state = self.state.lock().unwrap();
        local_state.create_table(table_desc)?;
        Ok(Response::new(CreateTableResponse {}))
    }

    async fn ping(
        &self,
        _request: Request<PingRequest>,
    ) -> std::result::Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::rtstore_base_proto::{RtStoreColumnDesc, RtStoreSchemaDesc};

    fn build_config() -> MetaConfig {
        let node = RtStoreNode {
            endpoint: "http://127.0.0.1:9191".to_string(),
            node_type: RtStoreNodeType::KMetaNode as i32,
            ns: "127.0.0.1".to_string(),
            port: 9191,
        };
        MetaConfig {
            node,
            etcd_cluster: "127.0.0.1:9191".to_string(),
            etcd_root_path: "/rtstore".to_string(),
        }
    }
    #[tokio::test]
    async fn test_ping() {
        let meta = MetaServiceImpl::new(build_config());
        let req = Request::new(PingRequest {});
        let result = meta.ping(req).await;
        if result.is_err() {
            panic!("should go error");
        }
    }

    #[tokio::test]
    async fn test_create_table_empty_desc() {
        let meta = MetaServiceImpl::new(build_config());
        let req = Request::new(CreateTableRequest { table_desc: None });
        let result = meta.create_table(req).await;
        if result.is_ok() {
            panic!("should go error");
        }
    }

    fn create_simple_table_desc(tname: &str) -> RtStoreTableDesc {
        let col1 = RtStoreColumnDesc {
            name: "col1".to_string(),
            ctype: 0,
            null_allowed: true,
        };
        let schema = RtStoreSchemaDesc {
            columns: vec![col1],
            version: 1,
        };
        RtStoreTableDesc {
            names: vec![tname.to_string()],
            schema: Some(schema),
            partition_desc: None,
        }
    }

    #[tokio::test]
    async fn test_create_table() {
        let table_desc = Some(create_simple_table_desc("test.t1"));
        let meta = MetaServiceImpl::new(build_config());
        let req = Request::new(CreateTableRequest { table_desc });
        let result = meta.create_table(req).await;
        assert!(result.is_ok());
    }
}
