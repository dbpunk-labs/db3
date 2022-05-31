//
//
// meta_store.rs
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
use crate::proto::rtstore_base_proto::{RtStoreNode, RtStoreNodeType, RtStoreTableDesc};
use bytes::{Bytes, BytesMut};
use etcd_client::{Client, ConnectOptions, Event, GetOptions, WatchOptions, WatchStream, Watcher};
use prost::Message;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
uselog!(info);

const BUFFER_SIZE: usize = 4 * 1024;

pub enum MetaStoreType {
    ImmutableMetaStore,
    MutableMetaStore,
}

struct MetaStoreState {
    watchers: HashMap<String, Watcher>,
}

pub struct MetaStoreConfig {
    pub store_type: MetaStoreType,
    pub root_path: String,
}

pub struct MetaStore {
    config: MetaStoreConfig,
    state: Arc<Mutex<MetaStoreState>>,
    client: Arc<Client>,
}

unsafe impl Send for MetaStore {}

unsafe impl Sync for MetaStore {}

impl MetaStore {
    pub fn new(client: Client, config: MetaStoreConfig) -> Self {
        let state = MetaStoreState {
            watchers: HashMap::new(),
        };
        Self {
            config,
            state: Arc::new(Mutex::new(state)),
            client: Arc::new(client),
        }
    }

    pub async fn add_table(
        &self,
        table_full_name: &str,
        table_desc: &RtStoreTableDesc,
    ) -> Result<()> {
        if let MetaStoreType::MutableMetaStore = self.config.store_type {
            let key = format!("{}/tables/{}", self.config.root_path, table_full_name);
            let mut buf = BytesMut::with_capacity(BUFFER_SIZE);
            if let Err(e) = table_desc.encode(&mut buf) {
                return Err(RTStoreError::MetaRpcCreateTableError {
                    err: format!(
                        "encode descriptor of table {} with err {} ",
                        table_full_name, e
                    )
                    .to_string(),
                });
            }
            let buf = buf.freeze();
            self._put(key.as_bytes(), buf.as_ref()).await
        } else {
            Err(RTStoreError::MetaStoreTypeMisatchErr)
        }
    }

    pub async fn add_node(&self, node: &RtStoreNode) -> Result<()> {
        let key = format!(
            "{}/nodes_{}/{}_{}",
            self.config.root_path, node.node_type as i32, node.ns, node.port
        );
        let mut buf = BytesMut::with_capacity(BUFFER_SIZE);
        if let Err(e) = node.encode(&mut buf) {
            return Err(RTStoreError::EtcdCodecError(
                format!("encode descriptor  with err {} ", e).to_string(),
            ));
        }
        let buf = buf.freeze();
        self._put(key.as_bytes(), buf.as_ref()).await
    }

    pub async fn get_nodes(&self, node_type: RtStoreNodeType) -> Result<Vec<RtStoreNode>> {
        let key = format!("{}/nodes_{}/", self.config.root_path, node_type as i32);
        let options = GetOptions::new().with_prefix();
        let mut kv_client = self.client.kv_client();
        match kv_client.get(key.as_bytes(), Some(options)).await {
            Ok(resp) => {
                let mut nodes: Vec<RtStoreNode> = Vec::new();
                for kv in resp.kvs() {
                    let buf = Bytes::from(kv.value().to_vec());
                    match RtStoreNode::decode(buf) {
                        Ok(node) => nodes.push(node),
                        Err(e) => {
                            return Err(RTStoreError::EtcdCodecError(
                                format!("decode table err {}", e).to_string(),
                            ));
                        }
                    }
                }
                Ok(nodes)
            }
            Err(e) => Err(RTStoreError::EtcdCodecError(
                format!("decode table err {}", e).to_string(),
            )),
        }
    }

    pub async fn subscribe_node_events(&self, node_type: &RtStoreNodeType) -> Result<WatchStream> {
        let key = format!("{}/nodes_{}", self.config.root_path, *node_type as i32);
        match self.state.lock() {
            Ok(state) => {
                if state.watchers.contains_key(&key) {
                    Err(RTStoreError::MetaStoreExistErr {
                        name: "watch node".to_string(),
                        key: key.to_string(),
                    })
                } else {
                    Ok(())
                }
            }
            _ => Err(RTStoreError::BaseBusyError(
                "fail to get lock of etcd client".to_string(),
            )),
        }?;
        let options = WatchOptions::new().with_prefix();
        let mut watch_client = self.client.watch_client();
        let (_, stream) = watch_client.watch(key.to_string(), Some(options)).await?;
        Ok(stream)
    }

    #[inline]
    async fn _put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        let mut kv_client = self.client.kv_client();
        if let Err(e) = kv_client.put(key, value, None).await {
            Err(RTStoreError::MetaRpcCreateTableError {
                err: format!("fail to save descriptor  with err {} ", e).to_string(),
            })
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::rtstore_base_proto::RtStoreTableDesc;
    use crate::proto::rtstore_base_proto::{RtStoreColumnDesc, RtStoreSchemaDesc, RtStoreType};
    async fn create_a_etcd_client() -> Result<Client> {
        let endpoints: Vec<&str> = "http://localhost:2379".split(",").collect();
        if let Ok(client) = Client::connect(endpoints, None).await {
            Ok(client)
        } else {
            Err(RTStoreError::NodeRPCInvalidEndpointError {
                name: "etcd".to_string(),
            })
        }
    }

    #[tokio::test]
    async fn test_meta_store_init() -> Result<()> {
        assert!(create_a_etcd_client().await.is_ok());
        Ok(())
    }

    async fn create_a_meta_store() -> Result<MetaStore> {
        let client = create_a_etcd_client().await?;
        let config = MetaStoreConfig {
            store_type: MetaStoreType::MutableMetaStore,
            root_path: "/rtstore_test".to_string(),
        };
        Ok(MetaStore::new(client, config))
    }

    #[tokio::test]
    async fn test_add_table() -> Result<()> {
        let table_desc = create_simple_table_desc("test.eth");
        let meta_store = create_a_meta_store().await?;
        assert!(meta_store.add_table("test.eth", &table_desc).await.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_add_node() -> Result<()> {
        let meta_store = create_a_meta_store().await?;
        let rtstore_node = RtStoreNode {
            endpoint: "127.0.0.1:8989".to_string(),
            node_type: RtStoreNodeType::KComputeNode as i32,
            ns: "127.0.0.1".to_string(),
            port: 8989,
        };
        assert!(meta_store.add_node(&rtstore_node).await.is_ok());
        let nodes = meta_store.get_nodes(rtstore_node.node_type)?;
        assert_eq!(1, nodes.len());
        assert_eq!(rtstore_node.ns, nodes[0].ns);
    }

    fn create_simple_table_desc(tname: &str) -> RtStoreTableDesc {
        let col1 = RtStoreColumnDesc {
            name: "col1".to_string(),
            ctype: RtStoreType::KBigInt as i32,
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
}