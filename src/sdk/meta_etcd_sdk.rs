//
//
// meta_etcd_sdk.rs
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
use etcd_client::{Client, ConnectOptions, GetOptions};
use prost::Message;
use std::sync::{Arc, Mutex};
use tonic::codec::EncodeBuf;
uselog!(info, warn);

const BUFFER_SIZE: usize = 4 * 1024;
pub struct MetaEtcdConfig {
    // the root path for rtstore
    pub root_path: String,
    // endpoints for etcd cluster
    pub endpoints: String,
    // auth options for etcd
    pub options: Option<ConnectOptions>,
}

pub struct MetaEtcdSDK {
    config: MetaEtcdConfig,
    client: Arc<Mutex<Client>>,
}

impl MetaEtcdSDK {
    pub async fn new(config: MetaEtcdConfig) -> Result<Self> {
        let endpoints: Vec<&str> = config.endpoints.split(",").collect();
        match Client::connect(endpoints, config.options.clone()).await {
            Ok(client) => Ok(MetaEtcdSDK {
                client: Arc::new(Mutex::new(client)),
                config,
            }),
            Err(e) => {
                warn!("fail to connect to etcd for err {}", e);
                Err(RTStoreError::NodeRPCInvalidEndpointError {
                    name: "etcd".to_string(),
                })
            }
        }
    }

    pub async fn get_nodes_by_type(&self, node_type: RtStoreNodeType) -> Result<Vec<RtStoreNode>> {
        let prefix_key = format!("{}/nodes_{}/", self.config.root_path, node_type as i32);
        let option = GetOptions::new().with_prefix();
        match self.client.lock() {
            Ok(etcd_client) => {
                let mut kv_client = etcd_client.kv_client();
                match kv_client.get(prefix_key.as_bytes(), Some(option)).await {
                    Ok(response) => {
                        let mut nodes: Vec<RtStoreNode> = Vec::new();
                        for kv in response.kvs() {
                            let buf = Bytes::from(kv.value().to_vec());
                            match RtStoreNode::decode(buf) {
                                Ok(node) => nodes.push(node),
                                Err(e) => {
                                    warn!("fail to decode data value ");
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
            _ => Err(RTStoreError::BaseBusyError(
                "fail to get lock of etcd client".to_string(),
            )),
        }
    }

    pub async fn register_node(&self, node: &RtStoreNode) -> Result<()> {
        //TODO validate rtstore node
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
        self.put(key.as_bytes(), buf.as_ref()).await
    }

    pub async fn create_table(&self, table_id: &str, table_desc: &RtStoreTableDesc) -> Result<()> {
        let key = format!("{}/tables/{}", self.config.root_path, table_id);
        let mut buf = BytesMut::with_capacity(BUFFER_SIZE);
        if let Err(e) = table_desc.encode(&mut buf) {
            return Err(RTStoreError::MetaRpcCreateTableError {
                err: format!("encode descriptor of table {} with err {} ", table_id, e).to_string(),
            });
        }
        let buf = buf.freeze();
        self.put(key.as_bytes(), buf.as_ref()).await
    }

    #[inline]
    async fn put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        match self.client.lock() {
            Ok(etcd_client) => {
                let mut kv_client = etcd_client.kv_client();
                if let Err(e) = kv_client.put(key, value, None).await {
                    Err(RTStoreError::MetaRpcCreateTableError {
                        err: format!("fail to save descriptor  with err {} ", e).to_string(),
                    })
                } else {
                    Ok(())
                }
            }
            _ => Err(RTStoreError::BaseBusyError(
                "fail to get lock of etcd client".to_string(),
            )),
        }
    }

    pub async fn get_table(&self, table_id: &str) -> Result<Option<RtStoreTableDesc>> {
        let key = format!("{}/tables/{}", self.config.root_path, table_id);
        match self.client.lock() {
            Ok(etcd_client) => {
                let mut kv_client = etcd_client.kv_client();
                match kv_client.get(key.as_bytes(), None).await {
                    Ok(response) => match response.kvs().first() {
                        Some(kv) => {
                            //TODO avoid multi copys
                            let buf = Bytes::from(kv.value().to_vec());
                            match RtStoreTableDesc::decode(buf) {
                                Ok(desc) => Ok(Some(desc)),
                                Err(e) => Err(RTStoreError::TableCodecError {
                                    table_id: table_id.to_string(),
                                    err: format!("decode table err {}", e).to_string(),
                                }),
                            }
                        }
                        None => Ok(None),
                    },
                    Err(e) => Err(RTStoreError::TableCodecError {
                        table_id: table_id.to_string(),
                        err: format!("decode table err {}", e).to_string(),
                    }),
                }
            }
            _ => Err(RTStoreError::BaseBusyError(
                "fail to get lock of etcd client".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::rtstore_base_proto::{RtStoreColumnDesc, RtStoreSchemaDesc, RtStoreType};

    async fn create_a_local_etcd_sdk() -> Result<MetaEtcdSDK> {
        let config = MetaEtcdConfig {
            root_path: "/rtstore".to_string(),
            endpoints: "http://localhost:2379".to_string(),
            options: None,
        };
        MetaEtcdSDK::new(config).await
    }

    #[tokio::test]
    async fn test_init_meta_etcd_sdk() {
        assert!(create_a_local_etcd_sdk().await.is_ok());
    }

    #[tokio::test]
    async fn test_register_node() -> Result<()> {
        let sdk = create_a_local_etcd_sdk().await?;
        let rtstore_node = RtStoreNode {
            endpoint: "127.0.0.1:8989".to_string(),
            node_type: RtStoreNodeType::KComputeNode as i32,
            ns: "127.0.0.1".to_string(),
            port: 8989,
        };
        assert!(sdk.register_node(&rtstore_node).await.is_ok());
        let nodes = sdk.get_nodes_by_type(RtStoreNodeType::KComputeNode).await?;
        assert_eq!(1, nodes.len());
        assert_eq!(rtstore_node, nodes[0]);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_table_sdk() -> Result<()> {
        let table_id = "test.eth";
        let sdk = create_a_local_etcd_sdk().await?;
        let table_desc = create_simple_table_desc(table_id);
        assert!(sdk.create_table(table_id, &table_desc).await.is_ok());
        assert!(sdk.create_table(table_id, &table_desc).await.is_ok());
        match sdk.get_table(table_id).await? {
            Some(new_table_desc) => {
                assert!(table_desc == new_table_desc);
            }
            _ => {
                panic!("should not be here");
            }
        }
        Ok(())
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
