//
//
// meta_store.rs
// Copyright (C) 2022 db3.network Author imotai <codego.me@gmail.com>
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

use crate::error::{DB3Error, Result};
use crate::proto::db3_base_proto::{Db3Database, Db3Node, Db3NodeType, Db3TableDesc};
use bytes::{Bytes, BytesMut};
use etcd_client::{Client, GetOptions, WatchOptions, WatchStream};
use prost::Message;
use std::sync::Arc;
uselog!(info, warn);

const BUFFER_SIZE: usize = 4 * 1024;

pub enum MetaStoreType {
    ImmutableMetaStore,
    MutableMetaStore,
}

pub struct MetaStoreConfig {
    pub store_type: MetaStoreType,
    pub root_path: String,
}

pub struct MetaStore {
    config: MetaStoreConfig,
    client: Arc<Client>,
}

unsafe impl Send for MetaStore {}

unsafe impl Sync for MetaStore {}

impl MetaStore {
    pub fn new(client: Client, config: MetaStoreConfig) -> Self {
        Self {
            config,
            client: Arc::new(client),
        }
    }

    pub async fn add_db(&self, db: &Db3Database) -> Result<()> {
        if let MetaStoreType::MutableMetaStore = self.config.store_type {
            let key = format!("{}/dbs/{}", self.config.root_path, &db.db);
            info!("add db with key {}", &key);
            let mut buf = BytesMut::with_capacity(BUFFER_SIZE);
            if let Err(e) = db.encode(&mut buf) {
                return Err(DB3Error::MetaRpcCreateTableError {
                    err: format!("encode descriptor of db {} with err {} ", db.db, e),
                });
            }
            let buf = buf.freeze();
            self._put(key.as_bytes(), buf.as_ref()).await
        } else {
            Err(DB3Error::MetaStoreTypeMisatchErr)
        }
    }

    pub async fn add_table(&self, table_desc: &Db3TableDesc) -> Result<()> {
        if let MetaStoreType::MutableMetaStore = self.config.store_type {
            let key = format!(
                "{}/tables/{}_{}",
                self.config.root_path, table_desc.db, table_desc.name
            );
            info!("add table with key {}", &key);
            let mut buf = BytesMut::with_capacity(BUFFER_SIZE);
            if let Err(e) = table_desc.encode(&mut buf) {
                return Err(DB3Error::MetaRpcCreateTableError {
                    err: format!(
                        "encode descriptor of table {} with err {} ",
                        table_desc.name, e
                    ),
                });
            }
            let buf = buf.freeze();
            self._put(key.as_bytes(), buf.as_ref()).await
        } else {
            Err(DB3Error::MetaStoreTypeMisatchErr)
        }
    }

    pub async fn add_node(&self, node: &Db3Node) -> Result<()> {
        let key = format!(
            "{}/nodes_{}/{}_{}",
            self.config.root_path, node.node_type as i32, node.ns, node.port
        );
        let mut buf = BytesMut::with_capacity(BUFFER_SIZE);
        if let Err(e) = node.encode(&mut buf) {
            return Err(DB3Error::EtcdCodecError(format!(
                "encode descriptor  with err {} ",
                e
            )));
        }
        let buf = buf.freeze();
        self._put(key.as_bytes(), buf.as_ref()).await
    }

    pub async fn get_nodes(&self, node_type: Db3NodeType) -> Result<Vec<Db3Node>> {
        let key = format!("{}/nodes_{}/", self.config.root_path, node_type as i32);
        let options = GetOptions::new().with_prefix();
        let mut kv_client = self.client.kv_client();
        match kv_client.get(key.as_bytes(), Some(options)).await {
            Ok(resp) => {
                let mut nodes: Vec<Db3Node> = Vec::new();
                for kv in resp.kvs() {
                    let buf = Bytes::from(kv.value().to_vec());
                    match Db3Node::decode(buf) {
                        Ok(node) => nodes.push(node),
                        Err(e) => {
                            return Err(DB3Error::EtcdCodecError(format!(
                                "decode table err {}",
                                e
                            )));
                        }
                    }
                }
                Ok(nodes)
            }
            Err(e) => Err(DB3Error::StoreS3Error(format!(
                "fail to get kv from etcd for e {}",
                e
            ))),
        }
    }

    pub async fn get_db(&self, db: &str) -> Result<Db3Database> {
        let key = format!("{}/dbs/{}", self.config.root_path, db);
        let options = GetOptions::new();
        let mut kv_client = self.client.kv_client();
        match kv_client.get(key.as_bytes(), Some(options)).await {
            Ok(resp) => {
                let mut dbs: Vec<Db3Database> = Vec::new();
                for kv in resp.kvs() {
                    let buf = Bytes::from(kv.value().to_vec());
                    match Db3Database::decode(buf) {
                        Ok(db) => dbs.push(db),
                        Err(e) => {
                            warn!("fail to decode table for err {}", e);
                        }
                    }
                }
                if dbs.is_empty() {
                    Err(DB3Error::StoreS3Error(
                        "fail to get kv from etcd".to_string(),
                    ))
                } else {
                    Ok(dbs[0].clone())
                }
            }
            Err(e) => Err(DB3Error::StoreS3Error(format!(
                "fail to get kv from etcd for e {}",
                e
            ))),
        }
    }

    pub async fn get_dbs(&self) -> Result<Vec<Db3Database>> {
        let key = format!("{}/dbs/", self.config.root_path);
        let options = GetOptions::new().with_prefix();
        let mut kv_client = self.client.kv_client();
        match kv_client.get(key.as_bytes(), Some(options)).await {
            Ok(resp) => {
                let mut dbs: Vec<Db3Database> = Vec::new();
                for kv in resp.kvs() {
                    let buf = Bytes::from(kv.value().to_vec());
                    match Db3Database::decode(buf) {
                        Ok(db) => dbs.push(db),
                        Err(e) => {
                            warn!("fail to decode table for err {}", e);
                        }
                    }
                }
                Ok(dbs)
            }
            Err(e) => Err(DB3Error::StoreS3Error(format!(
                "fail to get kv from etcd for e {}",
                e
            ))),
        }
    }

    pub async fn get_tables(&self, db: &str) -> Result<Vec<Db3TableDesc>> {
        let key = format!("{}/tables/{}_", self.config.root_path, db);
        let options = GetOptions::new().with_prefix();
        let mut kv_client = self.client.kv_client();
        match kv_client.get(key.as_bytes(), Some(options)).await {
            Ok(resp) => {
                let mut tables: Vec<Db3TableDesc> = Vec::new();
                for kv in resp.kvs() {
                    let buf = Bytes::from(kv.value().to_vec());
                    match Db3TableDesc::decode(buf) {
                        Ok(table) => tables.push(table),
                        Err(e) => {
                            warn!("fail to decode table for error {}", e);
                        }
                    }
                }
                Ok(tables)
            }
            Err(e) => Err(DB3Error::StoreS3Error(format!(
                "fail to get kv from etcd for e {}",
                e
            ))),
        }
    }

    #[inline]
    pub async fn subscribe_table_events(&self) -> Result<WatchStream> {
        let key = format!("{}/tables/", self.config.root_path);
        let options = WatchOptions::new().with_prefix();
        let mut watch_client = self.client.watch_client();
        let (_, stream) = watch_client.watch(key.to_string(), Some(options)).await?;
        Ok(stream)
    }

    #[inline]
    pub async fn subscribe_node_events(&self, node_type: &Db3NodeType) -> Result<WatchStream> {
        let key = format!("{}/nodes_{}", self.config.root_path, *node_type as i32);
        let options = WatchOptions::new().with_prefix();
        let mut watch_client = self.client.watch_client();
        let (_, stream) = watch_client.watch(key.to_string(), Some(options)).await?;
        Ok(stream)
    }

    #[inline]
    async fn _put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        let mut kv_client = self.client.kv_client();
        if let Err(e) = kv_client.put(key, value, None).await {
            Err(DB3Error::MetaRpcCreateTableError {
                err: format!("fail to save descriptor  with err {} ", e),
            })
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::db3_base_proto::Db3TableDesc;
    use crate::proto::db3_base_proto::{Db3ColumnDesc, Db3SchemaDesc, Db3Type};
    async fn create_etcd_client() -> Result<Client> {
        let endpoints: Vec<&str> = "http://localhost:2379".split(",").collect();
        if let Ok(client) = Client::connect(endpoints, None).await {
            Ok(client)
        } else {
            Err(DB3Error::NodeRPCInvalidEndpointError {
                name: "etcd".to_string(),
            })
        }
    }

    #[tokio::test]
    async fn test_meta_store_init() {
        assert!(create_meta_store().await.is_ok());
    }

    async fn create_meta_store() -> Result<MetaStore> {
        let client = create_etcd_client().await?;
        let config = MetaStoreConfig {
            store_type: MetaStoreType::MutableMetaStore,
            root_path: "/db3_test".to_string(),
        };
        Ok(MetaStore::new(client, config))
    }

    #[tokio::test]
    async fn test_add_table_flow() -> Result<()> {
        let table_desc = create_simple_table_desc("db1", "eth");
        let meta_store = create_meta_store().await?;
        assert!(meta_store.add_table(&table_desc).await.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn test_add_node() -> Result<()> {
        let meta_store = create_meta_store().await?;
        let db3_node = Db3Node {
            endpoint: "127.0.0.1:8989".to_string(),
            node_type: Db3NodeType::KComputeNode as i32,
            ns: "127.0.0.1".to_string(),
            port: 8989,
        };
        assert!(meta_store.add_node(&db3_node).await.is_ok());
        let nodes = meta_store.get_nodes(Db3NodeType::KComputeNode).await?;
        assert_eq!(1, nodes.len());
        assert_eq!(db3_node.ns, nodes[0].ns);
        Ok(())
    }

    fn create_simple_table_desc(db: &str, tname: &str) -> Db3TableDesc {
        let col1 = Db3ColumnDesc {
            name: "col1".to_string(),
            ctype: Db3Type::KBigInt as i32,
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
