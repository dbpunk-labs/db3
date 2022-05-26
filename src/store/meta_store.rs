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
use crate::proto::rtstore_base_proto::RtStoreTableDesc;
use bytes::{Bytes, BytesMut};
use etcd_client::{Client, ConnectOptions, GetOptions};
use prost::Message;
use std::sync::{Arc, Mutex};

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
    client: Arc<Mutex<Client>>,
}

impl MetaStore {
    pub fn new(client: Arc<Mutex<Client>>, config: MetaStoreConfig) -> Self {
        Self { config, client }
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

    pub async fn add_node(&self, node: &RTStoreNode) -> Result<()> {
        Ok(())
    }

    #[inline]
    async fn _put(&self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
