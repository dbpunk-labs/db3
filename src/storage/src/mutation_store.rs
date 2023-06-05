//
// mutation_store.rs
// Copyright (C) 2023 db3.network Author imotai <codego.me@gmail.com>
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

use bytes::BytesMut;
use db3_crypto::id::TxId;
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_v2_proto::MutationMessage;
use prost::Message;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options, WriteBatch};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;
use tracing::{debug, info};

pub type StorageEngine = DBWithThreadMode<MultiThreaded>;

pub struct MutationStoreConfig {
    pub db_path: String,
    pub block_store_cf_name: String,
    pub tx_store_cf_name: String,
    pub message_max_buffer: usize,
}

impl Default for MutationStoreConfig {
    fn default() -> MutationStoreConfig {
        MutationStoreConfig {
            db_path: "./store".to_string(),
            block_store_cf_name: "block_store_cf".to_string(),
            tx_store_cf_name: "tx_store_cf".to_string(),
            message_max_buffer: 8 * 1024,
        }
    }
}

pub struct MutationStore {
    config: MutationStoreConfig,
    se: StorageEngine,
    block: Arc<AtomicU64>,
    order_in_block: Arc<AtomicU32>,
}

impl MutationStore {
    pub fn new(config: MutationStoreConfig) -> Result<Self> {
        let mut cf_opts = Options::default();
        cf_opts.create_if_missing(true);
        cf_opts.create_missing_column_families(true);
        info!("open mutation store with path {}", config.db_path.as_str());
        let path = Path::new(config.db_path.as_str());
        let se = StorageEngine::open_cf(
            &cf_opts,
            &path,
            [
                config.block_store_cf_name.as_str(),
                config.tx_store_cf_name.as_str(),
            ],
        )
        .map_err(|e| DB3Error::OpenStoreError(config.db_path.to_string(), format!("{e}")))?;
        Ok(Self {
            config,
            se,
            block: Arc::new(AtomicU64::new(0)),
            order_in_block: Arc::new(AtomicU32::new(0)),
        })
    }

    pub fn increase_block(&self) {
        self.block
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn add_mutation(&self, payload: &[u8], signature: &[u8]) -> Result<String> {
        let block = self.block.load(std::sync::atomic::Ordering::Relaxed);
        let order = self
            .order_in_block
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        //TODO avoid the duplicated tx id
        let tx_id = TxId::from(payload);
        debug!("the tx id is {}", tx_id.to_hex());
        let mut encoded_id: Vec<u8> = Vec::new();
        encoded_id.extend_from_slice(&block.to_be_bytes());
        encoded_id.extend_from_slice(&order.to_be_bytes());
        let mutation_msg = MutationMessage {
            payload: payload.to_vec(),
            signature: signature.to_vec(),
            block_id: block,
            order,
        };
        let mut buf = BytesMut::with_capacity(self.config.message_max_buffer);
        mutation_msg
            .encode(&mut buf)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        let buf = buf.freeze();
        let tx_cf_handle = self
            .se
            .cf_handle(self.config.tx_store_cf_name.as_str())
            .ok_or(DB3Error::WriteStoreError("cf is not found".to_string()))?;
        let block_cf_handle = self
            .se
            .cf_handle(self.config.block_store_cf_name.as_str())
            .ok_or(DB3Error::WriteStoreError("cf is not found".to_string()))?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&tx_cf_handle, &tx_id, &encoded_id);
        batch.put_cf(&block_cf_handle, &encoded_id, buf.as_ref());
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        Ok(tx_id.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn test_new_mutation_store() {
        let tmp_dir_path = TempDir::new("new_mutation_store_path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = MutationStoreConfig {
            db_path: real_path,
            block_store_cf_name: "cf1".to_string(),
            tx_store_cf_name: "cf2".to_string(),

            message_max_buffer: 4 * 1024,
        };
        if let Err(e) = MutationStore::new(config) {
            println!("{:?}", e);
        }
    }

    #[test]
    fn test_add_mutation() {
        let tmp_dir_path = TempDir::new("add mutation store path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = MutationStoreConfig {
            db_path: real_path,
            block_store_cf_name: "cf1".to_string(),
            tx_store_cf_name: "cf2".to_string(),
            message_max_buffer: 4 * 1024,
        };
        let result = MutationStore::new(config);
        assert!(result.is_ok());
        if let Ok(store) = result {
            let payload: Vec<u8> = vec![1];
            let signature: Vec<u8> = vec![1];
            let result = store.add_mutation(payload.as_ref(), signature.as_ref());
            assert!(result.is_ok());
            let result = store.add_mutation(payload.as_ref(), signature.as_ref());
            assert!(result.is_ok());
        } else {
            assert!(false);
        }
    }
}
