//
// state_store.rs
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
use db3_crypto::db3_address::DB3Address;
use db3_error::{DB3Error, Result};
use db3_proto::db3_base_proto::SystemConfig;
use libmdbx::{Database, NoWriteMap, TableFlags, WriteFlags};
use prost::Message;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

const ACCOUNT_META_TABLE: &str = "ACCOUNT_META_TABLE";
const CONFIG_META_TABLE: &str = "CONFIG_META_TABLE";

type DB = Database<NoWriteMap>;

#[derive(Clone)]
pub struct StateStoreConfig {
    pub db_path: String,
}

pub struct StateStore {
    db: Arc<DB>,
}

impl StateStore {
    pub fn new(config: StateStoreConfig) -> Result<Self> {
        info!("open state store with path {}", config.db_path.as_str());
        let path = Path::new(config.db_path.as_str());
        let mut db_builder = DB::new();
        db_builder.set_max_tables(8);
        let db = Arc::new(db_builder.open(path).map_err(|e| {
            DB3Error::OpenStoreError(
                config.db_path.to_string(),
                format!("fail to open db path for state_store with error {e}"),
            )
        })?);
        let txn = db
            .begin_rw_txn()
            .map_err(|e| DB3Error::WriteStoreError(format!("open tx {e}")))?;
        txn.create_table(Some(ACCOUNT_META_TABLE), TableFlags::CREATE)
            .map_err(|e| {
                DB3Error::WriteStoreError(format!(
                    "fail to create account meta table with error {e}"
                ))
            })?;
        txn.create_table(Some(CONFIG_META_TABLE), TableFlags::CREATE)
            .map_err(|e| {
                DB3Error::WriteStoreError(format!(
                    "fail to create config meta table with error {e}"
                ))
            })?;
        txn.commit().map_err(|e| {
            DB3Error::WriteStoreError(format!("fail to commit the transaction with error {e}"))
        })?;
        Ok(Self { db })
    }

    pub fn get_nonce(&self, id: &DB3Address) -> Result<u64> {
        let tx = self
            .db
            .begin_ro_txn()
            .map_err(|e| DB3Error::ReadStoreError(format!("open tx {e}")))?;
        let table = tx
            .open_table(Some(ACCOUNT_META_TABLE))
            .map_err(|e| DB3Error::ReadStoreError(format!("open table {e}")))?;
        let value = tx
            .get::<[u8; 8]>(&table, id.as_ref())
            .map_err(|e| DB3Error::ReadStoreError(format!("get value with key {e}")))?;
        if let Some(v) = value {
            Ok(u64::from_be_bytes(v))
        } else {
            Ok(0)
        }
    }

    pub fn store_node_config(&self, key: &str, config: &SystemConfig) -> Result<()> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| DB3Error::WriteStoreError(format!("open tx {e}")))?;
        let table = txn
            .open_table(Some(CONFIG_META_TABLE))
            .map_err(|e| DB3Error::WriteStoreError(format!("open table {e}")))?;
        let mut buf = BytesMut::with_capacity(8 * 1024);
        config
            .encode(&mut buf)
            .map_err(|e| DB3Error::WriteStoreError(format!("{}", e)))?;
        let buf = buf.freeze();
        txn.put(&table, key.as_bytes(), &buf, WriteFlags::UPSERT)
            .map_err(|e| DB3Error::WriteStoreError(format!("get value with key {e}")))?;
        txn.commit()
            .map_err(|e| DB3Error::WriteStoreError(format!("get value with key {e}")))?;
        Ok(())
    }

    pub fn get_node_config(&self, key: &str) -> Result<Option<SystemConfig>> {
        let tx = self
            .db
            .begin_ro_txn()
            .map_err(|e| DB3Error::ReadStoreError(format!("open tx {e}")))?;
        let table = tx
            .open_table(Some(CONFIG_META_TABLE))
            .map_err(|e| DB3Error::ReadStoreError(format!("open table {e}")))?;
        let value = tx
            .get::<Vec<u8>>(&table, key.as_bytes())
            .map_err(|e| DB3Error::WriteStoreError(format!("get value with key {e}")))?;
        if let Some(v) = value {
            match SystemConfig::decode(v.as_ref()) {
                Ok(config) => Ok(Some(config)),
                Err(e) => Err(DB3Error::ReadStoreError(format!(
                    "fail to decode database message {e}"
                ))),
            }
        } else {
            Ok(None)
        }
    }

    pub fn incr_nonce(&self, id: &DB3Address, nonce: u64) -> Result<u64> {
        let txn = self
            .db
            .begin_rw_txn()
            .map_err(|e| DB3Error::WriteStoreError(format!("open tx {e}")))?;
        let table = txn
            .open_table(Some(ACCOUNT_META_TABLE))
            .map_err(|e| DB3Error::WriteStoreError(format!("open table {e}")))?;
        let value = txn
            .get::<[u8; 8]>(&table, id.as_ref())
            .map_err(|e| DB3Error::WriteStoreError(format!("get value with key {e}")))?;
        if let Some(v) = value {
            let new_nonce = u64::from_be_bytes(v) + 1;
            if new_nonce == nonce {
                let buffer = new_nonce.to_be_bytes();
                txn.put(&table, id.as_ref(), &buffer, WriteFlags::UPSERT)
                    .map_err(|e| DB3Error::WriteStoreError(format!("get value with key {e}")))?;
                txn.commit()
                    .map_err(|e| DB3Error::WriteStoreError(format!("get value with key {e}")))?;
                Ok(new_nonce)
            } else {
                Err(DB3Error::WriteStoreError("bad nonce".to_string()))
            }
        } else {
            if nonce == 1 {
                let buffer = 1_u64.to_be_bytes();
                txn.put(&table, id.as_ref(), &buffer, WriteFlags::UPSERT)
                    .map_err(|e| DB3Error::WriteStoreError(format!("get value with key {e}")))?;
                txn.commit()
                    .map_err(|e| DB3Error::WriteStoreError(format!("get value with key {e}")))?;
                Ok(1)
            } else {
                Err(DB3Error::WriteStoreError("bad nonce".to_string()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn test_new_state_store() {
        let tmp_dir_path = TempDir::new("new_state_store_path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = StateStoreConfig { db_path: real_path };
        let result = StateStore::new(config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_nonce() {
        let tmp_dir_path = TempDir::new("nonce_").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = StateStoreConfig { db_path: real_path };
        if let Ok(store) = StateStore::new(config) {
            if let Err(e) = store.get_nonce(&DB3Address::ZERO) {
                println!("{e}");
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_incr_nonce() {
        let tmp_dir_path = TempDir::new("nonce_").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = StateStoreConfig { db_path: real_path };
        if let Ok(store) = StateStore::new(config) {
            let nonce = store.incr_nonce(&DB3Address::ZERO, 1).unwrap();
            assert_eq!(1, nonce);
            let result = store.incr_nonce(&DB3Address::ZERO, 1);
            assert_eq!(false, result.is_ok());
            let nonce = store.incr_nonce(&DB3Address::ZERO, 2).unwrap();
            assert_eq!(2, nonce);
        } else {
            assert!(false)
        }
    }
}
