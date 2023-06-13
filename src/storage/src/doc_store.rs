//
// doc_store.rs
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

use db3_crypto::db3_address::DB3Address;
use db3_crypto::id::DbId;
use db3_error::{DB3Error, Result};
use db3_proto::db3_database_v2_proto::{Index, IndexType};
use db3_proto::db3_mutation_v2_proto::{CollectionMutation, DocumentDatabaseMutation};
use ejdb2::EJDB;
use moka::sync::Cache;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

const EJDB_INDEX: [u8; 4] = [0x01u8, 0x04u8, 0x08u8, 0x10u8];
#[derive(Clone)]
pub struct DocStoreConfig {
    pub db_root_path: String,
    pub in_memory_db_handle_limit: u32,
}

pub struct DocStore {
    config: DocStoreConfig,
    dbs: Cache<Vec<u8>, Arc<Mutex<EJDB>>>,
}

impl DocStore {
    pub fn new(config: DocStoreConfig) -> Result<Self> {
        let dbs = Cache::new(config.in_memory_db_handle_limit as u64);
        Ok(Self { config, dbs })
    }

    fn open_db_internal(db_root_path: String, db_addr: DB3Address) -> Option<EJDB> {
        let db_addr_str = db_addr.to_hex();
        info!("open database with address {}", db_addr_str.as_str());
        let mut db = EJDB::new();
        let mut path = PathBuf::new();
        path.push(db_root_path.as_str());
        path.push(db_addr_str);
        if let Some(path_str) = path.as_path().to_str() {
            if let Ok(_) = db.open(path_str) {
                return Some(db);
            }
        }
        warn!("fail to open db with addr {}", db_addr.to_hex());
        None
    }

    pub fn create_database(
        &self,
        sender: &DB3Address,
        nonce: u64,
        network_id: u64,
    ) -> Result<DB3Address> {
        let db_addr = DbId::from((sender, nonce, network_id));
        //ensure init the database
        if let Some(_) = Self::open_db_internal(
            self.config.db_root_path.to_string(),
            db_addr.address().clone(),
        ) {
            Ok(db_addr.address().clone())
        } else {
            Err(DB3Error::WriteStoreError(
                "fail to open database".to_string(),
            ))
        }
    }

    pub fn create_collection(
        &self,
        db_addr: &DB3Address,
        collection: &CollectionMutation,
    ) -> Result<()> {
        //TODO validata the db address
        if collection.index_fields.len() > 0 {
            let key = db_addr.as_ref().to_vec();
            let add_addr_clone = db_addr.clone();
            let db_root_path = self.config.db_root_path.to_string();
            let db_entry = self.dbs.entry(key).or_optionally_insert_with(|| {
                if let Some(db) = Self::open_db_internal(db_root_path, add_addr_clone) {
                    Some(Arc::new(Mutex::new(db)))
                } else {
                    None
                }
            });
            if let Some(entry) = db_entry {
                match entry.value().lock() {
                    Ok(db) => {
                        for field in &collection.index_fields {
                            db.ensure_index(
                                collection.collection_name.as_str(),
                                field.path.as_str(),
                                EJDB_INDEX[field.index_type as usize],
                            )
                            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                        }
                    }
                    Err(_) => todo!(),
                }
            }
        }
        Ok(())
    }

    pub fn add_str_doc(&self, db_addr: &DB3Address, col_name: &str, doc: &str) -> Result<i64> {
        // validata the db and col
        let key = db_addr.as_ref().to_vec();
        let add_addr_clone = db_addr.clone();
        let db_root_path = self.config.db_root_path.to_string();
        let db_entry = self.dbs.entry(key).or_optionally_insert_with(|| {
            if let Some(db) = Self::open_db_internal(db_root_path, add_addr_clone) {
                Some(Arc::new(Mutex::new(db)))
            } else {
                None
            }
        });
        if let Some(entry) = db_entry {
            match entry.value().lock() {
                Ok(db) => {
                    let id = db
                        .put_new(col_name, &doc)
                        .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                    Ok(id)
                }
                Err(_) => todo!(),
            }
        } else {
            Err(DB3Error::WriteStoreError(format!(
                "no database found with addr {}",
                db_addr.to_hex()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn test_create_ejdb_database() {
        let tmp_dir_path = TempDir::new("new_mutation_store_path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = DocStoreConfig {
            db_root_path: real_path,
            in_memory_db_handle_limit: 16,
        };
        let doc_store = DocStore::new(config).unwrap();
        let db_id_ret = doc_store.create_database(&DB3Address::ZERO, 1, 1);
        assert!(db_id_ret.is_ok());
        let db_id = db_id_ret.unwrap();
        let collection = CollectionMutation {
            index_fields: vec![Index {
                path: "/f1".to_string(),
                index_type: IndexType::StringKey.into(),
            }],
            collection_name: "col1".to_string(),
        };
        let result = doc_store.create_collection(&db_id, &collection);
        assert!(result.is_ok());
        let result = doc_store.create_collection(&db_id, &collection);
        assert!(result.is_ok());
        let doc_str = r#"{"test":"v1", "f1":"f1"}"#;
        if let Ok(id) = doc_store.add_str_doc(&db_id, "col1", doc_str) {
            println!("the doc id {id}");
        } else {
            assert!(false);
        }
    }
}
