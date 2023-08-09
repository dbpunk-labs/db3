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
use db3_error::{DB3Error, Result};
use db3_proto::db3_database_v2_proto::{query_parameter, Index, Query};
use ejdb2::SetPlaceholder;
use ejdb2::{EJDBQuery, EJDB};
use moka::sync::Cache;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, warn};

const EJDB_INDEX: [u8; 4] = [0x01u8, 0x04u8, 0x08u8, 0x10u8];
#[derive(Clone)]
pub struct DocStoreConfig {
    pub db_root_path: String,
    pub in_memory_db_handle_limit: u32,
}

impl Default for DocStoreConfig {
    fn default() -> DocStoreConfig {
        DocStoreConfig {
            db_root_path: "".to_string(),
            in_memory_db_handle_limit: 0,
        }
    }
}

pub struct DocStore {
    config: DocStoreConfig,
    dbs: Cache<Vec<u8>, Arc<EJDB>>,
}

impl DocStore {
    pub fn mock() -> Self {
        let config = DocStoreConfig::default();
        let dbs = Cache::new(config.in_memory_db_handle_limit as u64);
        Self { config, dbs }
    }

    pub fn new(config: DocStoreConfig) -> Result<Self> {
        info!(
            "open indexer store with path {}",
            config.db_root_path.as_str()
        );
        let path = Path::new(config.db_root_path.as_str());
        if !path.exists() {
            fs::create_dir(path).map_err(|e| {
                DB3Error::OpenStoreError(config.db_root_path.to_string(), format!("{e}"))
            })?;
        }
        let dbs = Cache::new(config.in_memory_db_handle_limit as u64);
        Ok(Self { config, dbs })
    }

    fn open_db_internal(db_root_path: String, db_addr: DB3Address) -> Option<EJDB> {
        let db_addr_str = db_addr.to_hex();
        info!(
            "open database with address {} db path {}",
            db_addr_str.as_str(),
            db_root_path.as_str()
        );
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

    pub fn create_database(&self, addr: &DB3Address) -> Result<()> {
        //ensure init the database
        if let Some(_) = Self::open_db_internal(self.config.db_root_path.to_string(), addr.clone())
        {
            Ok(())
        } else {
            Err(DB3Error::WriteStoreError(
                "fail to open database".to_string(),
            ))
        }
    }

    pub fn add_index(&self, db_addr: &DB3Address, name: &str, indexes: &Vec<Index>) -> Result<()> {
        if indexes.len() > 0 {
            let key = db_addr.as_ref().to_vec();
            let add_addr_clone = db_addr.clone();
            let db_root_path = self.config.db_root_path.to_string();
            let db_entry = self.dbs.entry(key).or_optionally_insert_with(|| {
                if let Some(db) = Self::open_db_internal(db_root_path, add_addr_clone) {
                    Some(Arc::new(db))
                } else {
                    None
                }
            });
            if let Some(entry) = db_entry {
                for field in indexes {
                    entry
                        .value()
                        .ensure_index(
                            name,
                            field.path.as_str(),
                            EJDB_INDEX[field.index_type as usize],
                        )
                        .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                }
            }
        }
        Ok(())
    }

    pub fn add_str_doc(&self, db_addr: &DB3Address, col_name: &str, doc: &str) -> Result<i64> {
        let db_opt = self.get_db_ref(db_addr);
        if let Some(db) = db_opt {
            let id = db
                .put_new(col_name, &doc)
                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
            Ok(id)
        } else {
            Err(DB3Error::WriteStoreError(format!(
                "no database found with addr {}",
                db_addr.to_hex()
            )))
        }
    }

    pub fn delete_docs(&self, db_addr: &DB3Address, col_name: &str, ids: &Vec<i64>) -> Result<()> {
        let db_opt = self.get_db_ref(db_addr);
        if let Some(db) = db_opt {
            for id in ids {
                db.del(col_name, *id)
                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
            }
            Ok(())
        } else {
            Err(DB3Error::WriteStoreError(format!(
                "no database found with addr {}",
                db_addr.to_hex()
            )))
        }
    }

    pub fn delete_doc(&self, db_addr: &DB3Address, col_name: &str, id: i64) -> Result<()> {
        let db_opt = self.get_db_ref(db_addr);
        if let Some(db) = db_opt {
            db.del(col_name, id)
                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
            Ok(())
        } else {
            Err(DB3Error::WriteStoreError(format!(
                "no database found with addr {}",
                db_addr.to_hex()
            )))
        }
    }

    pub fn execute_query(
        &self,
        db_addr: &DB3Address,
        col_name: &str,
        query: &Query,
    ) -> Result<(Vec<(i64, String)>, u64)> {
        let mut prepared_statement = EJDBQuery::new(col_name, query.query_str.as_str());
        prepared_statement
            .init()
            .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
        for param in query.parameters.iter() {
            match &param.parameter {
                Some(query_parameter::Parameter::Int64Value(v)) => {
                    prepared_statement
                        .set_placeholder(param.name.as_str(), param.idx, *v)
                        .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
                }
                Some(query_parameter::Parameter::BoolValue(v)) => {
                    prepared_statement
                        .set_placeholder(param.name.as_str(), param.idx, *v)
                        .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
                }
                Some(query_parameter::Parameter::StrValue(v)) => {
                    prepared_statement
                        .set_placeholder(param.name.as_str(), param.idx, v.as_str())
                        .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
                }
                _ => {}
            }
        }
        let db_opt = self.get_db_ref(db_addr);
        let mut result = Vec::<(i64, String)>::new();
        if let Some(db) = db_opt {
            let count = db
                .exec::<String>(&prepared_statement, &mut result)
                .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
            Ok((result, count as u64))
        } else {
            Err(DB3Error::WriteStoreError(format!(
                "no database found with addr {}",
                db_addr.to_hex()
            )))
        }
    }

    fn get_db_ref(&self, db_addr: &DB3Address) -> Option<Arc<EJDB>> {
        let key = db_addr.as_ref().to_vec();
        let add_addr_clone = db_addr.clone();
        let db_root_path = self.config.db_root_path.to_string();
        let db_entry = self.dbs.entry(key).or_optionally_insert_with(|| {
            if let Some(db) = Self::open_db_internal(db_root_path, add_addr_clone) {
                Some(Arc::new(db))
            } else {
                None
            }
        });
        if let Some(entry) = db_entry {
            Some(entry.value().clone())
        } else {
            None
        }
    }

    pub fn get_doc(&self, db_addr: &DB3Address, col_name: &str, id: i64) -> Result<Option<String>> {
        let db_opt = self.get_db_ref(db_addr);
        if let Some(db) = db_opt {
            let opt = db
                .get::<String>(col_name, id)
                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
            Ok(Some(opt))
        } else {
            Ok(None)
        }
    }

    pub fn patch_docs(
        &self,
        db_addr: &DB3Address,
        col_name: &str,
        docs: &[String],
        doc_ids: &[i64],
    ) -> Result<()> {
        let db_opt = self.get_db_ref(db_addr);
        if let Some(db) = db_opt {
            for idx in 0..docs.len() {
                db.patch(col_name, &docs[idx].as_str(), doc_ids[idx])
                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
            }
            Ok(())
        } else {
            Err(DB3Error::WriteStoreError(format!(
                "no database found with addr {}",
                db_addr.to_hex()
            )))
        }
    }
    pub fn patch_doc(
        &self,
        db_addr: &DB3Address,
        col_name: &str,
        doc: &str,
        id: i64,
    ) -> Result<()> {
        let db_opt = self.get_db_ref(db_addr);
        if let Some(db) = db_opt {
            db.patch(col_name, &doc, id)
                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
            Ok(())
        } else {
            Err(DB3Error::WriteStoreError(format!(
                "no database found with addr {}",
                db_addr.to_hex()
            )))
        }
    }
    pub fn add_str_docs(
        &self,
        db_addr: &DB3Address,
        col_name: &str,
        docs: &Vec<String>,
        ids: &Vec<i64>,
    ) -> Result<()> {
        let db_opt = self.get_db_ref(db_addr);
        if let Some(db) = db_opt {
            for (i, doc) in docs.iter().enumerate() {
                db.put(col_name, doc, ids[i])
                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
            }
            Ok(())
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
    use db3_proto::db3_database_v2_proto::QueryParameter;
    use db3_proto::db3_database_v2_proto::{Index, IndexType};
    use tempdir::TempDir;

    fn prepare_the_dataset() -> (DocStore, i64) {
        let tmp_dir_path = TempDir::new("new_doc_store_path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = DocStoreConfig {
            db_root_path: real_path,
            in_memory_db_handle_limit: 16,
        };
        let doc_store = DocStore::new(config).unwrap();
        let db_id_ret = doc_store.create_database(&DB3Address::ZERO);
        assert!(db_id_ret.is_ok());
        let indexes = vec![Index {
            path: "/f1".to_string(),
            index_type: IndexType::StringKey.into(),
        }];
        let result = doc_store.add_index(&DB3Address::ZERO, "col1", &indexes);
        assert!(result.is_ok());
        let doc_str = r#"{"f2":"f2", "f1":"f1"}"#;
        let id = doc_store
            .add_str_doc(&DB3Address::ZERO, "col1", doc_str)
            .unwrap();
        (doc_store, id)
    }

    #[test]
    fn doc_get_test() {
        let (doc_store, id) = prepare_the_dataset();
        let doc_str = r#"{"f2":"f2", "f1":"f1"}"#;
        if let Ok(Some(value)) = doc_store.get_doc(&DB3Address::ZERO, "col1", id) {
            let value: serde_json::Value = serde_json::from_str(value.as_str()).unwrap();
            let right: serde_json::Value = serde_json::from_str(doc_str).unwrap();
            assert_eq!(value, right);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn doc_store_project_test() {
        let (doc_store, _) = prepare_the_dataset();
        let query_str = "/* | /{f1}";
        let query = Query {
            query_str: query_str.to_string(),
            parameters: vec![],
        };
        let (docs, count) = doc_store
            .execute_query(&DB3Address::ZERO, "col1", &query)
            .unwrap();
        let doc_str = r#"{"f1":"f1"}"#;
        let left: serde_json::Value = serde_json::from_str(docs[0].1.as_str()).unwrap();
        let right: serde_json::Value = serde_json::from_str(doc_str).unwrap();
        assert_eq!(count, 1);
        assert_eq!(left, right);
    }

    #[test]
    fn doc_store_filter_and_test() {
        let (doc_store, _) = prepare_the_dataset();
        let doc_str = r#"{"f2":"f", "f1":"f"}"#;
        let id = doc_store
            .add_str_doc(&DB3Address::ZERO, "col1", doc_str)
            .unwrap();
        assert!(id > 0);

        {
            let query_str = "/[f1=\"f1\"] and /[f2=\"f2\"]";
            let query = Query {
                query_str: query_str.to_string(),
                parameters: vec![],
            };
            let (_result, count) = doc_store
                .execute_query(&DB3Address::ZERO, "col1", &query)
                .unwrap();
            assert_eq!(1, count);
        }
    }

    #[test]
    fn doc_store_project_count_test() {
        let (doc_store, _) = prepare_the_dataset();
        let query_str = "/* | count";
        let query = Query {
            query_str: query_str.to_string(),
            parameters: vec![],
        };
        let (docs, count) = doc_store
            .execute_query(&DB3Address::ZERO, "col1", &query)
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn doc_store_smoke_test() {
        let (doc_store, id) = prepare_the_dataset();
        let db_id = DB3Address::ZERO;
        if let Ok(Some(value)) = doc_store.get_doc(&db_id, "col1", id) {
            println!("{}", value.as_str());
            let value: serde_json::Value = serde_json::from_str(value.as_str()).unwrap();
            assert_eq!(value["f1"].as_str(), Some("f1"));
        } else {
            assert!(false);
        }
        let query = Query {
            query_str: "/*".to_string(),
            parameters: vec![],
        };
        if let Ok((result, count)) = doc_store.execute_query(&DB3Address::ZERO, "col1", &query) {
            assert_eq!(1, count);
            assert_eq!(id, result[0].0);
        }
        let query = Query {
            query_str: "/[f1 eq ?]".to_string(),
            parameters: vec![QueryParameter {
                name: "f1".to_string(),
                parameter: Some(query_parameter::Parameter::StrValue("f1".to_string())),
                idx: 0,
            }],
        };
        if let Ok((result, count)) = doc_store.execute_query(&DB3Address::ZERO, "col1", &query) {
            assert_eq!(1, count);
            assert_eq!(id, result[0].0);
        }

        let query = Query {
            query_str: "/[f1 eq ?]".to_string(),
            parameters: vec![QueryParameter {
                name: "f1".to_string(),
                parameter: Some(query_parameter::Parameter::StrValue("f2".to_string())),
                idx: 0,
            }],
        };
        if let Ok((_result, count)) = doc_store.execute_query(&DB3Address::ZERO, "col1", &query) {
            assert_eq!(0, count);
        }

        let query = Query {
            query_str: "/[f1 eq ? and test eq 'v1'] ".to_string(),
            parameters: vec![QueryParameter {
                name: "f1".to_string(),
                parameter: Some(query_parameter::Parameter::StrValue("f1".to_string())),
                idx: 0,
            }],
        };

        if let Ok((_result, count)) = doc_store.execute_query(&DB3Address::ZERO, "col1", &query) {
            assert_eq!(1, count);
        }

        let doc_str = r#"{"test":"v2", "f1":"f1"}"#;
        if let Err(_) = doc_store.patch_doc(&db_id, "col1", doc_str, id) {
            assert!(false);
        }

        if let Ok(Some(value)) = doc_store.get_doc(&db_id, "col1", id) {
            let value: serde_json::Value = serde_json::from_str(value.as_str()).unwrap();
            assert_eq!(value["test"].as_str(), Some("v2"));
        } else {
            assert!(false);
        }

        if let Err(_) = doc_store.delete_doc(&db_id, "col1", id) {
            assert!(false);
        }
        let result = doc_store.get_doc(&DB3Address::ZERO, "col1", 1);
        assert!(result.is_err());
    }
}
