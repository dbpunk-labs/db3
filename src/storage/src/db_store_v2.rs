//
// db_store_v2.rs
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
//
use crate::collection_key;
use crate::db_doc_key_v2::DbDocKeyV2;
use crate::db_owner_key_v2::DbOwnerKey;
use crate::doc_store::{DocStore, DocStoreConfig};
use bytes::BytesMut;
use chashmap::CHashMap;
use db3_crypto::db3_address::DB3Address;
use db3_crypto::id::DbId;
use db3_crypto::id_v2::OpEntryId;
use db3_error::{DB3Error, Result};
use db3_proto::db3_database_v2_proto::{
    database_message, Collection, DatabaseMessage, Document, DocumentDatabase, EventDatabase, Query,
};
use db3_proto::db3_mutation_v2_proto::{
    CollectionMutation, DocumentDatabaseMutation, EventDatabaseMutation,
};
use prost::Message;
use rocksdb::{DBRawIteratorWithThreadMode, DBWithThreadMode, MultiThreaded, Options, WriteBatch};
use std::collections::HashMap;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

type StorageEngine = DBWithThreadMode<MultiThreaded>;
type DBRawIterator<'a> = DBRawIteratorWithThreadMode<'a, StorageEngine>;

#[derive(Clone)]
pub struct DBStoreV2Config {
    pub db_path: String,
    pub db_store_cf_name: String,
    pub doc_store_cf_name: String,
    pub collection_store_cf_name: String,
    pub index_store_cf_name: String,
    pub doc_owner_store_cf_name: String,
    pub db_owner_store_cf_name: String,
    pub scan_max_limit: usize,
    pub enable_doc_store: bool,
    pub doc_store_conf: DocStoreConfig,
}

struct CollectionState {
    // the total doc count
    pub total_doc_count: u64,
}

struct DatabaseState {
    pub doc_order: i64,
    pub collection_state: HashMap<String, CollectionState>,
    pub total_doc_count: u64,
}

#[derive(Clone)]
pub struct DBStoreV2 {
    config: DBStoreV2Config,
    se: Arc<StorageEngine>,
    doc_store: Arc<DocStore>,
    db_state: Arc<CHashMap<String, DatabaseState>>,
}

impl DBStoreV2 {
    pub fn new(config: DBStoreV2Config) -> Result<Self> {
        let mut cf_opts = Options::default();
        cf_opts.create_if_missing(true);
        cf_opts.create_missing_column_families(true);
        info!("open db store with path {}", config.db_path.as_str());
        let path = Path::new(config.db_path.as_str());
        let se = Arc::new(
            StorageEngine::open_cf(
                &cf_opts,
                &path,
                [
                    config.db_store_cf_name.as_str(),
                    config.doc_store_cf_name.as_str(),
                    config.collection_store_cf_name.as_str(),
                    config.index_store_cf_name.as_str(),
                    config.doc_owner_store_cf_name.as_str(),
                    config.db_owner_store_cf_name.as_str(),
                ],
            )
            .map_err(|e| {
                DB3Error::OpenStoreError(config.db_path.to_string(), format!("db_store_v2 {e}"))
            })?,
        );
        let doc_store = match config.enable_doc_store {
            false => Arc::new(DocStore::mock()),
            true => Arc::new(DocStore::new(config.doc_store_conf.clone())?),
        };
        Ok(Self {
            config,
            se,
            doc_store,
            db_state: Arc::new(CHashMap::new()),
        })
    }

    fn update_db_state_for_add_db(&self, db_addr: &str) {
        self.db_state.insert(
            db_addr.to_string(),
            DatabaseState {
                doc_order: 0,
                collection_state: HashMap::new(),
                total_doc_count: 0,
            },
        );
    }

    fn update_db_state_for_new_collection(&self, db_addr: &str, col: &str) {
        if let Some(mut write_guard) = self.db_state.get_mut(db_addr) {
            let database_state = write_guard.deref_mut();
            database_state
                .collection_state
                .insert(col.to_string(), CollectionState { total_doc_count: 0 });
        }
    }

    fn update_db_state_for_delete_docs(&self, db_addr: &str, col: &str, count: u64) {
        if let Some(mut write_guard) = self.db_state.get_mut(db_addr) {
            let database_state = write_guard.deref_mut();
            database_state.total_doc_count = database_state.total_doc_count - count;
            if let Some(collection_state) = database_state.collection_state.get_mut(col) {
                collection_state.total_doc_count = collection_state.total_doc_count - count;
            }
        }
    }

    fn update_db_state_for_add_docs(
        &self,
        db_addr_hex: &str,
        col: &str,
        count: usize,
    ) -> Result<Option<Vec<i64>>> {
        if let Some(mut write_guard) = self.db_state.get_mut(db_addr_hex) {
            let database_state = write_guard.deref_mut();
            let mut ids: Vec<i64> = Vec::new();
            for id in 1..(count + 1) {
                ids.push(database_state.doc_order + id as i64);
            }
            database_state.doc_order = database_state.doc_order + count as i64;
            database_state.total_doc_count = database_state.total_doc_count + count as u64;
            if let Some(collection_state) = database_state.collection_state.get_mut(col) {
                collection_state.total_doc_count = collection_state.total_doc_count + count as u64;
            }
            Ok(Some(ids))
        } else {
            Ok(None)
        }
    }

    pub fn get_collection_of_database(&self, db_addr: &DB3Address) -> Result<Vec<Collection>> {
        self.get_entries_with_prefix::<Collection>(
            db_addr.as_ref(),
            self.config.collection_store_cf_name.as_str(),
        )
    }

    pub fn get_database_of_owner(&self, owner: &DB3Address) -> Result<Vec<DatabaseMessage>> {
        let cf_handle = self
            .se
            .cf_handle(self.config.db_owner_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it: DBRawIterator = self.se.prefix_iterator_cf(&cf_handle, owner).into();
        let mut entries: Vec<DatabaseMessage> = Vec::new();
        while it.valid() {
            if let Some(k) = it.key() {
                if &k[0..owner.as_ref().len()] != owner.as_ref() {
                    break;
                }
            } else {
                break;
            }
            if let Some(v) = it.value() {
                let addr = DB3Address::try_from(v)
                    .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
                if let Ok(Some(d)) = self.get_database(&addr) {
                    entries.push(d);
                }
            }
            it.next();
        }
        Ok(entries)
    }

    fn get_entries_with_prefix<T>(&self, prefix: &[u8], cf: &str) -> Result<Vec<T>>
    where
        T: Message + std::default::Default,
    {
        let cf_handle = self
            .se
            .cf_handle(cf)
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it: DBRawIterator = self.se.prefix_iterator_cf(&cf_handle, prefix).into();
        let mut entries: Vec<T> = Vec::new();
        while it.valid() {
            if let Some(k) = it.key() {
                if &k[0..prefix.len()] != prefix {
                    break;
                }
            } else {
                break;
            }
            if let Some(v) = it.value() {
                match T::decode(v.as_ref()) {
                    Ok(c) => {
                        entries.push(c);
                    }
                    Err(e) => {
                        return Err(DB3Error::ReadStoreError(format!("{e}")));
                    }
                }
            }
            it.next();
        }
        Ok(entries)
    }

    fn get_entry<T>(&self, cf: &str, id: &[u8]) -> Result<Option<T>>
    where
        T: Message + std::default::Default,
    {
        let cf_handle = self
            .se
            .cf_handle(cf)
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let value = self
            .se
            .get_cf(&cf_handle, id)
            .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
        if let Some(v) = value {
            match T::decode(v.as_ref()) {
                Ok(c) => Ok(Some(c)),
                Err(e) => Err(DB3Error::ReadStoreError(format!("{e}"))),
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_collection(&self, db_addr: &DB3Address, name: &str) -> Result<Option<Collection>> {
        let ck = collection_key::build_collection_key(db_addr, name)
            .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
        let ck_ref: &[u8] = ck.as_ref();
        self.get_entry::<Collection>(self.config.collection_store_cf_name.as_str(), ck_ref)
    }

    pub fn create_collection(
        &self,
        sender: &DB3Address,
        db_addr: &DB3Address,
        collection: &CollectionMutation,
        block: u64,
        order: u32,
        idx: u16,
    ) -> Result<()> {
        let db = self.get_database(db_addr)?;
        if db.is_none() {
            return Err(DB3Error::ReadStoreError(
                "fail to find database".to_string(),
            ));
        }
        if self.is_db_collection_exist(db_addr, collection.collection_name.as_str())? {
            return Err(DB3Error::ReadStoreError(
                "collection with name exist".to_string(),
            ));
        }
        let ck = collection_key::build_collection_key(db_addr, collection.collection_name.as_str())
            .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
        let collection_store_cf_handle = self
            .se
            .cf_handle(self.config.collection_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let ck_ref: &[u8] = ck.as_ref();
        let id = OpEntryId::create(block, order, idx)
            .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
        // validate the index
        let col = Collection {
            id: id.as_ref().to_vec(),
            name: collection.collection_name.to_string(),
            index_fields: collection.index_fields.to_vec(),
            sender: sender.as_ref().to_vec(),
        };
        let mut buf = BytesMut::with_capacity(1024);
        col.encode(&mut buf)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        let buf = buf.freeze();
        let mut batch = WriteBatch::default();
        batch.put_cf(&collection_store_cf_handle, ck_ref, buf.as_ref());
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        let db_addr_hex = db_addr.to_hex();
        self.update_db_state_for_new_collection(
            db_addr_hex.as_str(),
            collection.collection_name.as_str(),
        );
        if self.config.enable_doc_store {
            self.doc_store
                .create_collection(db_addr, collection)
                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        }
        Ok(())
    }

    pub fn get_database(&self, db_addr: &DB3Address) -> Result<Option<DatabaseMessage>> {
        self.get_entry::<DatabaseMessage>(self.config.db_store_cf_name.as_str(), db_addr.as_ref())
    }

    pub fn update_docs(
        &self,
        db_addr: &DB3Address,
        sender: &DB3Address,
        col_name: &str,
        docs: &Vec<String>,
        doc_ids: &Vec<i64>,
    ) -> Result<()> {
        if !self.is_db_collection_exist(db_addr, col_name)? {
            return Err(DB3Error::CollectionNotFound(
                col_name.to_string(),
                db_addr.to_hex(),
            ));
        }
        self.verify_docs_ownership(sender, db_addr, doc_ids)?;
        if self.config.enable_doc_store {
            //TODO add id-> owner mapping to control the permissions
            self.doc_store.patch_docs(db_addr, col_name, docs, &doc_ids)
        } else {
            Ok(())
        }
    }

    pub fn query_docs(
        &self,
        db_addr: &DB3Address,
        col_name: &str,
        query: &Query,
    ) -> Result<Vec<Document>> {
        if !self.is_db_collection_exist(db_addr, col_name)? {
            return Err(DB3Error::ReadStoreError(
                "collection name {col_name} does not exist".to_string(),
            ));
        }
        if self.config.enable_doc_store {
            let result = self.doc_store.execute_query(db_addr, col_name, query)?;
            let mut documents = vec![];

            for (id, doc) in result {
                documents.push(Document {
                    id,
                    doc: doc.to_string(),
                })
            }
            Ok(documents)
        } else {
            Ok(vec![])
        }
    }

    pub fn delete_docs(
        &self,
        db_addr: &DB3Address,
        sender: &DB3Address,
        col_name: &str,
        doc_ids: &Vec<i64>,
    ) -> Result<()> {
        if !self.is_db_collection_exist(db_addr, col_name)? {
            return Err(DB3Error::CollectionNotFound(
                col_name.to_string(),
                db_addr.to_hex(),
            ));
        }
        self.verify_docs_ownership(sender, db_addr, doc_ids)?;
        if self.config.enable_doc_store {
            //TODO add id-> owner mapping to control the permissions
            self.doc_store.delete_docs(db_addr, col_name, doc_ids)?;
        }
        let db_addr_hex = db_addr.to_hex();
        self.update_db_state_for_delete_docs(db_addr_hex.as_str(), col_name, doc_ids.len() as u64);
        self.delete_doc_ids_from_owner_store(db_addr, doc_ids)
    }

    pub fn add_docs(
        &self,
        db_addr: &DB3Address,
        sender: &DB3Address,
        col_name: &str,
        docs: &Vec<String>,
    ) -> Result<Vec<i64>> {
        if !self.is_db_collection_exist(db_addr, col_name)? {
            return Err(DB3Error::CollectionNotFound(
                col_name.to_string(),
                db_addr.to_hex(),
            ));
        }
        let db_addr_hex = db_addr.to_hex();
        let doc_ids =
            self.update_db_state_for_add_docs(db_addr_hex.as_str(), col_name, docs.len())?;
        if let Some(all_doc_ids) = doc_ids {
            self.create_doc_ownership(sender, db_addr, &all_doc_ids)?;
            // add db+id-> owner mapping to control the permissions
            if self.config.enable_doc_store {
                self.doc_store
                    .add_str_docs(db_addr, col_name, docs, &all_doc_ids)?;
            }
            return Ok(all_doc_ids);
        }
        Ok(vec![])
    }

    /// verify if the collection exists in the given db
    pub fn is_db_collection_exist(&self, db_addr: &DB3Address, col_name: &str) -> Result<bool> {
        let ck = collection_key::build_collection_key(db_addr, col_name)
            .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
        let collection_store_cf_handle = self
            .se
            .cf_handle(self.config.collection_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let ck_ref: &[u8] = ck.as_ref();
        let value = self
            .se
            .get_cf(&collection_store_cf_handle, ck_ref)
            .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
        Ok(value.is_some())
    }

    /// clean doc ids that are not in the collection
    pub fn delete_doc_ids_from_owner_store(
        &self,
        db_addr: &DB3Address,
        doc_ids: &Vec<i64>,
    ) -> Result<()> {
        let doc_owner_store_cf_handle = self
            .se
            .cf_handle(self.config.doc_owner_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut batch = WriteBatch::default();
        for id in doc_ids {
            let db_doc_key = DbDocKeyV2(db_addr, *id).encode()?;
            batch.delete_cf(&doc_owner_store_cf_handle, &db_doc_key);
        }
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        Ok(())
    }

    /// verify the ownership of the doc ids
    pub fn verify_docs_ownership(
        &self,
        sender: &DB3Address,
        db_addr: &DB3Address,
        doc_ids: &Vec<i64>,
    ) -> Result<()> {
        let doc_owner_store_cf_handle = self
            .se
            .cf_handle(self.config.doc_owner_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        for id in doc_ids {
            let db_doc_key = DbDocKeyV2(db_addr, *id).encode().unwrap();
            let value = self
                .se
                .get_cf(&doc_owner_store_cf_handle, db_doc_key)
                .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
            if let Some(owner) = value {
                if owner != sender.as_ref() {
                    return Err(DB3Error::OwnerVerifyFailed(format!(
                        "doc owner is not the sender"
                    )));
                }
            } else {
                return Err(DB3Error::OwnerVerifyFailed(format!("doc id is not found")));
            }
        }
        Ok(())
    }

    pub fn get_doc_key_from_doc_id(&self, doc_id: i64) -> Result<Vec<u8>> {
        let doc_owner_store_cf_handle = self
            .se
            .cf_handle(self.config.doc_owner_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let value = self
            .se
            .get_cf(&doc_owner_store_cf_handle, doc_id.to_be_bytes().as_ref())
            .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
        match value {
            Some(v) => Ok(v),
            None => {
                return Err(DB3Error::ReadStoreError(format!(
                    "doc owner key not found for doc id {}",
                    doc_id
                )))
            }
        }
    }
    /// create db+id-> owner mapping to control the permissions
    pub fn create_doc_ownership(
        &self,
        sender: &DB3Address,
        db_addr: &DB3Address,
        doc_ids: &Vec<i64>,
    ) -> Result<()> {
        let doc_owner_store_cf_handle = self
            .se
            .cf_handle(self.config.doc_owner_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut batch = WriteBatch::default();
        for id in doc_ids {
            let db_doc_key = DbDocKeyV2(db_addr, *id);
            let encoded_db_doc_key = db_doc_key.encode()?;
            batch.put_cf(
                &doc_owner_store_cf_handle,
                &encoded_db_doc_key,
                sender.as_ref(),
            );
        }
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        Ok(())
    }

    pub fn create_event_database(
        &self,
        sender: &DB3Address,
        mutation: &EventDatabaseMutation,
        nonce: u64,
        network_id: u64,
        block: u64,
        order: u32,
    ) -> Result<DbId> {
        let db_addr = DbId::from((sender, nonce, network_id));
        let db_store_cf_handle = self
            .se
            .cf_handle(self.config.db_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let db_owner_store_cf_handle = self
            .se
            .cf_handle(self.config.db_owner_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let db_owner = DbOwnerKey(sender, block, order);
        let db_owner_encoded_key = db_owner.encode()?;
        //TODO check the name
        let database = EventDatabase {
            address: db_addr.as_ref().to_vec(),
            sender: sender.as_ref().to_vec(),
            desc: mutation.desc.to_string(),
            contract_address: mutation.contract_address.to_string(),
            ttl: mutation.ttl,
            events_json_abi: mutation.events_json_abi.to_string(),
            evm_node_url: mutation.evm_node_url.to_string(),
        };
        let database_msg = DatabaseMessage {
            database: Some(database_message::Database::EventDb(database)),
        };
        let mut buf = BytesMut::with_capacity(1024);
        database_msg
            .encode(&mut buf)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        let buf = buf.freeze();
        let mut batch = WriteBatch::default();
        batch.put_cf(&db_store_cf_handle, db_addr.as_ref(), buf.as_ref());
        batch.put_cf(
            &db_owner_store_cf_handle,
            &db_owner_encoded_key,
            db_addr.as_ref(),
        );
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;

        let db_addr_hex = db_addr.to_hex();
        self.update_db_state_for_add_db(db_addr_hex.as_str());
        for (idx, cm) in mutation.tables.iter().enumerate() {
            self.create_collection(sender, db_addr.address(), cm, block, order, idx as u16)?;
        }
        if self.config.enable_doc_store {
            self.doc_store
                .create_database(db_addr.address())
                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        }
        Ok(db_addr)
    }

    pub fn create_doc_database(
        &self,
        sender: &DB3Address,
        mutation: &DocumentDatabaseMutation,
        nonce: u64,
        network_id: u64,
        block: u64,
        order: u32,
    ) -> Result<DbId> {
        let db_addr = DbId::from((sender, nonce, network_id));
        let db_store_cf_handle = self
            .se
            .cf_handle(self.config.db_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let db_owner_store_cf_handle = self
            .se
            .cf_handle(self.config.db_owner_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let db_owner = DbOwnerKey(sender, block, order);
        let db_owner_encoded_key = db_owner.encode()?;
        let database = DocumentDatabase {
            address: db_addr.as_ref().to_vec(),
            sender: sender.as_ref().to_vec(),
            desc: mutation.db_desc.to_string(),
        };
        let database_msg = DatabaseMessage {
            database: Some(database_message::Database::DocDb(database)),
        };
        let mut buf = BytesMut::with_capacity(1024);
        database_msg
            .encode(&mut buf)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        let buf = buf.freeze();
        let mut batch = WriteBatch::default();
        batch.put_cf(&db_store_cf_handle, db_addr.as_ref(), buf.as_ref());
        batch.put_cf(
            &db_owner_store_cf_handle,
            &db_owner_encoded_key,
            db_addr.as_ref(),
        );
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        let db_addr_hex = db_addr.to_hex();
        self.update_db_state_for_add_db(db_addr_hex.as_str());
        if self.config.enable_doc_store {
            self.doc_store
                .create_database(db_addr.address())
                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        }
        Ok(db_addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn test_new_db_store() {
        let tmp_dir_path = TempDir::new("new_db_store_path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = DBStoreV2Config {
            db_path: real_path,
            db_store_cf_name: "db".to_string(),
            doc_store_cf_name: "doc".to_string(),
            collection_store_cf_name: "cf2".to_string(),
            index_store_cf_name: "index".to_string(),
            doc_owner_store_cf_name: "doc_owner".to_string(),
            db_owner_store_cf_name: "db_owner".to_string(),
            scan_max_limit: 50,
            enable_doc_store: false,
            doc_store_conf: DocStoreConfig::default(),
        };
        let result = DBStoreV2::new(config);
        assert_eq!(result.is_ok(), true);
    }
    #[test]
    fn test_collection_test() {
        let tmp_dir_path = TempDir::new("new_database").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = DBStoreV2Config {
            db_path: real_path,
            db_store_cf_name: "db".to_string(),
            doc_store_cf_name: "doc".to_string(),
            collection_store_cf_name: "cf2".to_string(),
            index_store_cf_name: "index".to_string(),
            doc_owner_store_cf_name: "doc_owner".to_string(),
            db_owner_store_cf_name: "db_owner".to_string(),
            scan_max_limit: 50,
            enable_doc_store: false,
            doc_store_conf: DocStoreConfig::default(),
        };
        let result = DBStoreV2::new(config);
        assert_eq!(result.is_ok(), true);
        let db_m = DocumentDatabaseMutation {
            db_desc: "test_desc".to_string(),
        };
        let db3_store = result.unwrap();
        let result = db3_store.create_doc_database(&DB3Address::ZERO, &db_m, 1, 1, 1, 1);
        assert!(result.is_ok());
        let db_id = result.unwrap();
        if let Ok(Some(db)) = db3_store.get_database(db_id.address()) {
            if let Some(database_message::Database::DocDb(doc_db)) = db.database {
                assert_eq!("test_desc", doc_db.desc.as_str());
            }
        } else {
            assert!(false);
        }
        let collection = CollectionMutation {
            index_fields: vec![],
            collection_name: "col1".to_string(),
        };
        let result =
            db3_store.create_collection(&DB3Address::ZERO, db_id.address(), &collection, 1, 1, 1);
        assert!(result.is_ok());
        let result = db3_store.get_collection(db_id.address(), "col1");
        if let Ok(Some(_c)) = result {
            assert!(true);
        } else {
            assert!(false);
        }
        if let Ok(cl) = db3_store.get_collection_of_database(db_id.address()) {
            assert_eq!(cl.len(), 1);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_create_doc_db() {
        let tmp_dir_path = TempDir::new("new_database").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = DBStoreV2Config {
            db_path: real_path,
            db_store_cf_name: "db".to_string(),
            doc_store_cf_name: "doc".to_string(),
            collection_store_cf_name: "cf2".to_string(),
            index_store_cf_name: "index".to_string(),
            doc_owner_store_cf_name: "doc_owner".to_string(),
            db_owner_store_cf_name: "db_owner".to_string(),
            scan_max_limit: 50,
            enable_doc_store: false,
            doc_store_conf: DocStoreConfig::default(),
        };
        let result = DBStoreV2::new(config);
        assert_eq!(result.is_ok(), true);
        let db_m = DocumentDatabaseMutation {
            db_desc: "test_desc".to_string(),
        };
        let db3_store = result.unwrap();
        let result = db3_store.create_doc_database(&DB3Address::ZERO, &db_m, 1, 1, 1, 1);
        assert!(result.is_ok());
        let db_id = result.unwrap();
        if let Ok(Some(db)) = db3_store.get_database(db_id.address()) {
            if let Some(database_message::Database::DocDb(doc_db)) = db.database {
                assert_eq!("test_desc", doc_db.desc.as_str());
            }
        } else {
            assert!(false);
        }

        if let Ok(dbs) = db3_store.get_database_of_owner(&DB3Address::ZERO) {
            assert_eq!(dbs.len(), 1);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_increase_db_doc_order_ut() {
        let tmp_dir_path = TempDir::new("new_database").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = DBStoreV2Config {
            db_path: real_path,
            db_store_cf_name: "db".to_string(),
            doc_store_cf_name: "doc".to_string(),
            collection_store_cf_name: "cf2".to_string(),
            index_store_cf_name: "index".to_string(),
            doc_owner_store_cf_name: "doc_owner".to_string(),
            db_owner_store_cf_name: "db_owner".to_string(),
            scan_max_limit: 50,
            enable_doc_store: false,
            doc_store_conf: DocStoreConfig::default(),
        };
        let result = DBStoreV2::new(config);
        assert_eq!(result.is_ok(), true);
        let db_m = DocumentDatabaseMutation {
            db_desc: "test_desc".to_string(),
        };
        let db3_store = result.unwrap();
        let result = db3_store.create_doc_database(&DB3Address::ZERO, &db_m, 1, 1, 1, 1);
        assert!(result.is_ok());
        let db_id_1 = result.unwrap();
        let result = db3_store.create_doc_database(&DB3Address::ZERO, &db_m, 2, 1, 2, 1);
        assert!(result.is_ok());
        let db_id_2 = result.unwrap();

        let result = db3_store
            .update_db_state_for_add_docs(&db_id_1.address().to_hex(), "col1", 3)
            .unwrap();
        assert_eq!(result, Some(vec![1, 2, 3]));
    }
}
