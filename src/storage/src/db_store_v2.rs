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
use db3_base::bson_util::bytes_to_bson_document;
use db3_crypto::db3_address::DB3Address;
use db3_crypto::id::DbId;
use db3_error::{DB3Error, Result};
use db3_proto::db3_database_v2_proto::{
    database_message, BlockState, Collection, CollectionState as CollectionStateProto,
    DatabaseMessage, DatabaseState as DatabaseStateProto, DatabaseStatePersistence, Document,
    DocumentDatabase, EventDatabase, Index, Query,
};
use db3_proto::db3_mutation_v2_proto::mutation::body_wrapper::Body;
use db3_proto::db3_mutation_v2_proto::{
    CollectionMutation, DocumentDatabaseMutation, EventDatabaseMutation, Mutation, MutationAction,
};
use db3_proto::db3_storage_proto::ExtraItem;
use prost::Message;
use rocksdb::{DBRawIteratorWithThreadMode, DBWithThreadMode, MultiThreaded, Options, WriteBatch};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info, warn};
type StorageEngine = DBWithThreadMode<MultiThreaded>;
type DBRawIterator<'a> = DBRawIteratorWithThreadMode<'a, StorageEngine>;

const STATE_CF: &str = "DB_STATE_CF";
const BLOCK_STATE_CF: &str = "BLOCK_STATE_CF";
const BLOCK_STATE_KEY: &str = "BLOCK_STATE_KEY";

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
    pub doc_start_id: i64,
}

#[derive(Clone)]
struct CollectionState {
    // the total doc count
    pub total_doc_count: u64,
}

#[derive(Clone)]
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
                    STATE_CF,
                    BLOCK_STATE_CF,
                ],
            )
            .map_err(|e| {
                DB3Error::OpenStoreError(
                    config.db_path.to_string(),
                    format!("fail to open column family for db store v2 with error {e}"),
                )
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
    pub fn flush(&self) -> Result<()> {
        self.se
            .flush()
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))
    }

    pub fn flush_database_state(&self) -> Result<()> {
        let cf_handle = self
            .se
            .cf_handle(self.config.db_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it = self.se.raw_iterator_cf(&cf_handle);
        it.seek_to_first();
        loop {
            if !it.valid() {
                break;
            }
            if let Some(k) = it.key() {
                let addr = DB3Address::try_from(k)?;
                if let Some(state) = self.build_persistence_state(&addr) {
                    self.put_entry::<DatabaseStatePersistence>(STATE_CF, k, state)?;
                }
            }
            it.next();
        }
        Ok(())
    }

    pub fn get_event_db(&self, addr: &DB3Address) -> Result<Option<EventDatabase>> {
        let database = self.get_database(addr)?;
        if let Some(db) = database {
            if let Some(database_message::Database::EventDb(event_db)) = db.database {
                return Ok(Some(event_db));
            }
        }
        return Ok(None);
    }

    pub fn get_all_event_db(&self) -> Result<Vec<EventDatabase>> {
        let cf_handle = self
            .se
            .cf_handle(self.config.db_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it = self.se.raw_iterator_cf(&cf_handle);
        it.seek_to_first();
        let mut databases: Vec<EventDatabase> = Vec::new();
        loop {
            if !it.valid() {
                break;
            }
            if let Some(value) = it.value() {
                match DatabaseMessage::decode(value.as_ref()) {
                    Ok(c) => {
                        if let Some(database_message::Database::EventDb(db)) = c.database {
                            databases.push(db)
                        }
                    }
                    Err(e) => return Err(DB3Error::ReadStoreError(format!("{e}"))),
                }
            }
            it.next();
        }
        Ok(databases)
    }

    ///
    /// execute the function before exposing rpc service
    ///
    pub fn recover_db_state(&self) -> Result<()> {
        // recover states
        let cf_handle = self
            .se
            .cf_handle(self.config.db_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it = self.se.raw_iterator_cf(&cf_handle);
        it.seek_to_first();
        loop {
            if !it.valid() {
                break;
            }
            if let Some(key) = it.key() {
                let key_ref: &[u8] = key.as_ref();
                let address = DB3Address::try_from(key_ref)?;
                let address_str = address.to_hex();
                if let Some(state) = self.recover_from_state(&address).map_err(|e| {
                    DB3Error::ReadStoreError(format!("fail to convert db state with err {e}"))
                })? {
                    info!(
                        "recover db {} with doc order {} and total doc count {} from local state",
                        address_str.as_str(),
                        state.doc_order,
                        state.total_doc_count
                    );
                    let collection_state: HashMap<String, CollectionState> = state
                        .collection_states
                        .iter()
                        .map(|item| {
                            (
                                item.0.to_string(),
                                CollectionState {
                                    total_doc_count: item.1.total_doc_count,
                                },
                            )
                        })
                        .collect();
                    self.db_state.insert(
                        address_str,
                        DatabaseState {
                            doc_order: state.doc_order + 1,
                            collection_state,
                            total_doc_count: state.total_col_count,
                        },
                    );
                } else {
                    let collections = self.get_entries_with_prefix::<Collection>(
                        address.as_ref(),
                        self.config.collection_store_cf_name.as_str(),
                    )?;

                    let collection_state: HashMap<String, CollectionState> = collections
                        .iter()
                        .map(|item| {
                            (
                                item.name.to_string(),
                                CollectionState { total_doc_count: 0 },
                            )
                        })
                        .collect();

                    // try recover from doc ownership
                    if let Some(doc_order) = self.recover_db_doc_id(&address)? {
                        info!(
                            "recover db {} with doc order {} from doc owner store",
                            address_str.as_str(),
                            doc_order
                        );
                        self.db_state.insert(
                            address_str,
                            DatabaseState {
                                doc_order: doc_order + 1,
                                collection_state,
                                total_doc_count: 0,
                            },
                        );
                    } else {
                        // fail back the doc id config
                        info!(
                            "recover db {} with doc order {}",
                            address_str.as_str(),
                            self.config.doc_start_id
                        );
                        self.db_state.insert(
                            address_str,
                            DatabaseState {
                                doc_order: self.config.doc_start_id,
                                collection_state,
                                total_doc_count: 0,
                            },
                        );
                    }
                }
            }
            it.next();
        }
        Ok(())
    }

    pub fn recover_block_state(&self) -> Result<Option<BlockState>> {
        self.get_entry::<BlockState>(BLOCK_STATE_CF, BLOCK_STATE_KEY.as_ref())
    }
    fn store_block_state(&self, state: BlockState) -> Result<()> {
        self.put_entry(BLOCK_STATE_CF, BLOCK_STATE_KEY.as_ref(), state)
    }

    fn recover_from_state(&self, address: &DB3Address) -> Result<Option<DatabaseStatePersistence>> {
        self.get_entry::<DatabaseStatePersistence>(STATE_CF, address.as_ref())
    }

    fn recover_db_doc_id(&self, address: &DB3Address) -> Result<Option<i64>> {
        let cf_handle = self
            .se
            .cf_handle(self.config.doc_owner_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let prefix = DbDocKeyV2::build_prefix(address);
        let mut it: DBRawIterator = self.se.prefix_iterator_cf(&cf_handle, &prefix).into();
        let mut doc_id = 0;
        while it.valid() {
            if let Some(key) = it.key() {
                let key_ref: &[u8] = key.as_ref();
                if !DbDocKeyV2::is_the_same_db(key_ref, address) {
                    break;
                }
                doc_id = DbDocKeyV2::decode_id(key.as_ref())?;
            }
            it.next();
        }
        Ok(Some(doc_id))
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
        doc_count: usize,
        doc_ids: Option<&Vec<i64>>,
    ) -> Result<Option<Vec<i64>>> {
        if let Some(mut write_guard) = self.db_state.get_mut(db_addr_hex) {
            let database_state = write_guard.deref_mut();
            let mut ids: Vec<i64> = Vec::new();

            if let Some(temp_ids) = doc_ids {
                if temp_ids.len() != doc_count {
                    return Err(DB3Error::WriteStoreError(format!(
                        "doc_ids and docs length mismatch {} != {}",
                        temp_ids.len(),
                        doc_count
                    )));
                }
                ids = temp_ids.clone();
                database_state.doc_order = ids
                    .iter()
                    .max()
                    .unwrap_or(&(database_state.doc_order + doc_count as i64))
                    .clone();
            } else {
                for id in 1..(doc_count + 1) {
                    ids.push(database_state.doc_order + id as i64);
                }
                database_state.doc_order = database_state.doc_order + doc_count as i64;
            }
            database_state.total_doc_count = database_state.total_doc_count + doc_count as u64;
            if let Some(collection_state) = database_state.collection_state.get_mut(col) {
                collection_state.total_doc_count =
                    collection_state.total_doc_count + doc_count as u64;
            }
            Ok(Some(ids))
        } else {
            Ok(None)
        }
    }

    pub fn get_collection_of_database(
        &self,
        db_addr: &DB3Address,
    ) -> Result<(Vec<Collection>, Vec<CollectionStateProto>)> {
        let collections = self.get_entries_with_prefix::<Collection>(
            db_addr.as_ref(),
            self.config.collection_store_cf_name.as_str(),
        )?;
        let mut collection_states: Vec<CollectionStateProto> = Vec::new();
        for col in collections.iter() {
            if let Some(state) = self.get_collection_state(db_addr, col.name.as_str()) {
                collection_states.push(state);
            } else {
                return Err(DB3Error::CollectionNotFound(
                    db_addr.to_hex(),
                    col.name.to_string(),
                ));
            }
        }
        Ok((collections, collection_states))
    }

    pub fn get_database_of_owner(
        &self,
        owner: &DB3Address,
    ) -> Result<(Vec<DatabaseMessage>, Vec<DatabaseStateProto>)> {
        let (_keys, databases, states) = self.get_database_of_owner_internal(owner)?;
        Ok((databases, states))
    }

    fn get_database_of_owner_internal(
        &self,
        owner: &DB3Address,
    ) -> Result<(Vec<Vec<u8>>, Vec<DatabaseMessage>, Vec<DatabaseStateProto>)> {
        let cf_handle = self
            .se
            .cf_handle(self.config.db_owner_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it: DBRawIterator = self.se.prefix_iterator_cf(&cf_handle, owner).into();
        let mut entries: Vec<DatabaseMessage> = Vec::new();
        let mut database_state: Vec<DatabaseStateProto> = Vec::new();
        let mut keys: Vec<Vec<u8>> = Vec::new();
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
                    if let Some(state) = self.get_database_state(&addr) {
                        entries.push(d);
                        database_state.push(state);
                        if let Some(key_ref) = it.key() {
                            keys.push(key_ref.to_vec());
                        }
                    }
                }
            }
            it.next();
        }
        Ok((keys, entries, database_state))
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

    fn put_entry<T>(&self, cf: &str, ck_ref: &[u8], value: T) -> Result<()>
    where
        T: Message + std::default::Default,
    {
        let cf_handle = self
            .se
            .cf_handle(cf)
            .ok_or(DB3Error::WriteStoreError("cf is not found".to_string()))?;
        let mut buf = BytesMut::with_capacity(1024);
        value
            .encode(&mut buf)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        let buf = buf.freeze();
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_handle, ck_ref, buf.as_ref());
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        Ok(())
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
        let ck = collection_key::build_collection_key(db_addr, name)?;
        let ck_ref: &[u8] = ck.as_ref();
        self.get_entry::<Collection>(self.config.collection_store_cf_name.as_str(), ck_ref)
    }

    fn save_collection_internal(
        &self,
        sender: &DB3Address,
        db_addr: &DB3Address,
        name: &str,
        indexes: &Vec<Index>,
    ) -> Result<()> {
        let db = self.get_database(db_addr)?;
        if db.is_none() {
            return Err(DB3Error::ReadStoreError(
                "fail to find database".to_string(),
            ));
        }
        let ck = collection_key::build_collection_key(db_addr, name)?;
        let collection_store_cf_handle = self
            .se
            .cf_handle(self.config.collection_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let ck_ref: &[u8] = ck.as_ref();
        let col = Collection {
            name: name.to_string(),
            index_fields: indexes.to_vec(),
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
        self.update_db_state_for_new_collection(db_addr_hex.as_str(), name);
        if self.config.enable_doc_store {
            self.doc_store
                .add_index(db_addr, name, indexes)
                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        }
        Ok(())
    }

    pub fn create_collection(
        &self,
        sender: &DB3Address,
        db_addr: &DB3Address,
        collection: &CollectionMutation,
        _block: u64,
        _order: u32,
        _idx: u16,
    ) -> Result<()> {
        if self.is_db_collection_exist(db_addr, collection.collection_name.as_str())? {
            return Err(DB3Error::CollectionAlreadyExist(
                collection.collection_name.to_string(),
                db_addr.to_hex(),
            ));
        }
        self.save_collection_internal(
            sender,
            db_addr,
            collection.collection_name.as_str(),
            &collection.index_fields,
        )
    }

    pub fn get_database(&self, db_addr: &DB3Address) -> Result<Option<DatabaseMessage>> {
        self.get_entry::<DatabaseMessage>(self.config.db_store_cf_name.as_str(), db_addr.as_ref())
    }

    pub fn get_collection_state(
        &self,
        db_addr: &DB3Address,
        col: &str,
    ) -> Option<CollectionStateProto> {
        let db_addr_hex = db_addr.to_hex();
        if let Some(guard) = self.db_state.get(db_addr_hex.as_str()) {
            let database_state = guard.deref();
            if let Some(col_state) = database_state.collection_state.get(col) {
                return Some(CollectionStateProto {
                    total_doc_count: col_state.total_doc_count,
                });
            }
        }
        None
    }

    fn build_persistence_state(&self, db_addr: &DB3Address) -> Option<DatabaseStatePersistence> {
        let db_addr_hex = db_addr.to_hex();
        if let Some(guard) = self.db_state.get(db_addr_hex.as_str()) {
            let database_state = guard.deref();
            let collection_states: HashMap<String, CollectionStateProto> = database_state
                .collection_state
                .iter()
                .map(|(key, value)| {
                    (
                        key.to_string(),
                        CollectionStateProto {
                            total_doc_count: value.total_doc_count,
                        },
                    )
                })
                .collect();
            Some(DatabaseStatePersistence {
                addr: db_addr_hex,
                total_doc_count: database_state.total_doc_count,
                total_col_count: database_state.collection_state.len() as u64,
                collection_states,
                doc_order: database_state.doc_order,
            })
        } else {
            None
        }
    }

    pub fn get_database_state(&self, db_addr: &DB3Address) -> Option<DatabaseStateProto> {
        let db_addr_hex = db_addr.to_hex();
        if let Some(guard) = self.db_state.get(db_addr_hex.as_str()) {
            let database_state = guard.deref();
            Some(DatabaseStateProto {
                total_doc_count: database_state.total_doc_count,
                total_col_count: database_state.collection_state.len() as u64,
                doc_order: database_state.doc_order,
            })
        } else {
            None
        }
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
    ) -> Result<(Vec<Document>, u64)> {
        if !self.is_db_collection_exist(db_addr, col_name)? {
            return Err(DB3Error::CollectionNotFound(
                col_name.to_string(),
                db_addr.to_hex(),
            ));
        }
        if self.config.enable_doc_store {
            let (result, count) = self.doc_store.execute_query(db_addr, col_name, query)?;
            let mut documents = vec![];
            for (id, doc) in result {
                documents.push(Document { id, doc })
            }
            Ok((documents, count))
        } else {
            Ok((vec![], 0))
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
        given_doc_ids: Option<&Vec<i64>>,
    ) -> Result<Vec<i64>> {
        if !self.is_db_collection_exist(db_addr, col_name)? {
            return Err(DB3Error::CollectionNotFound(
                col_name.to_string(),
                db_addr.to_hex(),
            ));
        }
        let db_addr_hex = db_addr.to_hex();
        let doc_ids = self.update_db_state_for_add_docs(
            db_addr_hex.as_str(),
            col_name,
            docs.len(),
            given_doc_ids,
        )?;
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
        let ck = collection_key::build_collection_key(db_addr, col_name)?;
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

    pub fn get_doc(
        &self,
        db_addr: &DB3Address,
        col_name: &str,
        doc_id: i64,
    ) -> Result<Option<Document>> {
        if !self.is_db_collection_exist(db_addr, col_name)? {
            return Err(DB3Error::CollectionNotFound(
                col_name.to_string(),
                db_addr.to_hex(),
            ));
        }
        if self.config.enable_doc_store {
            let doc = self.doc_store.get_doc(db_addr, col_name, doc_id)?;
            if let Some(d) = doc {
                Ok(Some(Document { id: doc_id, doc: d }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
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
            start_block: mutation.start_block,
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

    pub fn delete_event_db(&self, sender: &DB3Address, db_addr: &DB3Address) -> Result<()> {
        match self.get_event_db(db_addr)? {
            Some(database) => {
                let sender_ref: &[u8] = database.sender.as_ref();
                if sender_ref != sender.as_ref() {
                    return Err(DB3Error::DatabasePermissionDenied());
                }
                self.delete_event_db_internal(sender, db_addr)?;
            }
            None => return Err(DB3Error::DatabaseNotFound(db_addr.to_hex())),
        }
        Ok(())
    }

    // make sure the db is event db
    fn delete_event_db_internal(&self, owner: &DB3Address, db_addr: &DB3Address) -> Result<()> {
        let cf_handle = self
            .se
            .cf_handle(self.config.db_owner_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it: DBRawIterator = self.se.prefix_iterator_cf(&cf_handle, owner).into();
        let mut batch = WriteBatch::default();
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
                if addr.as_ref() == db_addr.as_ref() {
                    if let Some(key_ref) = it.key() {
                        batch.delete_cf(&cf_handle, key_ref);
                    }
                    break;
                }
            }
            it.next();
        }
        let db_store_cf_handle = self
            .se
            .cf_handle(self.config.db_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        batch.delete_cf(&db_store_cf_handle, db_addr);
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        Ok(())
    }

    pub fn create_predefined_doc_database(
        &self,
        sender: &DB3Address,
        db_addr: &DbId,
        desc: &str,
        block: u64,
        order: u32,
    ) -> Result<()> {
        if let Ok(Some(_)) = self.get_database(db_addr.address()) {
            return Err(DB3Error::DatabaseAlreadyExist(db_addr.to_hex()));
        }
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
            desc: desc.to_string(),
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
        Ok(())
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
        self.create_predefined_doc_database(
            sender,
            &db_addr,
            mutation.db_desc.as_str(),
            block,
            order,
        )?;
        Ok(db_addr)
    }

    fn add_index<'a>(
        &self,
        db_addr: &DB3Address,
        col: &str,
        indexes: &Vec<Index>,
        sender: &DB3Address,
    ) -> Result<()> {
        let db = self.get_database(db_addr)?;
        if db.is_none() {
            return Err(DB3Error::DatabaseNotFound(db_addr.to_hex()));
        }
        match self.get_collection(db_addr, col)? {
            Some(collection) => {
                let collection_sender: &[u8] = collection.sender.as_ref();
                if collection_sender != sender.as_ref() {
                    return Err(DB3Error::CollectionPermissionDenied());
                }
                let path_set: HashSet<&String> =
                    collection.index_fields.iter().map(|x| &x.path).collect();
                let exist_paths: Vec<&String> = indexes
                    .iter()
                    .filter(|x| path_set.contains(&x.path))
                    .map(|x| &x.path)
                    .collect();
                if exist_paths.len() > 0 {
                    return Err(DB3Error::InvalidKeyPathError(format!(
                        "the index paths {:?} exist",
                        exist_paths
                    )));
                }
                let new_indexes = [collection.index_fields, indexes.clone()].concat();
                self.save_collection_internal(sender, db_addr, col, &new_indexes)?;
                Ok(())
            }
            None => Err(DB3Error::CollectionNotFound(
                col.to_string(),
                db_addr.to_hex(),
            )),
        }
    }

    pub fn apply_mutation(
        &self,
        action: MutationAction,
        dm: Mutation,
        address: &DB3Address,
        network: u64,
        nonce: u64,
        block: u64,
        order: u32,
        doc_ids_map: &HashMap<String, Vec<i64>>,
    ) -> Result<Vec<ExtraItem>> {
        let mut items: Vec<ExtraItem> = Vec::new();
        match action {
            MutationAction::DeleteEventDb => {
                for body in dm.bodies {
                    if let Some(Body::DeleteEventDatabaseMutation(ref _del_mutation)) = &body.body {
                        let db_address_ref: &[u8] = body.db_address.as_ref();
                        let db_addr = DB3Address::try_from(db_address_ref)?;
                        self.delete_event_db(address, &db_addr)?;
                        let item = ExtraItem {
                            key: "db_addr".to_string(),
                            value: db_addr.to_hex(),
                        };
                        items.push(item)
                    }
                    break;
                }
            }

            MutationAction::AddIndex => {
                for body in dm.bodies {
                    if let Some(Body::AddIndexMutation(ref add_index_mutation)) = &body.body {
                        let db_address_ref: &[u8] = body.db_address.as_ref();
                        let db_addr = DB3Address::try_from(db_address_ref)?;
                        self.add_index(
                            &db_addr,
                            add_index_mutation.collection_name.as_str(),
                            &add_index_mutation.index_fields,
                            address,
                        )?;
                        let item = ExtraItem {
                            key: "collection".to_string(),
                            value: add_index_mutation.collection_name.to_string(),
                        };
                        items.push(item);
                        info!(
                            "add index to collection {} done",
                            add_index_mutation.collection_name.as_str()
                        );
                        break;
                    }
                }
            }

            MutationAction::MintCollection => {
                for body in dm.bodies {
                    if let Some(Body::MintCollectionMutation(ref mint_col_mutation)) = &body.body {
                        let sender = DB3Address::try_from(mint_col_mutation.sender.as_str())?;
                        let db_addr = DB3Address::try_from(mint_col_mutation.db_addr.as_str())?;
                        if self.is_db_collection_exist(&db_addr, mint_col_mutation.name.as_str())? {
                            return Err(DB3Error::CollectionAlreadyExist(
                                mint_col_mutation.name.to_string(),
                                db_addr.to_hex(),
                            ));
                        }
                        self.save_collection_internal(
                            &sender,
                            &db_addr,
                            mint_col_mutation.name.as_str(),
                            &vec![],
                        )?;
                        info!(
                            "add collection with db_addr {}, collection_name: {}, from owner {}",
                            db_addr.to_hex().as_str(),
                            mint_col_mutation.name.as_str(),
                            sender.to_hex().as_str()
                        );
                        let item = ExtraItem {
                            key: "collection".to_string(),
                            value: mint_col_mutation.name.to_string(),
                        };
                        items.push(item);
                        break;
                    }
                }
            }
            MutationAction::MintDocumentDb => {
                for body in dm.bodies {
                    if let Some(Body::MintDocDatabaseMutation(ref min_doc_db_mutation)) = &body.body
                    {
                        let sender = DB3Address::try_from(min_doc_db_mutation.sender.as_str())?;
                        let db_id =
                            DbId::from(DB3Address::try_from(min_doc_db_mutation.db_addr.as_str())?);
                        self.create_predefined_doc_database(
                            &sender,
                            &db_id,
                            min_doc_db_mutation.desc.as_str(),
                            block,
                            order,
                        )
                        .map_err(|e| {
                            DB3Error::ApplyMutationError(format!(
                                "fail to create predefined database {e}"
                            ))
                        })?;
                        let db_id_hex = db_id.to_hex();
                        info!(
                            "mint database with addr {} from owner {}",
                            db_id_hex.as_str(),
                            address.to_hex().as_str()
                        );
                        let item = ExtraItem {
                            key: "db_addr".to_string(),
                            value: db_id_hex,
                        };
                        items.push(item);
                        break;
                    }
                }
            }
            MutationAction::CreateEventDb => {
                for body in dm.bodies {
                    if let Some(Body::EventDatabaseMutation(ref mutation)) = &body.body {
                        let db_id = self
                            .create_event_database(address, mutation, nonce, network, block, order)
                            .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                        let db_id_hex = db_id.to_hex();
                        info!(
                            "add database with addr {} from owner {}",
                            db_id_hex.as_str(),
                            address.to_hex().as_str()
                        );
                        let item = ExtraItem {
                            key: "db_addr".to_string(),
                            value: db_id_hex,
                        };
                        items.push(item);
                        break;
                    }
                }
            }
            MutationAction::CreateDocumentDb => {
                for body in dm.bodies {
                    if let Some(Body::DocDatabaseMutation(ref doc_db_mutation)) = &body.body {
                        let db_id = self
                            .create_doc_database(
                                address,
                                doc_db_mutation,
                                nonce,
                                network,
                                block,
                                order,
                            )
                            .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                        let db_id_hex = db_id.to_hex();
                        info!(
                            "add database with addr {} from owner {}",
                            db_id_hex.as_str(),
                            address.to_hex().as_str()
                        );
                        let item = ExtraItem {
                            key: "db_addr".to_string(),
                            value: db_id_hex,
                        };
                        items.push(item);
                        break;
                    }
                }
            }
            MutationAction::AddCollection => {
                for (i, body) in dm.bodies.iter().enumerate() {
                    let db_address_ref: &[u8] = body.db_address.as_ref();
                    let db_addr = DB3Address::try_from(db_address_ref)
                        .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                    if let Some(Body::CollectionMutation(ref col_mutation)) = &body.body {
                        self.create_collection(
                            address,
                            &db_addr,
                            col_mutation,
                            block,
                            order,
                            i as u16,
                        )
                        .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                        info!(
                            "add collection with db_addr {}, collection_name: {}, from owner {}",
                            db_addr.to_hex().as_str(),
                            col_mutation.collection_name.as_str(),
                            address.to_hex().as_str()
                        );
                        let item = ExtraItem {
                            key: "collection".to_string(),
                            value: col_mutation.collection_name.to_string(),
                        };
                        items.push(item);
                    }
                }
            }
            MutationAction::AddDocument => {
                for (i, body) in dm.bodies.iter().enumerate() {
                    let db_address_ref: &[u8] = body.db_address.as_ref();
                    let db_addr = DB3Address::try_from(db_address_ref)
                        .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                    if let Some(Body::DocumentMutation(ref doc_mutation)) = &body.body {
                        let mut docs = Vec::<String>::new();
                        for buf in doc_mutation.documents.iter() {
                            let document = bytes_to_bson_document(buf.clone())
                                .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                            docs.push(document.to_string());
                        }
                        let ids = self
                            .add_docs(
                                &db_addr,
                                address,
                                doc_mutation.collection_name.as_str(),
                                &docs,
                                doc_ids_map.get(i.to_string().as_str()),
                            )
                            .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                        debug!(
                                    "add documents with db_addr {}, collection_name: {}, from owner {}, document size: {}",
                                    db_addr.to_hex().as_str(),
                                    doc_mutation.collection_name.as_str(),
                                    address.to_hex().as_str(),
                                    ids.len()
                                );
                        // return document keys
                        for id in ids {
                            let item = ExtraItem {
                                key: "document".to_string(),
                                value: id.to_string(),
                            };
                            items.push(item);
                        }
                    }
                }
            }
            MutationAction::UpdateDocument => {
                for (_i, body) in dm.bodies.iter().enumerate() {
                    let db_address_ref: &[u8] = body.db_address.as_ref();
                    let db_addr = DB3Address::try_from(db_address_ref)
                        .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                    if let Some(Body::DocumentMutation(ref doc_mutation)) = &body.body {
                        if doc_mutation.documents.len() != doc_mutation.ids.len() {
                            let msg = format!(
                                "doc ids size {} not equal to documents size {}",
                                doc_mutation.ids.len(),
                                doc_mutation.documents.len()
                            );
                            warn!("{}", msg.as_str());
                            return Err(DB3Error::ApplyMutationError(msg));
                        }
                        let mut docs = Vec::<String>::new();
                        for buf in doc_mutation.documents.iter() {
                            let document = bytes_to_bson_document(buf.clone())
                                .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                            let doc_str = document.to_string();
                            debug!("update document: {}", doc_str);
                            docs.push(doc_str);
                        }
                        self.update_docs(
                            &db_addr,
                            address,
                            doc_mutation.collection_name.as_str(),
                            &docs,
                            &doc_mutation.ids,
                        )
                        .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                        info!(
                            "update documents with db_addr {}, collection_name: {}, from owner {}",
                            db_addr.to_hex().as_str(),
                            doc_mutation.collection_name.as_str(),
                            address.to_hex().as_str()
                        );
                    }
                }
            }
            MutationAction::DeleteDocument => {
                for (_i, body) in dm.bodies.iter().enumerate() {
                    let db_address_ref: &[u8] = body.db_address.as_ref();
                    let db_addr = DB3Address::try_from(db_address_ref)
                        .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                    if let Some(Body::DocumentMutation(ref doc_mutation)) = &body.body {
                        self.delete_docs(
                            &db_addr,
                            address,
                            doc_mutation.collection_name.as_str(),
                            &doc_mutation.ids,
                        )
                        .map_err(|e| DB3Error::ApplyMutationError(format!("{e}")))?;
                        info!(
                            "delete documents with db_addr {}, collection_name: {}, from owner {}",
                            db_addr.to_hex().as_str(),
                            doc_mutation.collection_name.as_str(),
                            address.to_hex().as_str()
                        );
                    }
                }
            }
        };
        self.store_block_state(BlockState { block, order })?;
        Ok(items)
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
            doc_start_id: 1000,
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
            doc_start_id: 1000,
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
        let col_state = db3_store.get_collection_state(&db_id.address(), "col1");
        assert!(col_state.is_none());
        let result =
            db3_store.create_collection(&DB3Address::ZERO, db_id.address(), &collection, 1, 1, 1);
        assert!(result.is_ok());
        let col_state = db3_store.get_collection_state(&db_id.address(), "col1");
        assert!(col_state.is_some());
        let result = db3_store.get_collection(db_id.address(), "col1");
        if let Ok(Some(_c)) = result {
            assert!(true);
        } else {
            assert!(false);
        }
        if let Ok((cl, states)) = db3_store.get_collection_of_database(db_id.address()) {
            assert_eq!(cl.len(), 1);
            assert_eq!(states.len(), 1);
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
            doc_start_id: 1000,
        };
        let result = DBStoreV2::new(config);
        assert_eq!(result.is_ok(), true);
        let db_m = DocumentDatabaseMutation {
            db_desc: "test_desc".to_string(),
        };
        let db3_store = result.unwrap();
        let db_state = db3_store.get_database_state(&DB3Address::ZERO);
        assert!(db_state.is_none());

        let result = db3_store.create_doc_database(&DB3Address::ZERO, &db_m, 1, 1, 1, 1);
        assert!(result.is_ok());
        let db_id = result.unwrap();
        let db_state = db3_store.get_database_state(&db_id.address());
        assert!(db_state.is_some());
        if let Ok(Some(db)) = db3_store.get_database(db_id.address()) {
            if let Some(database_message::Database::DocDb(doc_db)) = db.database {
                assert_eq!("test_desc", doc_db.desc.as_str());
            }
        } else {
            assert!(false);
        }

        if let Ok((dbs, states)) = db3_store.get_database_of_owner(&DB3Address::ZERO) {
            assert_eq!(dbs.len(), 1);
            assert_eq!(states.len(), 1);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn event_db_smoke_test() {
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
            doc_start_id: 1000,
        };
        let result = DBStoreV2::new(config);
        assert_eq!(result.is_ok(), true);
        let emutation = EventDatabaseMutation {
            contract_address: "".to_string(),
            ttl: 0,
            desc: "desc".to_string(),
            tables: vec![],
            events_json_abi: "".to_string(),
            evm_node_url: "".to_string(),
            start_block: 0,
        };
        let db3_store = result.unwrap();
        let result = db3_store.create_event_database(&DB3Address::ZERO, &emutation, 1, 1, 1, 1);
        assert_eq!(result.is_ok(), true);
        let db_id = result.unwrap();
        if let Ok(Some(_d)) = db3_store.get_event_db(db_id.address()) {
        } else {
            assert!(false);
        }
        let result = db3_store.delete_event_db(&DB3Address::ZERO, db_id.address());
        assert_eq!(result.is_ok(), true);
        if let Ok(Some(_d)) = db3_store.get_event_db(db_id.address()) {
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
            doc_start_id: 1000,
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
        let result = db3_store
            .update_db_state_for_add_docs(&db_id_1.address().to_hex(), "col1", 3, None)
            .unwrap();
        assert_eq!(result, Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_update_db_state_for_add_docs_with_given_doc_ids() {
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
            doc_start_id: 1000,
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
        let doc_ids: Vec<i64> = vec![1, 2, 3];
        let result = db3_store
            .update_db_state_for_add_docs(&db_id_1.address().to_hex(), "col1", 3, Some(&doc_ids))
            .unwrap();
        assert_eq!(result, Some(vec![1, 2, 3]));
    }

    #[test]
    fn recover_and_store_block_state_ut() {
        let tmp_dir_path = TempDir::new("recover_block_state_ut").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        {
            let config = DBStoreV2Config {
                db_path: real_path.to_string(),
                db_store_cf_name: "db".to_string(),
                doc_store_cf_name: "doc".to_string(),
                collection_store_cf_name: "cf2".to_string(),
                index_store_cf_name: "index".to_string(),
                doc_owner_store_cf_name: "doc_owner".to_string(),
                db_owner_store_cf_name: "db_owner".to_string(),
                scan_max_limit: 50,
                enable_doc_store: false,
                doc_store_conf: DocStoreConfig::default(),
                doc_start_id: 1000,
            };
            let result = DBStoreV2::new(config);
            assert_eq!(result.is_ok(), true);
            let db3_store = result.unwrap();

            // recover from empty block state
            let block_state_none = db3_store.recover_block_state().unwrap();
            assert!(block_state_none.is_none());

            // store block state
            let res = db3_store.store_block_state(BlockState { block: 1, order: 2 });
            assert!(res.is_ok());

            // recover block state
            let block_state = db3_store.recover_block_state().unwrap();
            assert_eq!(block_state, Some(BlockState { block: 1, order: 2 }));
        }
    }
    #[test]
    fn test_recover_db_state_with_persistence() {
        let tmp_dir_path = TempDir::new("new_database").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let mut address: Vec<DB3Address> = Vec::new();

        {
            let config = DBStoreV2Config {
                db_path: real_path.to_string(),
                db_store_cf_name: "db".to_string(),
                doc_store_cf_name: "doc".to_string(),
                collection_store_cf_name: "cf2".to_string(),
                index_store_cf_name: "index".to_string(),
                doc_owner_store_cf_name: "doc_owner".to_string(),
                db_owner_store_cf_name: "db_owner".to_string(),
                scan_max_limit: 50,
                enable_doc_store: false,
                doc_store_conf: DocStoreConfig::default(),
                doc_start_id: 1000,
            };
            let result = DBStoreV2::new(config);
            assert_eq!(result.is_ok(), true);
            let db_m = DocumentDatabaseMutation {
                db_desc: "test_desc".to_string(),
            };
            let db3_store = result.unwrap();
            let result = db3_store.create_doc_database(&DB3Address::ZERO, &db_m, 1, 1, 1, 1);
            assert_eq!(result.is_ok(), true);
            let db_id = result.unwrap();
            let result = db3_store.create_doc_database(&DB3Address::ZERO, &db_m, 2, 2, 2, 2);
            assert_eq!(result.is_ok(), true);
            let db_id2 = result.unwrap();

            let collection = CollectionMutation {
                index_fields: vec![],
                collection_name: "col1".to_string(),
            };

            let result = db3_store.create_collection(
                &DB3Address::ZERO,
                db_id.address(),
                &collection,
                1,
                1,
                1,
            );
            assert!(result.is_ok());
            let result = db3_store.create_collection(
                &DB3Address::ZERO,
                db_id2.address(),
                &collection,
                1,
                1,
                1,
            );
            assert!(result.is_ok());
            let docs = vec!["{\"test\":0}".to_string()];
            address.push(db_id.address().clone());
            for _n in 0..1003 {
                db3_store
                    .add_docs(db_id.address(), &DB3Address::ZERO, "col1", &docs, None)
                    .unwrap();
            }
            for _n in 0..91 {
                db3_store
                    .add_docs(db_id2.address(), &DB3Address::ZERO, "col1", &docs, None)
                    .unwrap();
            }
            let result = db3_store.flush_database_state();
            assert_eq!(result.is_ok(), true);
        }

        {
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
                doc_start_id: 1000,
            };
            let result = DBStoreV2::new(config);
            let db3_store = result.unwrap();
            let result = db3_store.recover_db_state();
            println!("{:?}", result);
            assert_eq!(result.is_ok(), true);
            let database_state_ret = db3_store.get_database_state(&address[0]);
            println!("{:?}", database_state_ret);
            let database_state = database_state_ret.unwrap();
            assert_eq!(database_state.doc_order, 1004);
        }
    }

    #[test]
    fn test_recover_db_state() {
        let tmp_dir_path = TempDir::new("new_database").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let mut address: Vec<DB3Address> = Vec::new();

        {
            let config = DBStoreV2Config {
                db_path: real_path.to_string(),
                db_store_cf_name: "db".to_string(),
                doc_store_cf_name: "doc".to_string(),
                collection_store_cf_name: "cf2".to_string(),
                index_store_cf_name: "index".to_string(),
                doc_owner_store_cf_name: "doc_owner".to_string(),
                db_owner_store_cf_name: "db_owner".to_string(),
                scan_max_limit: 50,
                enable_doc_store: false,
                doc_store_conf: DocStoreConfig::default(),
                doc_start_id: 1000,
            };
            let result = DBStoreV2::new(config);
            assert_eq!(result.is_ok(), true);
            let db_m = DocumentDatabaseMutation {
                db_desc: "test_desc".to_string(),
            };
            let db3_store = result.unwrap();
            let result = db3_store.create_doc_database(&DB3Address::ZERO, &db_m, 1, 1, 1, 1);
            assert_eq!(result.is_ok(), true);
            let db_id = result.unwrap();
            let result = db3_store.create_doc_database(&DB3Address::ZERO, &db_m, 2, 2, 2, 2);
            assert_eq!(result.is_ok(), true);
            let db_id2 = result.unwrap();

            let collection = CollectionMutation {
                index_fields: vec![],
                collection_name: "col1".to_string(),
            };

            let result = db3_store.create_collection(
                &DB3Address::ZERO,
                db_id.address(),
                &collection,
                1,
                1,
                1,
            );
            assert!(result.is_ok());
            let result = db3_store.create_collection(
                &DB3Address::ZERO,
                db_id2.address(),
                &collection,
                1,
                1,
                1,
            );
            assert!(result.is_ok());
            let docs = vec!["{\"test\":0}".to_string()];
            address.push(db_id.address().clone());
            for _n in 0..1003 {
                db3_store
                    .add_docs(db_id.address(), &DB3Address::ZERO, "col1", &docs, None)
                    .unwrap();
            }
            for _n in 0..91 {
                db3_store
                    .add_docs(db_id2.address(), &DB3Address::ZERO, "col1", &docs, None)
                    .unwrap();
            }
        }

        {
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
                doc_start_id: 1000,
            };
            let result = DBStoreV2::new(config);
            let db3_store = result.unwrap();
            let result = db3_store.recover_db_state();
            println!("{:?}", result);
            assert_eq!(result.is_ok(), true);
            let database_state_ret = db3_store.get_database_state(&address[0]);
            println!("{:?}", database_state_ret);
            let database_state = database_state_ret.unwrap();
            assert_eq!(database_state.doc_order, 1004);
        }
    }
}
