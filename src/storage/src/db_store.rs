//
// ns_store.rs
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
use super::db_key::DbKey;
use super::db_owner_key::DbOwnerKey;
use crate::db3_document::DB3Document;
use bytes::BytesMut;
use db3_base::bson_util;
use db3_crypto::{
    db3_address::DB3Address, id::CollectionId, id::DbId, id::DocumentEntryId, id::DocumentId,
    id::FieldKey, id::IndexId, id::TxId,
};
use db3_error::{DB3Error, Result};
use db3_proto::db3_database_proto::index::IndexField;
use db3_proto::db3_database_proto::structured_query::composite_filter::Operator as CompositeOp;
use db3_proto::db3_database_proto::structured_query::field_filter::Operator;
use db3_proto::db3_database_proto::structured_query::filter::FilterType;
use db3_proto::db3_database_proto::{Collection, Database, Document, Index, StructuredQuery};
use db3_proto::db3_mutation_proto::{DatabaseAction, DatabaseMutation};
use db3_types::cost::DbStoreOp;
use itertools::Itertools;
use merkdb::{tree::Tree, BatchEntry, Merk, Op};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::pin::Pin;
use tracing::{debug, info, span, warn, Level};

pub struct DbStore {}

impl DbStore {
    pub fn new() -> Self {
        Self {}
    }

    fn update_database(
        old_db: &Database,
        mutation: &DatabaseMutation,
        tx_id: &TxId,
        block_id: u64,
        mutation_id: u16,
    ) -> Result<(Database, DbStoreOp)> {
        let mut new_db = old_db.clone();
        let mut name_set: HashSet<&str> = HashSet::new();
        for collection in old_db.collections.iter() {
            name_set.insert(collection.name.as_str());
        }
        let mut idx: u16 = 0;
        let mut collection_ops: u64 = 0;
        let mut index_ops: u64 = 0;
        for new_collection in mutation.collection_mutations.iter() {
            if name_set.contains(new_collection.collection_name.as_str()) {
                return Err(DB3Error::ApplyDatabaseError(format!(
                    "duplicated collection names {}",
                    new_collection.collection_name
                )));
            } else {
                idx += 1;
                collection_ops += 1;
                index_ops += new_collection.index.len() as u64;
                new_db.collections.push(Collection {
                    id: CollectionId::create(block_id, mutation_id, idx)
                        .map_err(|e| {
                            DB3Error::ApplyDatabaseError(format!(
                                "fail to generate collection id {block_id}, {mutation_id}, {idx} with err {e}"
                            ))
                        })?
                        .as_ref()
                        .to_vec(),
                    name: new_collection.collection_name.to_string(),
                    index_list: new_collection.index.to_vec(),
                });
            }
        }
        new_db.tx.push(tx_id.as_ref().to_vec());
        Ok((
            new_db,
            DbStoreOp::DbOp {
                create_db_ops: 0,
                create_collection_ops: collection_ops,
                create_index_ops: index_ops,
                data_in_bytes: 0,
            },
        ))
    }

    fn new_database(
        id: &DbId,
        sender: &DB3Address,
        txid: &TxId,
        mutation: &DatabaseMutation,
        block_id: u64,
        mutation_id: u16,
        desc: &str,
    ) -> (Database, DbStoreOp) {
        //TODO check the duplicated collection id
        let mut idx: u16 = 0;
        let mut collection_count: u64 = 0;
        let mut index_count: u64 = 0;
        let mut name_set: HashSet<&str> = HashSet::new();
        let collections: Vec<Collection> = mutation
            .collection_mutations
            .iter()
            .filter(|x| {
                if name_set.contains(x.collection_name.as_str()) {
                    return false;
                } else {
                    name_set.insert(x.collection_name.as_str());
                    return true;
                }
            })
            .map(move |x| {
                idx += 1;
                collection_count = collection_count + 1;
                index_count = index_count + x.index.len() as u64;
                Collection {
                    id: CollectionId::create(block_id, mutation_id, idx)
                        .unwrap()
                        .as_ref()
                        .to_vec(),
                    name: x.collection_name.to_string(),
                    index_list: x.index.to_vec(),
                }
            })
            .collect();
        (
            Database {
                address: id.as_ref().to_vec(),
                sender: sender.as_ref().to_vec(),
                tx: vec![txid.as_ref().to_vec()],
                collections,
                desc: desc.to_string(),
            },
            DbStoreOp::DbOp {
                create_db_ops: 1,
                create_collection_ops: collection_count,
                create_index_ops: index_count,
                data_in_bytes: 0,
            },
        )
    }

    fn convert(
        sender: &DB3Address,
        nonce: u64,
        tx: &TxId,
        mutation: &DatabaseMutation,
        block_id: u64,
        mutation_id: u16,
        desc: &str,
    ) -> Result<(Vec<BatchEntry>, DbStoreOp)> {
        let dbid = DbId::try_from((sender, nonce))?;
        info!(
            "create a database with id {} and sender {}",
            dbid.to_hex(),
            sender.to_hex()
        );
        let (db, mut ops) =
            Self::new_database(&dbid, sender, tx, mutation, block_id, mutation_id, desc);
        let (batches, data_in_bytes) =
            Self::encode_database(dbid, &db, sender, block_id, mutation_id, true)?;
        ops.update_data_size(data_in_bytes as u64);
        Ok((batches, ops))
    }

    fn encode_database(
        dbid: DbId,
        database: &Database,
        sender: &DB3Address,
        height: u64,
        mutation_id: u16,
        add_owner: bool,
    ) -> Result<(Vec<BatchEntry>, usize)> {
        let key = DbKey(dbid);
        let encoded_key = key.encode()?;
        let mut buf = BytesMut::with_capacity(1024 * 2);
        database
            .encode(&mut buf)
            .map_err(|e| DB3Error::ApplyDatabaseError(format!("{e}")))?;
        let buf = buf.freeze();
        if add_owner {
            let db_owner = DbOwnerKey(sender, height, mutation_id);
            let db_owner_encoded_key = db_owner.encode()?;
            let total_in_bytes = encoded_key.len()
                + buf.as_ref().len()
                + db_owner_encoded_key.len()
                + DbId::length();
            let batches = vec![
                (encoded_key, Op::Put(buf.as_ref().to_vec())),
                (db_owner_encoded_key, Op::Put(dbid.as_ref().to_vec())),
            ];
            Ok((batches, total_in_bytes))
        } else {
            let total_in_bytes = encoded_key.len() + buf.as_ref().len();
            let batches = vec![(encoded_key, Op::Put(buf.as_ref().to_vec()))];
            Ok((batches, total_in_bytes))
        }
    }

    //
    // create a new database
    //
    fn create_database(
        db: Pin<&mut Merk>,
        sender: &DB3Address,
        nonce: u64,
        tx: &TxId,
        mutation: &DatabaseMutation,
        block_id: u64,
        mutation_id: u16,
        desc: &str,
    ) -> Result<DbStoreOp> {
        let (batches, ops) =
            Self::convert(sender, nonce, tx, mutation, block_id, mutation_id, desc)?;
        unsafe {
            Pin::get_unchecked_mut(db)
                .apply(&batches, &[])
                .map_err(|e| DB3Error::ApplyDatabaseError(format!("{e}")))?;
        }
        Ok(ops)
    }

    //
    // add a new collection to database
    //
    fn add_collection(
        db: Pin<&mut Merk>,
        sender: &DB3Address,
        tx: &TxId,
        mutation: &DatabaseMutation,
        block_id: u64,
        mutation_id: u16,
    ) -> Result<DbStoreOp> {
        let addr_ref: &[u8] = mutation.db_address.as_ref();
        let db_id = DbId::try_from(addr_ref)?;
        let database = Self::get_database(db.as_ref(), &db_id)?;
        match database {
            Some(d) => {
                let sender_ref: &[u8] = d.sender.as_ref();
                if sender_ref != sender.as_ref() {
                    warn!(
                        "no permission to add collection to database {}",
                        db_id.to_hex()
                    );
                    Ok(DbStoreOp::DbOp {
                        create_db_ops: 0,
                        create_collection_ops: 0,
                        create_index_ops: 0,
                        data_in_bytes: 0,
                    })
                } else {
                    let (new_db, mut ops) =
                        Self::update_database(&d, mutation, tx, block_id, mutation_id)?;
                    //TODO how to get the byte size that was updated
                    let (batches, data_in_bytes) = Self::encode_database(
                        db_id,
                        &new_db,
                        sender,
                        block_id,
                        mutation_id,
                        false,
                    )?;
                    ops.update_data_size(data_in_bytes as u64);
                    unsafe {
                        Pin::get_unchecked_mut(db)
                            .apply(&batches, &[])
                            .map_err(|e| DB3Error::ApplyDatabaseError(format!("{e}")))?;
                    }
                    Ok(ops)
                }
            }
            None => {
                warn!("database not found with addr {}", db_id.to_hex());
                return Err(DB3Error::ApplyDatabaseError(format!(
                    "database not found with addr {}",
                    db_id.to_hex()
                )));
            }
        }
    }

    //
    // delete document
    //
    fn delete_document(
        db: Pin<&mut Merk>,
        sender: &DB3Address,
        mutation: &DatabaseMutation,
    ) -> Result<DbStoreOp> {
        let span = span!(Level::INFO, "document").entered();
        let addr_ref: &[u8] = mutation.db_address.as_ref();
        let db_id = DbId::try_from(addr_ref)?;
        let database = Self::get_database(db.as_ref(), &db_id)?;
        let mut entries: Vec<BatchEntry> = Vec::new();
        match database {
            Some(d) => {
                for document_mutation in &mutation.document_mutations {
                    if let Some(collection) = d
                        .collections
                        .iter()
                        .find(|x| x.name.as_str() == document_mutation.collection_name.as_str())
                    {
                        let collection_id = CollectionId::try_from_bytes(collection.id.as_slice())?;
                        for doc_id_base64 in document_mutation.ids.iter() {
                            let document_id = DocumentId::try_from_base64(&doc_id_base64)?;
                            if let Some(v) = db
                                .get(document_id.as_ref())
                                .map_err(|e| DB3Error::QueryDocumentError(format!("{:?}", e)))?
                            {
                                let db3_doc = DB3Document::try_from(v.clone())?;
                                let owner = &db3_doc.get_owner()?;
                                if sender == owner {
                                    info!("delete doc id {}", document_id);
                                    entries.push((document_id.as_ref().to_vec(), Op::Delete));
                                    for index in collection.index_list.iter() {
                                        let key = db3_doc.get_keys(index)?;
                                        match key {
                                            Some(k) => {
                                                let index_id = IndexId::create(
                                                    &collection_id,
                                                    index.id,
                                                    k.as_ref(),
                                                    &document_id,
                                                )?;
                                                entries
                                                    .push((index_id.as_ref().to_vec(), Op::Delete));
                                            }
                                            None => {}
                                        }
                                    }
                                } else {
                                    return Err(DB3Error::DocumentModifiedPermissionError);
                                }
                            } else {
                                warn!("delete doc with id {} not exist", doc_id_base64);
                                return Err(DB3Error::DocumentNotExist(doc_id_base64.clone()));
                            }
                        }
                    } else {
                        return Err(DB3Error::CollectionNotFound(
                            document_mutation.collection_name.to_string(),
                        ));
                    }
                }
            }
            None => {
                return Err(DB3Error::ApplyDatabaseError(format!(
                    "database not found with addr {}",
                    db_id.to_hex()
                )));
            }
        }
        let del_doc_ops: u64 = entries.len() as u64;
        unsafe {
            entries.sort_by(|(a_key, _), (b_key, _)| a_key.cmp(&b_key));
            Pin::get_unchecked_mut(db)
                .apply(&entries, &[])
                .map_err(|e| DB3Error::ApplyDatabaseError(format!("{:?}", e)))?;
        }
        span.exit();
        Ok(DbStoreOp::DocOp {
            add_doc_ops: 0,
            del_doc_ops,
            update_doc_ops: 0,
            data_in_bytes: 0,
        })
    }

    //
    // add document
    //
    fn add_document(
        db: Pin<&mut Merk>,
        sender: &DB3Address,
        tx: &TxId,
        mutation: &DatabaseMutation,
        block_id: u64,
        mutation_id: u16,
    ) -> Result<(DbStoreOp, Vec<DocumentId>)> {
        let addr_ref: &[u8] = mutation.db_address.as_ref();
        let db_id = DbId::try_from(addr_ref)?;
        let database = Self::get_database(db.as_ref(), &db_id)?;
        let span = span!(Level::INFO, "document").entered();
        let mut add_doc_ops: u64 = 0;
        let mut data_in_bytes: u64 = 0;
        match database {
            Some(d) => {
                let mut entries: Vec<BatchEntry> = Vec::new();
                let mut document_ids: Vec<DocumentId> = Vec::new();
                let cid_index_map: HashMap<&str, &Collection> =
                    d.collections.iter().map(|x| (x.name.as_str(), x)).collect();
                for document_mutation in &mutation.document_mutations {
                    if let Some(collection) =
                        cid_index_map.get(document_mutation.collection_name.as_str())
                    {
                        let collection_id = CollectionId::try_from_bytes(collection.id.as_slice())
                            .map_err(|e| DB3Error::InvalidCollectionIdBytes(format!("{:?}", e)))?;
                        for document in document_mutation.documents.iter() {
                            // generate document entry id
                            let document_entry_id = DocumentEntryId::create(
                                block_id,
                                mutation_id,
                                entries.len() as u16,
                            )
                            .map_err(|e| DB3Error::ApplyDocumentError(format!("{:?}", e)))?;
                            // generate document id
                            let document_id =
                                DocumentId::create(&collection_id, &document_entry_id).map_err(
                                    |e| DB3Error::ApplyDocumentError(format!("{:?}", e)),
                                )?;

                            // construct db3 document with tx_id and owner addr
                            let db3_document = DB3Document::create_from_document_bytes(
                                document.clone(),
                                &document_id,
                                &tx,
                                &sender,
                            )
                            .map_err(|e| DB3Error::ApplyDocumentError(format!("{:?}", e)))?;
                            let document_vec = db3_document.into_bytes().to_vec();
                            add_doc_ops += 1;
                            data_in_bytes += document_vec.len() as u64;
                            info!("put document id {}", document_id.to_string());
                            entries.push((document_id.as_ref().to_vec(), Op::Put(document_vec)));
                            // insert index key -> document_id
                            for index in collection.index_list.iter() {
                                // retrieve key(single/multiple) from db3 document
                                match db3_document.get_keys(index)? {
                                    Some(key) => {
                                        // generate index id
                                        let index_id = IndexId::create(
                                            &collection_id,
                                            index.id,
                                            // TODO: convert key into bson bytes
                                            key.as_ref(),
                                            &document_id,
                                        )?;
                                        // put indexId->documentId
                                        info!("put index id {}", index_id.to_string());
                                        add_doc_ops += 1;
                                        entries.push((index_id.as_ref().to_vec(), Op::Put(vec![])));
                                    }
                                    None => {
                                        info!("no index value");
                                    }
                                }
                            }
                            document_ids.push(document_id);
                        }
                    }
                }
                unsafe {
                    entries.sort_by(|(a_key, _), (b_key, _)| a_key.cmp(&b_key));
                    Pin::get_unchecked_mut(db)
                        .apply(&entries, &[])
                        .map_err(|e| DB3Error::ApplyDocumentError(format!("{:?}", e)))?;
                }
                span.exit();
                let ops = DbStoreOp::DocOp {
                    add_doc_ops,
                    del_doc_ops: 0,
                    update_doc_ops: 0,
                    data_in_bytes,
                };
                return Ok((ops, document_ids));
            }
            None => {
                span.exit();
                return Err(DB3Error::ApplyDocumentError(format!(
                    "database not found with addr {}",
                    db_id.to_hex()
                )));
            }
        }
    }

    //
    // update document
    //
    fn update_document(
        db: Pin<&mut Merk>,
        sender: &DB3Address,
        tx: &TxId,
        mutation: &DatabaseMutation,
        _block_id: u64,
        _mutation_id: u16,
    ) -> Result<DbStoreOp> {
        let addr_ref: &[u8] = mutation.db_address.as_ref();
        let db_id = DbId::try_from(addr_ref)?;
        let database = Self::get_database(db.as_ref(), &db_id)?;
        let span = span!(Level::INFO, "document").entered();
        let mut data_in_bytes: u64 = 0;
        match database {
            Some(d) => {
                let mut entries: Vec<BatchEntry> = Vec::new();
                let cid_index_map: HashMap<&str, &Collection> =
                    d.collections.iter().map(|x| (x.name.as_str(), x)).collect();
                for document_mutation in &mutation.document_mutations {
                    if document_mutation.ids.len() != document_mutation.documents.len() {
                        return Err(DB3Error::ApplyDocumentError(
                            "invalid update document mutation, ids and documents size different"
                                .to_string(),
                        ));
                    }
                    if document_mutation.ids.len() != document_mutation.masks.len() {
                        return Err(DB3Error::ApplyDocumentError(
                            "invalid update document mutation, ids and masks size different"
                                .to_string(),
                        ));
                    }
                    if let Some(collection) =
                        cid_index_map.get(document_mutation.collection_name.as_str())
                    {
                        let collection_id = CollectionId::try_from_bytes(collection.id.as_slice())
                            .map_err(|e| DB3Error::InvalidCollectionIdBytes(format!("{:?}", e)))?;

                        let field_index_map = Self::collect_field_index_map(collection);
                        for idx in 0..document_mutation.documents.len() {
                            if document_mutation.masks[idx].fields.len() == 0 {
                                info!("skip update doc when masks fields are empty");
                                continue;
                            }
                            info!("document id {}", document_mutation.ids[idx].as_str());
                            let document_id =
                                DocumentId::try_from_base64(document_mutation.ids[idx].as_str())?;
                            let old_document = if let Some(v) = db
                                .get(document_id.as_ref())
                                .map_err(|e| DB3Error::QueryDocumentError(format!("{e}")))?
                            {
                                DB3Document::try_from(v.clone())?
                            } else {
                                return Err(DB3Error::DocumentNotExist(
                                    document_mutation.ids[idx].to_string(),
                                ));
                            };

                            if sender != &old_document.get_owner()? {
                                return Err(DB3Error::DocumentModifiedPermissionError);
                            }
                            let mut new_doc = old_document.get_document()?.clone();
                            let update_doc = bson_util::bytes_to_bson_document(
                                document_mutation.documents[idx].clone(),
                            )?;

                            // update document based on update_doc and update masks
                            //
                            // update masks - The fields to update.
                            // None of the field paths in the mask may contain a reserved name.
                            //
                            // If the document exists on the server and has fields not referenced in the
                            // mask, they are left unchanged.
                            // Fields referenced in the mask, but not present in the input document, are
                            // deleted from the document on the server.
                            for field in document_mutation.masks[idx].fields.iter() {
                                match update_doc.get(field) {
                                    Some(bson) => {
                                        // update the fields with the value from updated document
                                        new_doc.insert(field, bson);
                                    }
                                    None => {
                                        // deleted from the document
                                        new_doc.remove(field);
                                    }
                                }
                            }
                            // construct new db3 document with new tx_id and new document
                            let new_document = DB3Document::create_from_document_bytes(
                                bson_util::bson_document_into_bytes(&new_doc),
                                &document_id,
                                &tx,
                                &sender,
                            )?;

                            let old_doc = old_document.get_document()?;
                            let new_doc = new_document.get_document()?;

                            // collection modified index set
                            let mut updated_index_set: HashMap<String, &Index> = HashMap::new();
                            for (field, index_vec) in field_index_map.iter() {
                                if old_doc.get(field) == new_doc.get(field) {
                                    continue;
                                }
                                index_vec.iter().for_each(|index| {
                                    updated_index_set.insert(index.name.to_string(), *index);
                                });
                            }

                            // update index filed related index
                            for (_, index) in updated_index_set.iter() {
                                // step 1. remove old index entry
                                if let Some(old_key) = old_document.get_keys(index)? {
                                    let index_id = IndexId::create(
                                        &collection_id,
                                        index.id,
                                        &old_key.as_ref(),
                                        &document_id,
                                    )?;
                                    // remove old index entry
                                    info!("update doc: delete index id {}", index_id);
                                    entries.push((index_id.as_ref().to_vec(), Op::Delete));
                                } else {
                                    info!("update doc: skip delete index");
                                }

                                // step 2. add new index entry
                                if let Some(new_key) = new_document.get_keys(index)? {
                                    let index_id = IndexId::create(
                                        &collection_id,
                                        index.id,
                                        &new_key.as_ref(),
                                        &document_id,
                                    )?;
                                    // add new index entry
                                    info!("update doc: add index id {}", index_id);
                                    entries.push((index_id.as_ref().to_vec(), Op::Put(vec![])));
                                } else {
                                    info!("update doc: skip add index");
                                }
                            }

                            let document_vec = new_document.into_bytes().to_vec();
                            data_in_bytes += document_vec.len() as u64;
                            info!("update doc: put document id {}", document_id.to_string());
                            entries.push((document_id.as_ref().to_vec(), Op::Put(document_vec)));
                        }
                    }
                }
                let update_doc_ops: u64 = entries.len() as u64;
                unsafe {
                    entries.sort_by(|(a_key, _), (b_key, _)| a_key.cmp(&b_key));
                    Pin::get_unchecked_mut(db)
                        .apply(&entries, &[])
                        .map_err(|e| DB3Error::ApplyDocumentError(format!("{:?}", e)))?;
                }
                span.exit();
                let ops = DbStoreOp::DocOp {
                    add_doc_ops: 0,
                    del_doc_ops: 0,
                    update_doc_ops,
                    data_in_bytes,
                };
                return Ok(ops);
            }
            None => {
                span.exit();
                return Err(DB3Error::ApplyDocumentError(format!(
                    "database not found with addr {}",
                    db_id.to_hex()
                )));
            }
        }
    }

    fn collect_field_index_map(collection: &Collection) -> HashMap<String, Vec<&Index>> {
        let mut field_index_map: HashMap<String, Vec<_>> = HashMap::new();
        for index in collection.index_list.iter() {
            for field in index.fields.iter() {
                if let Some(list) = field_index_map.get_mut(&field.field_path) {
                    list.push(index);
                } else {
                    field_index_map.insert(field.field_path.to_string(), vec![index]);
                }
            }
        }
        field_index_map
    }

    /// check if the fields_names is a prefix of field_indexes
    /// e.g. fields_names = ["a", "b"]
    ///     field_indexes = ["a", "b", "c", "d"]
    fn index_start_with_fields(
        fields_names: &Vec<String>,
        field_indexes: &Vec<IndexField>,
    ) -> bool {
        if fields_names.len() > field_indexes.len() {
            return false;
        }

        for i in 0..fields_names.len() {
            if fields_names[i] != field_indexes[i].field_path {
                return false;
            }
        }
        true
    }
    /// run a query to fetch target documents from given database and collection
    pub fn run_query(
        db: Pin<&Merk>,
        db_id: &DbId,
        query: &StructuredQuery,
    ) -> Result<Vec<Document>> {
        debug!("run_query : {:?}", query);
        match Self::get_database(db, db_id) {
            Ok(Some(database)) => {
                if let Some(collection) = database
                    .collections
                    .iter()
                    .find(|x| x.name.as_str() == query.collection_name.as_str())
                {
                    let limit = match &query.limit {
                        Some(v) => Some(v.limit),
                        None => None,
                    };
                    let field_index_map = Self::collect_field_index_map(collection);
                    let collection_id = CollectionId::try_from_bytes(collection.id.as_slice())?;
                    if let Some(where_filter) = &query.r#where {
                        match &where_filter.filter_type {
                            Some(FilterType::FieldFilter(field_filter)) => {
                                let index = match field_index_map.get(&field_filter.field) {
                                    Some(index_list) => {
                                        match index_list
                                            .iter()
                                            .find_or_first(|i| i.fields.len() == 1)
                                        {
                                            Some(index_match) => index_match,
                                            None => {
                                                return Err(DB3Error::IndexNotFoundForFiledFilter(
                                                    field_filter.field.to_string(),
                                                ));
                                            }
                                        }
                                    }
                                    None => {
                                        return Err(DB3Error::IndexNotFoundForFiledFilter(
                                            field_filter.field.to_string(),
                                        ));
                                    }
                                };

                                let key = match &field_filter.value {
                                    Some(value) => FieldKey::create_single_key(Some(
                                        bson_util::bson_value_from_proto_value(value)?,
                                    ))?,
                                    None => {
                                        return Err(DB3Error::InvalidFilterValue(
                                            "None field filter value un-support".to_string(),
                                        ));
                                    }
                                };
                                let range = Self::generate_range_with_single_field_filter(
                                    &collection_id,
                                    index,
                                    key.as_ref(),
                                    Operator::from_i32(field_filter.op),
                                )?;
                                Self::execute_query(db, &range, limit)
                            }
                            Some(FilterType::CompositeFilter(composite_filter)) => {
                                match CompositeOp::from_i32(composite_filter.op) {
                                    Some(CompositeOp::And) => {
                                        let mut fields_names = vec![];
                                        let mut fields_values = vec![];

                                        for filter in composite_filter.filters.iter() {
                                            match &filter.filter_type {
                                                Some(FilterType::FieldFilter(field_filter)) => {
                                                    match &field_filter.value {
                                                        Some(value) => {
                                                            if field_filter.op
                                                                != Operator::Equal as i32
                                                            {
                                                                return Err(DB3Error::InvalidFilterValue(
                                                                    "CompositeOp And only support Equal".to_string(),
                                                                ));
                                                            }
                                                            fields_names.push(
                                                                field_filter.field.to_string(),
                                                            );
                                                            fields_values.push(Some(
                                                                bson_util::bson_value_from_proto_value(value)?));
                                                        }
                                                        None => {
                                                            return Err(DB3Error::InvalidFilterValue(
                                                                "None field filter value un-support".to_string(),
                                                            ));
                                                        }
                                                    }
                                                }
                                                _ => {
                                                    return Err(DB3Error::InvalidFilterValue(
                                                        "CompositeOp And only support FieldFilter"
                                                            .to_string(),
                                                    ));
                                                }
                                            }
                                        }

                                        if fields_names.is_empty() {
                                            return Err(DB3Error::InvalidFilterValue(
                                                "CompositeOp can't support empty filters"
                                                    .to_string(),
                                            ));
                                        }
                                        let key = FieldKey::create(&fields_values)?;
                                        let index = match field_index_map.get(&fields_names[0]) {
                                            Some(index_list) => {
                                                match index_list.iter().find_or_first(|i| {
                                                    Self::index_start_with_fields(
                                                        &fields_names,
                                                        &i.fields,
                                                    )
                                                }) {
                                                    Some(index_match) => index_match,
                                                    None => {
                                                        return Err(
                                                            DB3Error::IndexNotFoundForFiledFilter(
                                                                fields_names[0].to_string(),
                                                            ),
                                                        );
                                                    }
                                                }
                                            }
                                            None => {
                                                return Err(DB3Error::IndexNotFoundForFiledFilter(
                                                    fields_names[0].to_string(),
                                                ));
                                            }
                                        };
                                        debug!("filter names: {:?}", fields_names);
                                        debug!("index: {:?}", index);

                                        let range = Self::generate_range_with_single_field_filter(
                                            &collection_id,
                                            index,
                                            key.as_ref(),
                                            Some(Operator::Equal),
                                        )?;
                                        debug!("range: {:?}", range);
                                        Self::execute_query(db, &range, limit)
                                    }
                                    _ => {
                                        return Err(DB3Error::InvalidFilterType(format!(
                                            "Invalid composite op {:?}",
                                            CompositeOp::from_i32(composite_filter.op)
                                        )));
                                    }
                                }
                            }
                            None => {
                                return Err(DB3Error::InvalidFilterType(
                                    "None filter type unsupport".to_string(),
                                ));
                            }
                        }
                    } else {
                        Self::get_documents(db, &collection_id, limit)
                    }
                } else {
                    Err(DB3Error::QueryDocumentError(format!(
                        "collection not exist with target name {}",
                        query.collection_name
                    )))
                }
            }
            Ok(None) => Err(DB3Error::QueryDocumentError(format!(
                "database not exist with target id {}",
                db_id.to_hex()
            ))),
            Err(e) => Err(e),
        }
    }

    /// generate range for single field filter
    /// e.g : field = "name", value = "jack", op = "equal"
    /// range = [name:jack:00000, name:jack:11111]
    /// e.g : field = "name", value = "jack", op = "less_than"
    /// range = (unbounded, name:jack:00000)
    /// etc.
    fn generate_range_with_single_field_filter(
        collection_id: &CollectionId,
        index: &Index,
        key: &Vec<u8>,
        op: Option<Operator>,
    ) -> Result<(Bound<IndexId>, Bound<IndexId>)> {
        match op {
            Some(Operator::Equal) => {
                let start_key = IndexId::create(
                    &collection_id,
                    index.id,
                    key.as_slice(),
                    &DocumentId::zero(),
                )?;

                let end_key =
                    IndexId::create(&collection_id, index.id, key.as_slice(), &DocumentId::one())?;
                Ok((Bound::Included(start_key), Bound::Included(end_key)))
            }
            Some(Operator::LessThan) => {
                let start_key =
                    IndexId::create(&collection_id, index.id, "".as_bytes(), &DocumentId::zero())?;
                let end_key = IndexId::create(
                    &collection_id,
                    index.id,
                    key.as_slice(),
                    &DocumentId::zero(),
                )?;
                Ok((Bound::Included(start_key), Bound::Excluded(end_key)))
            }
            Some(Operator::LessThanOrEqual) => {
                let start_key =
                    IndexId::create(&collection_id, index.id, "".as_bytes(), &DocumentId::zero())?;
                let end_key =
                    IndexId::create(&collection_id, index.id, key.as_slice(), &DocumentId::one())?;
                Ok((Bound::Included(start_key), Bound::Included(end_key)))
            }
            Some(Operator::GreaterThan) => {
                let start_key =
                    IndexId::create(&collection_id, index.id, key.as_slice(), &DocumentId::one())?;
                let end_key = IndexId::create(
                    &collection_id,
                    index.id + 1,
                    "".as_bytes(),
                    &DocumentId::zero(),
                )?;
                Ok((Bound::Excluded(start_key), Bound::Excluded(end_key)))
            }
            Some(Operator::GreaterThanOrEqual) => {
                let start_key = IndexId::create(
                    &collection_id,
                    index.id,
                    key.as_slice(),
                    &DocumentId::zero(),
                )?;
                let end_key = IndexId::create(
                    &collection_id,
                    index.id + 1,
                    "".as_bytes(),
                    &DocumentId::zero(),
                )?;
                Ok((Bound::Included(start_key), Bound::Excluded(end_key)))
            }
            _ => {
                // TODO: support more not equal operator
                return Err(DB3Error::InvalidFilterType(format!(
                    "Filed Filter Op {:?} un-support",
                    op
                )));
            }
        }
    }

    /// execute a query to fetch target documents from given database and index range
    fn execute_query(
        db: Pin<&Merk>,
        range: &(Bound<IndexId>, Bound<IndexId>),
        limit: Option<i32>,
    ) -> Result<Vec<Document>> {
        let mut values: Vec<_> = Vec::new();
        let mut count = 0;

        let mut it = db.raw_iter();
        match &range.0 {
            Bound::Included(start) => it.seek(start.as_ref()),
            Bound::Excluded(start) => {
                // 1. Seeks to the start key, or the first key that lexicographically precedes it.
                // 2. Advance iterator to the next key to exclude the start key
                it.seek_for_prev(start.as_ref());
                if it.valid() {
                    it.next();
                }
            }
            Bound::Unbounded => it.seek_to_first(),
        };

        if !it.valid() {
            return Ok(values);
        }

        let mut it_end = db.raw_iter();
        it_end.seek_to_last();
        let it_end_key = it_end.key().unwrap();

        let (end_key_ref, end_key_exclude) = match &range.1 {
            Bound::Unbounded => (it_end_key, false),
            Bound::Included(end) => (end.as_ref().as_slice(), false),
            Bound::Excluded(end) => (end.as_ref().as_slice(), true),
        };

        while it.valid() {
            if limit.is_some() && count >= limit.unwrap() {
                break;
            }
            if let Some(k) = it.key() {
                if k > end_key_ref {
                    break;
                }
                if k == end_key_ref && end_key_exclude {
                    break;
                }
                let index_id = IndexId::new(k.to_vec());
                let document_id = index_id.get_document_id()?;
                if let Ok(Some(document)) = Self::get_document(db, &document_id) {
                    count += 1;
                    values.push(document)
                } else {
                    warn!("document not exist with target id {}", document_id);
                }
            }
            it.next();
        }
        Ok(values)
    }
    //
    // add document
    //
    pub fn get_document(db: Pin<&Merk>, document_id: &DocumentId) -> Result<Option<Document>> {
        //TODO use reference
        debug!("get document id: {}", document_id);
        if let Some(v) = db
            .get(document_id.as_ref())
            .map_err(|e| DB3Error::QueryDocumentError(format!("{e}")))?
        {
            let db3_doc = DB3Document::try_from(v.clone())?;
            let doc = bson_util::bson_document_into_bytes(db3_doc.get_document()?);
            let owner = db3_doc.get_owner()?.to_vec();
            let tx_id = db3_doc.get_tx_id()?.as_ref().to_vec();
            Ok(Some(Document {
                id: document_id.as_ref().to_vec(),
                doc,
                owner,
                tx_id,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_documents(
        db: Pin<&Merk>,
        collection_id: &CollectionId,
        limit: Option<i32>,
    ) -> Result<Vec<Document>> {
        let start_key = DocumentId::create(collection_id, &DocumentEntryId::zero())
            .unwrap()
            .as_ref()
            .to_vec();
        let end_key = DocumentId::create(collection_id, &DocumentEntryId::one())
            .unwrap()
            .as_ref()
            .to_vec();
        let mut it = db.raw_iter();
        it.seek(start_key);
        let mut count = 0;
        let mut docs: Vec<Document> = Vec::new();
        let end_key_ref: &[u8] = end_key.as_ref();
        while it.valid() {
            if limit.is_some() && count >= limit.unwrap() {
                break;
            }
            if let Some(k) = it.key() {
                if k >= end_key_ref {
                    break;
                }
                if let Some(data) = it.value() {
                    let tree: Tree = ed::Decode::decode(data).unwrap();
                    let db3_doc = DB3Document::try_from(tree.value().to_vec())?;
                    //TODO too much overhead
                    let doc = bson_util::bson_document_into_bytes(db3_doc.get_document()?);
                    let owner = db3_doc.get_owner()?.to_vec();
                    let tx_id = db3_doc.get_tx_id()?.as_ref().to_vec();
                    docs.push(Document {
                        id: k.to_vec(),
                        doc,
                        owner,
                        tx_id,
                    });
                }
            } else {
                //invalid key
                break;
            }
            if limit.is_some() && count >= limit.unwrap() {
                break;
            }
            count += 1;
            it.next();
        }
        Ok(docs)
    }

    pub fn apply_mutation(
        db: Pin<&mut Merk>,
        sender: &DB3Address,
        nonce: u64,
        tx: &TxId,
        mutation: &DatabaseMutation,
        block_id: u64,
        mutation_id: u16,
    ) -> Result<DbStoreOp> {
        let action = DatabaseAction::from_i32(mutation.action);
        match action {
            Some(DatabaseAction::CreateDb) => Self::create_database(
                db,
                sender,
                nonce,
                tx,
                mutation,
                block_id,
                mutation_id,
                mutation.db_desc.as_str(),
            ),
            Some(DatabaseAction::AddCollection) => {
                Self::add_collection(db, sender, tx, mutation, block_id, mutation_id)
            }
            Some(DatabaseAction::AddDocument) => {
                // TODO: send event with added ids
                let (ops, _) = Self::add_document(db, sender, tx, mutation, block_id, mutation_id)?;
                Ok(ops)
            }
            Some(DatabaseAction::UpdateDocument) => {
                Self::update_document(db, sender, tx, mutation, block_id, mutation_id)
            }

            Some(DatabaseAction::DeleteDocument) => Self::delete_document(db, sender, mutation),
            None => todo!(),
        }
    }

    pub fn get_my_database(db: Pin<&Merk>, sender: &DB3Address) -> Result<Vec<Database>> {
        let start_key = DbOwnerKey::min(sender)?;
        let end_key = DbOwnerKey::max(sender)?;
        let mut it = db.raw_iter();
        it.seek(start_key);
        let mut count = 0;
        let mut dbs: Vec<Database> = Vec::new();
        let end_key_ref: &[u8] = end_key.as_ref();
        //TODO limit the max database
        let limit: u32 = 100;
        while it.valid() {
            if count >= limit {
                break;
            }
            if let Some(k) = it.key() {
                if k >= end_key_ref {
                    break;
                }
                if let Some(data) = it.value() {
                    let tree: Tree = ed::Decode::decode(data).unwrap();
                    let dbid = DbId::try_from(tree.value())?;
                    match Self::get_database(db.as_ref(), &dbid) {
                        Ok(Some(database)) => {
                            dbs.push(database);
                        }
                        _ => {}
                    }
                }
            } else {
                //invalid key
                break;
            }
            if count >= limit {
                break;
            }
            count += 1;
            it.next();
        }
        Ok(dbs)
    }

    pub fn get_database(db: Pin<&Merk>, id: &DbId) -> Result<Option<Database>> {
        //TODO use reference
        let key = DbKey(id.clone());
        let encoded_key = key.encode()?;
        let value = db
            .get(encoded_key.as_ref())
            .map_err(|e| DB3Error::QueryDatabaseError(format!("{:?}", e)))?;
        if let Some(v) = value {
            match Database::decode(v.as_ref()) {
                Ok(database) => Ok(Some(database)),
                Err(e) => Err(DB3Error::QueryDatabaseError(format!("{:?}", e))),
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::Bson;
    use db3_base::bson_util;
    use db3_crypto::key_derive;
    use db3_crypto::signature_scheme::SignatureScheme;
    use db3_proto::db3_database_proto::structured_query::{
        value::ValueType, CompositeFilter, FieldFilter, Filter, Limit, Projection, Value,
    };
    use db3_proto::db3_database_proto::{
        index::index_field::{Order, ValueMode},
        index::IndexField,
        Index,
    };
    use db3_proto::db3_mutation_proto::CollectionMutation;
    use db3_proto::db3_mutation_proto::DocumentMask;
    use db3_proto::db3_mutation_proto::DocumentMutation;
    use std::boxed::Box;
    use std::ops::Bound::{Excluded, Included};
    use tempdir::TempDir;

    fn gen_address() -> DB3Address {
        let seed: [u8; 32] = [0; 32];
        let (address, _) =
            key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::ED25519).unwrap();
        address
    }

    fn build_delete_document_mutation(
        addr: &DB3Address,
        collection_name: &str,
        ids: Vec<String>,
    ) -> DatabaseMutation {
        let document_mutations = vec![DocumentMutation {
            collection_name: collection_name.to_string(),
            documents: vec![],
            ids,
            masks: vec![],
        }];
        let dm = DatabaseMutation {
            meta: None,
            collection_mutations: vec![],
            document_mutations,
            db_address: addr.to_vec(),
            action: DatabaseAction::DeleteDocument.into(),
            db_desc: "".to_string(),
        };
        let json_data = serde_json::to_string(&dm).unwrap();
        println!("{json_data}");
        dm
    }

    fn build_add_document_mutation(
        addr: &DB3Address,
        collection_name: &str,
        docs: Vec<String>,
    ) -> DatabaseMutation {
        let documents = docs
            .iter()
            .map(|data| bson_util::json_str_to_bson_bytes(data).unwrap())
            .collect();
        let document_mutations = vec![DocumentMutation {
            collection_name: collection_name.to_string(),
            documents,
            ids: vec![],
            masks: vec![],
        }];
        let dm = DatabaseMutation {
            meta: None,
            collection_mutations: vec![],
            document_mutations,
            db_address: addr.to_vec(),
            action: DatabaseAction::AddDocument.into(),
            db_desc: "".to_string(),
        };
        let json_data = serde_json::to_string(&dm).unwrap();
        println!("{json_data}");
        dm
    }

    fn build_update_document_mutation(
        addr: &DB3Address,
        collection_name: &str,
        ids: Vec<String>,
        docs: Vec<String>,
        masks: Vec<Vec<String>>,
    ) -> DatabaseMutation {
        let documents = docs
            .iter()
            .map(|data| bson_util::json_str_to_bson_bytes(data).unwrap())
            .collect();
        let masks: Vec<_> = masks
            .iter()
            .map(|m| DocumentMask { fields: m.to_vec() })
            .collect();
        let document_mutations = vec![DocumentMutation {
            collection_name: collection_name.to_string(),
            documents,
            ids,
            masks,
        }];
        let dm = DatabaseMutation {
            meta: None,
            collection_mutations: vec![],
            document_mutations,
            db_address: addr.to_vec(),
            action: DatabaseAction::UpdateDocument.into(),
            db_desc: "".to_string(),
        };
        let json_data = serde_json::to_string(&dm).unwrap();
        println!("{json_data}");
        dm
    }

    fn build_database_mutation(addr: &DB3Address, collection_name: &str) -> DatabaseMutation {
        let index_field_name = IndexField {
            field_path: "name".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };

        let index_name = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![index_field_name],
        };

        let index_field_age = IndexField {
            field_path: "age".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };

        let index_age = Index {
            id: 1,
            name: "idx2".to_string(),
            fields: vec![index_field_age],
        };

        let index_mutation = CollectionMutation {
            index: vec![index_name, index_age],
            collection_name: collection_name.to_string(),
        };

        let dm = DatabaseMutation {
            meta: None,
            collection_mutations: vec![index_mutation],
            document_mutations: vec![],
            db_address: addr.to_vec(),
            action: DatabaseAction::CreateDb.into(),
            db_desc: "".to_string(),
        };
        let json_data = serde_json::to_string(&dm).unwrap();
        println!("{json_data}");
        dm
    }

    fn build_database_mutation_with_multi_key_index(
        addr: &DB3Address,
        collection_name: &str,
    ) -> DatabaseMutation {
        let index_field_name = IndexField {
            field_path: "name".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };

        let index_field_age = IndexField {
            field_path: "age".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };

        let index_name_age = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![index_field_name, index_field_age],
        };

        let index_mutation = CollectionMutation {
            index: vec![index_name_age],
            collection_name: collection_name.to_string(),
        };

        let dm = DatabaseMutation {
            meta: None,
            collection_mutations: vec![index_mutation],
            document_mutations: vec![],
            db_address: addr.to_vec(),
            action: DatabaseAction::CreateDb.into(),
            db_desc: "".to_string(),
        };
        let json_data = serde_json::to_string(&dm).unwrap();
        println!("{json_data}");
        dm
    }

    #[test]
    fn collect_field_index_map_ut() {
        let index_field1 = IndexField {
            field_path: "name".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };
        let index_field2 = IndexField {
            field_path: "age".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };
        let index1 = Index {
            name: "index1".to_string(),
            id: 1,
            fields: vec![index_field1.clone()],
        };
        let index2 = Index {
            name: "index2".to_string(),
            id: 1,
            fields: vec![index_field2.clone()],
        };
        let index3 = Index {
            name: "index3".to_string(),
            id: 1,
            fields: vec![index_field1.clone(), index_field2.clone()],
        };
        let collection = Collection {
            id: vec![],
            name: "collection1".to_string(),
            index_list: vec![index1, index2, index3],
        };

        let field_index_map = DbStore::collect_field_index_map(&collection);
        assert_eq!(2, field_index_map.len());
        assert!(field_index_map.contains_key("name"));
        assert!(field_index_map.contains_key("age"));

        let name_related_index = field_index_map.get("name").unwrap();
        assert_eq!(2, name_related_index.len());
        assert_eq!("index1", name_related_index[0].name);
        assert_eq!("index3", name_related_index[1].name);

        let age_related_index = field_index_map.get("age").unwrap();
        assert_eq!(2, age_related_index.len());
        assert_eq!("index2", age_related_index[0].name);
        assert_eq!("index3", age_related_index[1].name);
    }

    #[test]
    fn db_store_new_database_test() {
        let addr = gen_address();
        let db_id = DbId::try_from((&addr, 1)).unwrap();
        let db_mutation = build_database_mutation(&addr, "collection1");
        let desc = "";
        let (database, _) =
            DbStore::new_database(&db_id, &addr, &TxId::zero(), &db_mutation, 1000, 100, desc);
        assert!(database
            .collections
            .iter()
            .find(|x| x.name.as_str() == "collection1")
            .is_some());
    }

    #[test]
    fn db_store_update_database_test() {
        let addr = gen_address();
        let db_id = DbId::try_from((&addr, 1)).unwrap();
        let db_mutation = build_database_mutation(&addr, "collection1");
        let desc = "";
        let (old_database, _) =
            DbStore::new_database(&db_id, &addr, &TxId::zero(), &db_mutation, 1000, 100, desc);
        assert!(old_database
            .collections
            .iter()
            .find(|x| x.name.as_str() == "collection1")
            .is_some());
        let db_mutation_2 = build_database_mutation(&addr, "collection2");
        let (new_database, _) =
            DbStore::update_database(&old_database, &db_mutation_2, &TxId::zero(), 1000, 101)
                .unwrap();

        assert!(new_database
            .collections
            .iter()
            .find(|x| x.name.as_str() == "collection1")
            .is_some());
        assert!(new_database
            .collections
            .iter()
            .find(|x| x.name.as_str() == "collection2")
            .is_some());
    }

    #[test]
    fn db_store_update_database_wrong_path() {
        let addr = gen_address();
        let db_id = DbId::try_from((&addr, 1)).unwrap();
        let db_mutation = build_database_mutation(&addr, "collection1");
        let desc = "";
        let (old_database, _) =
            DbStore::new_database(&db_id, &addr, &TxId::zero(), &db_mutation, 1000, 100, desc);
        assert!(old_database
            .collections
            .iter()
            .find(|x| x.name.as_str() == "collection1")
            .is_some());
        let db_mutation_2 = build_database_mutation(&addr, "collection1");
        let res = DbStore::update_database(&old_database, &db_mutation_2, &TxId::zero(), 1000, 101);
        assert!(res.is_err());
    }

    #[test]
    fn db_store_run_composite_filter_query_test() {
        let tmp_dir_path =
            TempDir::new("db_store_run_composite_filter_query_test").expect("create temp dir");
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let collection_name = "db_store_run_composite_filter_query_test".to_string();
        let block_id: u64 = 1001;

        // create DB Test
        let addr = gen_address();
        let db_id = DbId::try_from((&addr, 1)).unwrap();
        let db_mutation = build_database_mutation_with_multi_key_index(
            &db_id.address(),
            collection_name.as_str(),
        );
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result =
            DbStore::apply_mutation(db_m, &addr, 1, &TxId::zero(), &db_mutation, block_id, 1);
        assert!(result.is_ok());

        // add 4 documents into collection
        let db_mutation = build_add_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![
                r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Mike",
            "age": 44,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Bill",
            "age": 44,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Bill",
            "age": 45,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "",
            "age": 45,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
            ],
        );

        // add document test
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let (_, ids) =
            DbStore::add_document(db_m, &addr, &TxId::zero(), &db_mutation, block_id, 2).unwrap();
        assert_eq!(5, ids.len());

        // test query with composite filter
        for (name, age, exp) in [
            // Select * from collection where name = "Bill" and age = 44
            ("Bill", 44, vec![("Bill", 44)]),
            // Select * from collection where name = "Bill" and age = 45
            ("Bill", 45, vec![("Bill", 45)]),
            // Select * from collection where name = "Bill" and age = 46
            ("Bill", 46, vec![]),
            // Select * from collection where name = "Mike" and age = 44
            ("Mike", 44, vec![("Mike", 44)]),
            // Select * from collection where name = "" and age = 45
            ("", 45, vec![("", 45)]),
            // Select * from collection where name = "" and age = 46
            ("", 46, vec![]),
        ] {
            let query = StructuredQuery {
                collection_name: collection_name.to_string(),
                select: Some(Projection { fields: vec![] }),
                r#where: Some(Filter {
                    filter_type: Some(FilterType::CompositeFilter(CompositeFilter {
                        filters: vec![
                            Filter {
                                filter_type: Some(FilterType::FieldFilter(FieldFilter {
                                    field: "name".to_string(),
                                    op: Operator::Equal.into(),
                                    value: Some(Value {
                                        value_type: Some(ValueType::StringValue(name.to_string())),
                                    }),
                                })),
                            },
                            Filter {
                                filter_type: Some(FilterType::FieldFilter(FieldFilter {
                                    field: "age".to_string(),
                                    op: Operator::Equal.into(),
                                    value: Some(Value {
                                        value_type: Some(ValueType::IntegerValue(age as i64)),
                                    }),
                                })),
                            },
                        ],
                        op: CompositeOp::And.into(),
                    })),
                }),
                limit: None,
            };
            let docs = DbStore::run_query(db.as_ref(), &db_id, &query).unwrap();
            assert_eq!(exp.len(), docs.len(), "run query fail for {:?}", query);
            for i in 0..exp.len() {
                let document = bson_util::bytes_to_bson_document(docs[i].doc.clone()).unwrap();
                assert_eq!(
                    exp[i].0,
                    document.get_str("name").unwrap(),
                    "run query fail for {:?}",
                    query
                );
                assert_eq!(
                    exp[i].1,
                    document.get_i64("age").unwrap(),
                    "run query fail for {:?}",
                    query
                );
            }
        }
    }
    #[test]
    fn db_store_run_composite_filter_query_wrong_path_test() {
        let tmp_dir_path = TempDir::new("db_store_run_composite_filter_query_wrong_path_test")
            .expect("create temp dir");
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let collection_name = "db_store_run_composite_filter_query_wrong_path_test".to_string();
        let block_id: u64 = 1001;

        // create DB Test
        let addr = gen_address();
        let db_id = DbId::try_from((&addr, 1)).unwrap();
        let db_mutation = build_database_mutation_with_multi_key_index(
            &db_id.address(),
            collection_name.as_str(),
        );
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result =
            DbStore::apply_mutation(db_m, &addr, 1, &TxId::zero(), &db_mutation, block_id, 1);
        assert!(result.is_ok());

        // add 4 documents into collection
        let db_mutation = build_add_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![
                r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Mike",
            "age": 44,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Bill",
            "age": 44,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Bill",
            "age": 45,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "",
            "age": 45,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
            ],
        );

        // add document test
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let (_, ids) =
            DbStore::add_document(db_m, &addr, &TxId::zero(), &db_mutation, block_id, 2).unwrap();
        assert_eq!(5, ids.len());

        // test query with composite filter
        for (name, name_op, age, age_op, composite_op, is_ok) in [
            // Select * from collection where name = "Bill" and age = 44
            (
                "Bill",
                Operator::Equal,
                44,
                Operator::Equal,
                CompositeOp::And,
                true,
            ),
            (
                "",
                Operator::Equal,
                45,
                Operator::Equal,
                CompositeOp::And,
                true,
            ),
            (
                "Bill",
                Operator::Equal,
                44,
                Operator::Equal,
                CompositeOp::Unspecified,
                false,
            ),
            (
                "Bill",
                Operator::LessThan,
                44,
                Operator::Equal,
                CompositeOp::And,
                false,
            ),
            (
                "Bill",
                Operator::Equal,
                44,
                Operator::LessThan,
                CompositeOp::And,
                false,
            ),
        ] {
            let query = StructuredQuery {
                collection_name: collection_name.to_string(),
                select: Some(Projection { fields: vec![] }),
                r#where: Some(Filter {
                    filter_type: Some(FilterType::CompositeFilter(CompositeFilter {
                        filters: vec![
                            Filter {
                                filter_type: Some(FilterType::FieldFilter(FieldFilter {
                                    field: "name".to_string(),
                                    op: name_op.into(),
                                    value: Some(Value {
                                        value_type: Some(ValueType::StringValue(name.to_string())),
                                    }),
                                })),
                            },
                            Filter {
                                filter_type: Some(FilterType::FieldFilter(FieldFilter {
                                    field: "age".to_string(),
                                    op: age_op.into(),
                                    value: Some(Value {
                                        value_type: Some(ValueType::IntegerValue(age as i64)),
                                    }),
                                })),
                            },
                        ],
                        op: composite_op.into(),
                    })),
                }),
                limit: None,
            };
            let res = DbStore::run_query(db.as_ref(), &db_id, &query);
            assert_eq!(is_ok, res.is_ok());
            println!("res: {:?}", res);
        }
    }

    #[test]
    fn db_store_run_simple_query_test() {
        let tmp_dir_path = TempDir::new("db_store_run_query_test").expect("create temp dir");
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let collection_name = "collection_run_query_test".to_string();
        let block_id: u64 = 1001;

        // create DB Test
        let addr = gen_address();
        let db_id = DbId::try_from((&addr, 1)).unwrap();
        let db_mutation = build_database_mutation(&db_id.address(), collection_name.as_str());
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result =
            DbStore::apply_mutation(db_m, &addr, 1, &TxId::zero(), &db_mutation, block_id, 1);
        assert!(result.is_ok());

        // add 4 documents into collection
        let db_mutation = build_add_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![
                r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Mike",
            "age": 44,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Bill",
            "age": 44,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Bill",
            "age": 45,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "",
            "age": 45,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
            ],
        );

        // add document test
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let (_, ids) =
            DbStore::add_document(db_m, &addr, &TxId::zero(), &db_mutation, block_id, 2).unwrap();
        assert_eq!(5, ids.len());

        // run query db not exist
        let query = StructuredQuery {
            collection_name: collection_name.to_string(),
            select: Some(Projection { fields: vec![] }),
            r#where: None,
            limit: None,
        };
        let db_id_not_exist = DbId::try_from((&addr, 999)).unwrap();
        let res = DbStore::run_query(db.as_ref(), &db_id_not_exist, &query);
        assert!(res.is_err(), "{:?}", res);

        // run query collection not exist
        let query = StructuredQuery {
            collection_name: "collection_not_exist".to_string(),
            select: Some(Projection { fields: vec![] }),
            r#where: None,
            limit: None,
        };
        let res = DbStore::run_query(db.as_ref(), &db_id, &query);
        assert!(res.is_err(), "{:?}", res);

        // run query: select * from collection
        let query = StructuredQuery {
            collection_name: collection_name.to_string(),
            select: Some(Projection { fields: vec![] }),
            r#where: None,
            limit: None,
        };
        let res = DbStore::run_query(db.as_ref(), &db_id, &query);
        assert!(res.is_ok(), "{:?}", res);
        assert_eq!(5, res.unwrap().len());

        // run query: select * from collection limit 2
        let query = StructuredQuery {
            collection_name: collection_name.to_string(),
            select: Some(Projection { fields: vec![] }),
            r#where: None,
            limit: Some(Limit { limit: 2 }),
        };
        let docs = DbStore::run_query(db.as_ref(), &db_id, &query).unwrap();
        assert_eq!(2, docs.len());

        // test query with ==, <, <=, >, >= condition
        for (field, value_str, op, exp) in [
            ("name", "", Operator::Equal, vec![""]),
            ("name", "Bill", Operator::Equal, vec!["Bill", "Bill"]),
            (
                "name",
                "John Doe",
                Operator::LessThan,
                vec!["", "Bill", "Bill"],
            ),
            (
                "name",
                "John Doe",
                Operator::LessThanOrEqual,
                vec!["", "Bill", "Bill", "John Doe"],
            ),
            (
                "name",
                "John Doe",
                Operator::GreaterThanOrEqual,
                vec!["John Doe", "Mike"],
            ),
            ("name", "John Doe", Operator::GreaterThan, vec!["Mike"]),
        ] {
            let query = StructuredQuery {
                collection_name: collection_name.to_string(),
                select: Some(Projection { fields: vec![] }),
                r#where: Some(Filter {
                    filter_type: Some(FilterType::FieldFilter(FieldFilter {
                        field: field.to_string(),
                        op: op.into(),
                        value: Some(Value {
                            value_type: Some(ValueType::StringValue(value_str.to_string())),
                        }),
                    })),
                }),
                limit: None,
            };
            let docs = DbStore::run_query(db.as_ref(), &db_id, &query).unwrap();
            assert_eq!(exp.len(), docs.len(), "run query fail for {:?}", query);
            for i in 0..exp.len() {
                let document = bson_util::bytes_to_bson_document(docs[i].doc.clone()).unwrap();
                assert_eq!(
                    exp[i],
                    document.get_str(field).unwrap(),
                    "run query fail for {:?}",
                    query
                );
            }
        }

        // run query: select * from collection where name = "Bill" limit 1
        let query = StructuredQuery {
            collection_name: collection_name.to_string(),
            select: Some(Projection { fields: vec![] }),
            r#where: Some(Filter {
                filter_type: Some(FilterType::FieldFilter(FieldFilter {
                    field: "name".to_string(),
                    op: Operator::Equal.into(),
                    value: Some(Value {
                        value_type: Some(ValueType::StringValue("Bill".to_string())),
                    }),
                })),
            }),
            limit: Some(Limit { limit: 1 }),
        };
        let docs = DbStore::run_query(db.as_ref(), &db_id, &query).unwrap();
        assert_eq!(1, docs.len());
        let document = bson_util::bytes_to_bson_document(docs[0].doc.clone()).unwrap();
        assert_eq!("Bill", document.get_str("name").unwrap());

        // run query: select * from collection where name = "Mike"
        let query = StructuredQuery {
            collection_name: collection_name.to_string(),
            select: Some(Projection { fields: vec![] }),
            r#where: Some(Filter {
                filter_type: Some(FilterType::FieldFilter(FieldFilter {
                    field: "name".to_string(),
                    op: Operator::Equal.into(),
                    value: Some(Value {
                        value_type: Some(ValueType::StringValue("Mike".to_string())),
                    }),
                })),
            }),
            limit: None,
        };
        let docs = DbStore::run_query(db.as_ref(), &db_id, &query).unwrap();
        assert_eq!(1, docs.len());
        let document = bson_util::bytes_to_bson_document(docs[0].doc.clone()).unwrap();
        assert_eq!("Mike", document.get_str("name").unwrap());

        // run query: select * from collection where age = 44
        let query = StructuredQuery {
            collection_name: collection_name.to_string(),
            select: Some(Projection { fields: vec![] }),
            r#where: Some(Filter {
                filter_type: Some(FilterType::FieldFilter(FieldFilter {
                    field: "age".to_string(),
                    op: Operator::Equal.into(),
                    value: Some(Value {
                        value_type: Some(ValueType::IntegerValue(44 as i64)),
                    }),
                })),
            }),
            limit: None,
        };
        let docs = DbStore::run_query(db.as_ref(), &db_id, &query).unwrap();
        assert_eq!(2, docs.len());
        let document = bson_util::bytes_to_bson_document(docs[0].doc.clone()).unwrap();
        assert_eq!(44, document.get_i64("age").unwrap());
        let document = bson_util::bytes_to_bson_document(docs[1].doc.clone()).unwrap();
        assert_eq!(44, document.get_i64("age").unwrap());
    }

    #[test]
    fn db_store_update_document_test() {
        let tmp_dir_path = TempDir::new("db_store_update_document_test").expect("create temp dir");
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let collection_name = "collection_update_document_test".to_string();
        let block_id: u64 = 1002;
        let mut mutation_id: u16 = 1;

        // create DB Test
        let addr = gen_address();
        let db_id = DbId::try_from((&addr, 1)).unwrap();
        let db_mutation = build_database_mutation(&db_id.address(), collection_name.as_str());
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = DbStore::apply_mutation(
            db_m,
            &addr,
            1,
            &TxId::zero(),
            &db_mutation,
            block_id,
            mutation_id,
        );
        mutation_id += 1;
        assert!(result.is_ok());

        // add 4 documents into collection
        let db_mutation = build_add_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![
                r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Mike",
            "age": 44,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Bill",
            "age": 44,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
                r#"
        {
            "name": "Bill",
            "age": 45,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string(),
            ],
        );

        // add document test
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let (_, ids) = DbStore::add_document(
            db_m,
            &addr,
            &TxId::zero(),
            &db_mutation,
            block_id,
            mutation_id,
        )
        .unwrap();
        mutation_id += 1;
        assert_eq!(4, ids.len());

        // update document - no index key modified
        let db_mutation = build_update_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![ids[0].to_base64()],
            vec![r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+1234567",
                "+2345678"
            ]
        }"#
            .to_string()],
            vec![vec![
                "name".to_string(),
                "age".to_string(),
                "phones".to_string(),
            ]],
        );
        // update document test
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let store_ops = DbStore::update_document(
            db_m,
            &addr,
            &TxId::zero(),
            &db_mutation,
            block_id,
            mutation_id,
        )
        .unwrap();
        mutation_id += 1;
        assert_eq!(
            DbStoreOp::DocOp {
                add_doc_ops: 0,
                del_doc_ops: 0,
                update_doc_ops: 1,
                data_in_bytes: 214,
            },
            store_ops
        );
        let document = DbStore::get_document(db.as_ref(), &ids[0])
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"Document({"name": String("John Doe"), "age": Int64(43), "phones": Array([String("+1234567"), String("+2345678")])})"#,
            format!(
                "{:?}",
                bson_util::bytes_to_bson_document(document.doc).unwrap()
            )
        );

        // update document - name related index update
        let db_mutation = build_update_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![ids[0].to_base64()],
            vec![r#"
        {
            "name": "Bill"
        }"#
            .to_string()],
            vec![vec!["name".to_string()]],
        );
        // update document test
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let store_ops = DbStore::update_document(
            db_m,
            &addr,
            &TxId::zero(),
            &db_mutation,
            block_id,
            mutation_id,
        )
        .unwrap();
        mutation_id += 1;
        assert_eq!(
            DbStoreOp::DocOp {
                add_doc_ops: 0,
                del_doc_ops: 0,
                update_doc_ops: 3,
                data_in_bytes: 210,
            },
            store_ops
        );
        let document = DbStore::get_document(db.as_ref(), &ids[0])
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"Document({"name": String("Bill"), "age": Int64(43), "phones": Array([String("+1234567"), String("+2345678")])})"#,
            format!(
                "{:?}",
                bson_util::bytes_to_bson_document(document.doc).unwrap()
            )
        );
        // update document - name and age related index update
        let db_mutation = build_update_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![ids[0].to_base64()],
            vec![r#"
        {
            "name": "Mike",
            "age": 44
        }"#
            .to_string()],
            vec![vec!["name".to_string(), "age".to_string()]],
        );
        // update document test
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let store_ops = DbStore::update_document(
            db_m,
            &addr,
            &TxId::zero(),
            &db_mutation,
            block_id,
            mutation_id,
        )
        .unwrap();
        mutation_id += 1;
        assert_eq!(
            DbStoreOp::DocOp {
                add_doc_ops: 0,
                del_doc_ops: 0,
                update_doc_ops: 5,
                data_in_bytes: 210,
            },
            store_ops
        );
        let document = DbStore::get_document(db.as_ref(), &ids[0])
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"Document({"name": String("Mike"), "age": Int64(44), "phones": Array([String("+1234567"), String("+2345678")])})"#,
            format!(
                "{:?}",
                bson_util::bytes_to_bson_document(document.doc).unwrap()
            )
        );

        // update document - delete age and update phone
        let db_mutation = build_update_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![ids[0].to_base64()],
            vec![r#"
        {
            "phones": [
                "+86 1234567",
                "+86 2345678"
            ]
        }"#
            .to_string()],
            vec![vec!["phones".to_string(), "age".to_string()]],
        );
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let store_ops = DbStore::update_document(
            db_m,
            &addr,
            &TxId::zero(),
            &db_mutation,
            block_id,
            mutation_id,
        )
        .unwrap();
        mutation_id += 1;
        assert_eq!(
            DbStoreOp::DocOp {
                add_doc_ops: 0,
                del_doc_ops: 0,
                update_doc_ops: 2, // update doc entry, remove age index
                data_in_bytes: 203,
            },
            store_ops
        );
        let document = DbStore::get_document(db.as_ref(), &ids[0])
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"Document({"name": String("Mike"), "phones": Array([String("+86 1234567"), String("+86 2345678")])})"#,
            format!(
                "{:?}",
                bson_util::bytes_to_bson_document(document.doc).unwrap()
            )
        );

        // update document - empty update doc, delete phones
        let db_mutation = build_update_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![ids[0].to_base64()],
            vec![r#"
        {
        }"#
            .to_string()],
            vec![vec!["phones".to_string()]],
        );
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let store_ops = DbStore::update_document(
            db_m,
            &addr,
            &TxId::zero(),
            &db_mutation,
            block_id,
            mutation_id,
        )
        .unwrap();
        mutation_id += 1;
        assert_eq!(
            DbStoreOp::DocOp {
                add_doc_ops: 0,
                del_doc_ops: 0,
                update_doc_ops: 1, // update doc entry
                data_in_bytes: 152,
            },
            store_ops
        );
        let document = DbStore::get_document(db.as_ref(), &ids[0])
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"Document({"name": String("Mike")})"#,
            format!(
                "{:?}",
                bson_util::bytes_to_bson_document(document.doc).unwrap()
            )
        );

        // update document - empty update doc, delete phones
        let db_mutation = build_update_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![ids[0].to_base64()],
            vec![r#"
        {
        }"#
            .to_string()],
            vec![vec![]],
        );
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let store_ops = DbStore::update_document(
            db_m,
            &addr,
            &TxId::zero(),
            &db_mutation,
            block_id,
            mutation_id,
        )
        .unwrap();
        mutation_id += 1;
        assert_eq!(
            DbStoreOp::DocOp {
                add_doc_ops: 0,
                del_doc_ops: 0,
                update_doc_ops: 0, // update doc entry
                data_in_bytes: 0,
            },
            store_ops
        );
        let document = DbStore::get_document(db.as_ref(), &ids[0])
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"Document({"name": String("Mike")})"#,
            format!(
                "{:?}",
                bson_util::bytes_to_bson_document(document.doc).unwrap()
            )
        );
        // update document, id not exist
        let id_not_exist = DocumentId::zero().to_base64();
        let db_mutation = build_update_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![id_not_exist],
            vec![r#"
        {
            "name": "Mike",
            "age": 44,
            "phones": [
                "+1234567",
                "+2345678"
            ]
        }"#
            .to_string()],
            vec![vec![
                "name".to_string(),
                "age".to_string(),
                "phones".to_string(),
            ]],
        );
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let res = DbStore::update_document(
            db_m,
            &addr,
            &TxId::zero(),
            &db_mutation,
            block_id,
            mutation_id,
        );
        assert!(res.is_err(), "{:?}", res);
        assert_eq!(
            r#"DocumentNotExist("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==")"#,
            format!("{:?}", res.err().unwrap())
        );
        mutation_id += 1;

        // update document, id not exist
        let id_not_exist = DocumentId::zero().to_base64();
        let db_mutation = build_update_document_mutation(
            db_id.address(),
            collection_name.as_str(),
            vec![id_not_exist],
            vec![r#"
        {
            "name": "Mike",
            "age": 44,
            "phones": [
                "+1234567",
                "+2345678"
            ]
        }"#
            .to_string()],
            vec![],
        );
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let res = DbStore::update_document(
            db_m,
            &addr,
            &TxId::zero(),
            &db_mutation,
            block_id,
            mutation_id,
        );
        assert!(res.is_err(), "{:?}", res);
        assert_eq!(
            r#"ApplyDocumentError("invalid update document mutation, ids and masks size different")"#,
            format!("{:?}", res.err().unwrap())
        );
    }

    #[test]
    fn db_store_create_database_happy_path() {
        let tmp_dir_path =
            TempDir::new("db_store_create_database_happy_path").expect("create temp dir");
        let addr = gen_address();
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let collection_name = "db_store_create_database_happy_path".to_string();
        let db_mutation =
            build_database_mutation_with_multi_key_index(&addr, collection_name.as_str());
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);

        // create DB wrong path
        let result = DbStore::apply_mutation(db_m, &addr, 1, &TxId::zero(), &db_mutation, 1000, 1);
        assert!(result.is_ok());
    }

    #[test]
    fn db_store_smoke_test() {
        let tmp_dir_path = TempDir::new("db_store_test").expect("create temp dir");
        let addr = gen_address();
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let collection_name = "db_store_smoke_test".to_string();
        let db_mutation = build_database_mutation(&addr, collection_name.as_str());
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);

        // create DB Test
        let result = DbStore::apply_mutation(db_m, &addr, 1, &TxId::zero(), &db_mutation, 1000, 1);
        assert!(result.is_ok());

        // get database test
        let db_id = DbId::try_from((&addr, 1)).unwrap();
        if let Ok(Some(res)) = DbStore::get_database(db.as_ref(), &db_id) {
            assert_eq!(1, res.collections.len());
            assert!(res
                .collections
                .iter()
                .find(|x| x.name.as_str() == collection_name.as_str())
                .is_some());
            let collection = res
                .collections
                .iter()
                .find(|x| x.name.as_str() == collection_name.as_str())
                .unwrap();
            let collection_id = CollectionId::try_from_bytes(collection.id.as_slice()).unwrap();
            let db_mutation = build_add_document_mutation(
                db_id.address(),
                collection.name.as_str(),
                vec![r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                .to_string()],
            );

            // add document test
            let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
            let (_, ids) =
                DbStore::add_document(db_m, &addr, &TxId::zero(), &db_mutation, 1000, 2).unwrap();
            assert_eq!(1, ids.len());
            let document_id_1 = ids[0];

            // get document test
            let res = DbStore::get_document(db.as_ref(), &document_id_1);
            if let Ok(Some(document)) = res {
                assert_eq!(
                    r#"Document({"name": String("John Doe"), "age": Int64(43), "phones": Array([String("+44 1234567"), String("+44 2345678")])})"#,
                    format!(
                        "{:?}",
                        bson_util::bytes_to_bson_document(document.doc).unwrap()
                    )
                );
                assert_eq!(document_id_1.as_ref(), document.id);
                assert_eq!(addr.to_vec(), document.owner)
            } else {
                assert!(false);
            }

            // insert two documents
            let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
            let db_mutation = build_add_document_mutation(
                db_id.address(),
                collection.name.as_str(),
                vec![
                    r#"
        {
            "name": "Mike",
            "age": 44,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                    .to_string(),
                    r#"
        {
            "name": "Bob",
            "age": 45,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#
                    .to_string(),
                ],
            );
            let (_, ids) =
                DbStore::add_document(db_m, &addr, &TxId::zero(), &db_mutation, 1000, 3).unwrap();
            assert_eq!(2, ids.len());
            let document_id_2 = ids[0];
            let document_id_3 = ids[1];

            // show documents
            if let Ok(documents) = DbStore::get_documents(db.as_ref(), &collection_id, None) {
                assert_eq!(3, documents.len());
            } else {
                assert!(false);
            }

            // show documents
            if let Ok(documents) = DbStore::get_documents(db.as_ref(), &collection_id, Some(2)) {
                assert_eq!(2, documents.len());
            } else {
                assert!(false);
            }

            // show documents
            if let Ok(documents) = DbStore::get_documents(db.as_ref(), &collection_id, Some(2)) {
                assert_eq!(2, documents.len());
            } else {
                assert!(false);
            }

            // delete document
            let db_mutation = build_delete_document_mutation(
                db_id.address(),
                &collection_name,
                vec![document_id_2.to_base64(), document_id_3.to_base64()],
            );
            let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
            let res = DbStore::delete_document(db_m, &addr, &db_mutation);
            assert!(res.is_ok());

            // show documents
            if let Ok(documents) = DbStore::get_documents(db.as_ref(), &collection_id, None) {
                assert_eq!(1, documents.len());
            } else {
                assert!(false);
            }

            assert!(DbStore::get_document(db.as_ref(), &document_id_2)
                .unwrap()
                .is_none());
            assert!(DbStore::get_document(db.as_ref(), &document_id_3)
                .unwrap()
                .is_none());
            assert!(DbStore::get_document(db.as_ref(), &document_id_1)
                .unwrap()
                .is_some());
        } else {
            assert!(false);
        }
    }

    #[test]
    fn generate_range_with_single_field_filter_test() {
        let collection_id = CollectionId::create(1000, 100, 10).unwrap();

        let index_field_name = IndexField {
            field_path: "name".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };

        let index_name = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![index_field_name],
        };
        let key = FieldKey::create_single_key(Some(Bson::String("Bill".to_string()))).unwrap();

        let res = DbStore::generate_range_with_single_field_filter(
            &collection_id,
            &index_name,
            &key.as_ref(),
            Some(Operator::Equal),
        );
        assert!(res.is_ok(), "{:?}", res);
        let (start, end) = res.unwrap();
        // range [collection-index-key-00000000, collection-index-key-11111111]
        match start {
            Included(start) => {
                assert_eq!(start.get_document_id().unwrap(), DocumentId::zero());
            }
            _ => {
                assert!(false);
            }
        }
        match end {
            Included(start) => {
                assert_eq!(start.get_document_id().unwrap(), DocumentId::one());
            }
            _ => {
                assert!(false);
            }
        }

        let res = DbStore::generate_range_with_single_field_filter(
            &collection_id,
            &index_name,
            &key.as_ref(),
            Some(Operator::GreaterThan),
        );
        assert!(res.is_ok(), "{:?}", res);
        let (start, end) = res.unwrap();
        // range (collection-index-key-11111111, collection-next_index-0-00000000)
        match start {
            Excluded(start) => {
                assert_eq!(start.get_document_id().unwrap(), DocumentId::one());
            }
            _ => {
                assert!(false);
            }
        }
        match end {
            Excluded(end) => {
                assert_eq!(end.get_document_id().unwrap(), DocumentId::zero());
                assert_eq!(end.get_index_field_id(), index_name.id + 1);
            }
            _ => {
                assert!(false);
            }
        }

        let res = DbStore::generate_range_with_single_field_filter(
            &collection_id,
            &index_name,
            &key.as_ref(),
            Some(Operator::GreaterThanOrEqual),
        );
        assert!(res.is_ok(), "{:?}", res);
        let (start, end) = res.unwrap();
        // range [collection-index-key-00000000, collection-next_index-0-00000000)
        match start {
            Included(start) => {
                assert_eq!(start.get_document_id().unwrap(), DocumentId::zero());
            }
            _ => {
                assert!(false);
            }
        }
        match end {
            Excluded(end) => {
                assert_eq!(end.get_document_id().unwrap(), DocumentId::zero());
                assert_eq!(end.get_index_field_id(), index_name.id + 1);
            }
            _ => {
                assert!(false);
            }
        }

        let res = DbStore::generate_range_with_single_field_filter(
            &collection_id,
            &index_name,
            &key.as_ref(),
            Some(Operator::LessThan),
        );
        assert!(res.is_ok(), "{:?}", res);
        let (start, end) = res.unwrap();
        // range [collection-index-0-00000000, collection-index-key-00000000)
        match end {
            Excluded(end) => {
                assert_eq!(end.get_document_id().unwrap(), DocumentId::zero());
            }
            _ => {
                assert!(false);
            }
        }

        match start {
            Included(start) => {
                assert_eq!(start.get_document_id().unwrap(), DocumentId::zero());
            }
            _ => {
                assert!(false);
            }
        }

        let res = DbStore::generate_range_with_single_field_filter(
            &collection_id,
            &index_name,
            &key.as_ref(),
            Some(Operator::LessThanOrEqual),
        );
        assert!(res.is_ok(), "{:?}", res);
        let (start, end) = res.unwrap();
        // range [collection-index-0-00000000, collection-index-key-11111111]
        match end {
            Included(end) => {
                assert_eq!(end.get_document_id().unwrap(), DocumentId::one());
            }
            _ => {
                assert!(false);
            }
        }
        match start {
            Included(start) => {
                assert_eq!(start.get_document_id().unwrap(), DocumentId::zero());
            }
            _ => {
                assert!(false);
            }
        }
    }
}
