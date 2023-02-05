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
use bytes::BytesMut;
use db3_crypto::{
    db3_address::DB3Address, db3_document::DB3Document, id::DbId, id::DocumentId, id::TxId,
};
use db3_error::{DB3Error, Result};
use db3_proto::db3_database_proto::Index;
use db3_proto::db3_database_proto::{Collection, Database};
use db3_proto::db3_mutation_proto::{DatabaseAction, DatabaseMutation, DocumentMutation};
use merkdb::proofs::{query::Query, Op as ProofOp};
use merkdb::{BatchEntry, Merk, Op};
use prost::Message;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::ops::Range;
use std::pin::Pin;
use tracing::warn;

pub struct DbStore {}

impl DbStore {
    pub fn new() -> Self {
        Self {}
    }

    fn update_database(
        old_db: &Database,
        mutation: &DatabaseMutation,
        tx_id: &TxId,
    ) -> Result<Database> {
        let collection_ids: HashSet<String> =
            HashSet::from_iter(old_db.collections.iter().map(|x| x.name.to_string()));
        let new_collections: Vec<Collection> = mutation
            .collection_mutations
            .iter()
            .filter(|x| !collection_ids.contains(&x.collection_id))
            .map(|x| Collection {
                name: x.collection_id.to_string(),
                index_list: x.index.to_vec(),
            })
            .collect();
        if new_collections.len() != mutation.collection_mutations.len() {
            Err(DB3Error::ApplyDatabaseError(
                "duplicated collection names".to_string(),
            ))
        } else {
            let mut collections = old_db.collections.to_vec();
            collections.extend_from_slice(new_collections.as_ref());
            let mut tx_list = old_db.tx.to_vec();
            tx_list.push(tx_id.as_ref().to_vec());
            Ok(Database {
                address: old_db.address.to_vec(),
                sender: old_db.sender.to_vec(),
                tx: tx_list,
                collections,
            })
        }
    }

    fn new_database(
        id: &DbId,
        sender: &DB3Address,
        txid: &TxId,
        mutation: &DatabaseMutation,
    ) -> Database {
        //TODO check the duplicated collection id
        let collections: Vec<Collection> = mutation
            .collection_mutations
            .iter()
            .map(move |x| Collection {
                name: x.collection_id.to_string(),
                index_list: x.index.to_vec(),
            })
            .collect();

        Database {
            address: id.as_ref().to_vec(),
            sender: sender.as_ref().to_vec(),
            tx: vec![txid.as_ref().to_vec()],
            collections,
        }
    }

    fn convert(
        sender: &DB3Address,
        nonce: u64,
        tx: &TxId,
        mutation: &DatabaseMutation,
    ) -> Result<(BatchEntry, usize)> {
        let dbid = DbId::try_from((sender, nonce))?;
        let db = Self::new_database(&dbid, sender, tx, mutation);
        Self::encode_database(dbid, &db)
    }

    fn encode_database(dbid: DbId, database: &Database) -> Result<(BatchEntry, usize)> {
        let key = DbKey(dbid);
        let encoded_key = key.encode()?;
        let mut buf = BytesMut::with_capacity(1024 * 2);
        database
            .encode(&mut buf)
            .map_err(|e| DB3Error::ApplyDatabaseError(format!("{e}")))?;
        let buf = buf.freeze();
        let total_in_bytes = encoded_key.len() + buf.as_ref().len();
        Ok((
            (encoded_key, Op::Put(buf.as_ref().to_vec())),
            total_in_bytes,
        ))
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
    ) -> Result<()> {
        let mut entries: Vec<BatchEntry> = Vec::new();
        let (batch_entry, _) = Self::convert(sender, nonce, tx, mutation)?;
        entries.push(batch_entry);
        unsafe {
            Pin::get_unchecked_mut(db)
                .apply(&entries, &[])
                .map_err(|e| DB3Error::ApplyDatabaseError(format!("{e}")))?;
        }
        Ok(())
    }

    //
    // add a new collection to database
    //
    fn add_collection(
        db: Pin<&mut Merk>,
        sender: &DB3Address,
        tx: &TxId,
        mutation: &DatabaseMutation,
    ) -> Result<()> {
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
                } else {
                    let mut entries: Vec<BatchEntry> = Vec::new();
                    let new_db = Self::update_database(&d, mutation, tx)?;
                    let (entry, _) = Self::encode_database(db_id, &new_db)?;
                    entries.push(entry);
                    unsafe {
                        Pin::get_unchecked_mut(db)
                            .apply(&entries, &[])
                            .map_err(|e| DB3Error::ApplyDatabaseError(format!("{e}")))?;
                    }
                }
            }
            None => {
                warn!("database not found with addr {}", db_id.to_hex());
            }
        }
        Ok(())
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
        mutation_id: u32,
    ) -> Result<()> {
        let addr_ref: &[u8] = mutation.db_address.as_ref();
        let db_id = DbId::try_from(addr_ref)?;
        let database = Self::get_database(db.as_ref(), &db_id)?;
        match database {
            Some(d) => {
                let mut entries: Vec<BatchEntry> = Vec::new();
                let mut idx = 0;
                let mut cid_index_map: HashMap<String, _> = HashMap::new();
                for collection in d.collections.iter() {
                    cid_index_map.insert(collection.name.to_string(), &collection.index_list);
                }
                for document_mutation in &mutation.document_mutations {
                    if let Some(index_list) = cid_index_map.get(&document_mutation.collection_id) {
                        for document in document_mutation.document.iter() {
                            let document_id = DocumentId::create(block_id, mutation_id, idx)
                                .map_err(|e| DB3Error::ApplyDatabaseError(format!("{:?}", e)))
                                .unwrap();
                            let db3_document =
                                DB3Document::new(document.clone(), &document_id, &tx, &sender);
                            let key_vec = document_id.as_ref().to_vec();
                            let document_vec = db3_document.into_bytes().to_vec();
                            idx += 1;
                            entries.push((key_vec, Op::Put(document_vec)));
                            // TODO(chenjing): add key for document with given index_list
                        }
                    }
                }
                unsafe {
                    Pin::get_unchecked_mut(db)
                        .apply(&entries, &[])
                        .map_err(|e| DB3Error::ApplyDatabaseError(format!("{:?}", e)))?;
                }
            }
            None => {
                warn!("database not found with addr {}", db_id.to_hex());
            }
        }
        Ok(())
    }
    //
    // add document
    //
    fn get_document(db: Pin<&mut Merk>, documentId: &DocumentId) -> Result<Option<Vec<u8>>> {
        //TODO use reference
        let value = db
            .get(documentId.as_ref())
            .map_err(|e| DB3Error::QueryDocumentError(format!("{e}")))?;
        Ok(value)
    }
    pub fn apply_mutation(
        db: Pin<&mut Merk>,
        sender: &DB3Address,
        nonce: u64,
        tx: &TxId,
        mutation: &DatabaseMutation,
        block_id: u64,
        mutation_id: u32,
    ) -> Result<()> {
        let action = DatabaseAction::from_i32(mutation.action);
        match action {
            Some(DatabaseAction::CreateDb) => {
                Self::create_database(db, sender, nonce, tx, mutation)
            }
            Some(DatabaseAction::AddCollection) => Self::add_collection(db, sender, tx, mutation),
            Some(DatabaseAction::AddDocument) => {
                Self::add_document(db, sender, tx, mutation, block_id, mutation_id)
            }
            None => Ok(()),
        }
    }
    pub fn get_database(db: Pin<&Merk>, id: &DbId) -> Result<Option<Database>> {
        //TODO use reference
        let key = DbKey(id.clone());
        let encoded_key = key.encode()?;
        let value = db
            .get(encoded_key.as_ref())
            .map_err(|e| DB3Error::QueryDatabaseError(format!("{e}")))?;
        if let Some(v) = value {
            match Database::decode(v.as_ref()) {
                Ok(database) => Ok(Some(database)),
                Err(e) => Err(DB3Error::QueryDatabaseError(format!("{e}"))),
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_databases(db: Pin<&Merk>) -> Result<LinkedList<ProofOp>> {
        let start_key = DbKey::min();
        let end_key = DbKey::max();
        let range = Range {
            start: start_key.encode()?,
            end: end_key.encode()?,
        };
        let mut query = Query::new();
        query.insert_range(range);
        let ops = db
            .execute_query(query)
            .map_err(|e| DB3Error::QueryDatabaseError(format!("{e}")))?;
        Ok(ops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_crypto::key_derive;
    use db3_crypto::signature_scheme::SignatureScheme;
    use db3_proto::db3_database_proto::{
        index::index_field::{Order, ValueMode},
        index::IndexField,
        Index,
    };
    use db3_proto::db3_mutation_proto::CollectionMutation;
    use std::boxed::Box;
    use tempdir::TempDir;

    fn gen_address() -> DB3Address {
        let seed: [u8; 32] = [0; 32];
        let (address, _) =
            key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::ED25519).unwrap();
        address
    }

    fn build_document_mutation(addr: &DB3Address) -> DatabaseMutation {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let document = DB3Document::try_from(data).unwrap();
        let document_mutations = vec![DocumentMutation {
            collection_id: "collection1".to_string(),
            document: vec![document.into_bytes()],
        }];
        let dm = DatabaseMutation {
            meta: None,
            collection_mutations: vec![],
            document_mutations,
            db_address: addr.to_vec(),
            action: DatabaseAction::CreateDb.into(),
        };
        let json_data = serde_json::to_string(&dm).unwrap();
        println!("{json_data}");
        dm
    }
    fn build_database_mutation(addr: &DB3Address) -> DatabaseMutation {
        let index_field = IndexField {
            field_path: "test1".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };

        let index = Index {
            name: "idx1".to_string(),
            fields: vec![index_field],
        };

        let index_mutation = CollectionMutation {
            index: vec![index],
            collection_id: "collection1".to_string(),
        };

        let dm = DatabaseMutation {
            meta: None,
            collection_mutations: vec![index_mutation],
            document_mutations: vec![],
            db_address: addr.to_vec(),
            action: DatabaseAction::CreateDb.into(),
        };
        let json_data = serde_json::to_string(&dm).unwrap();
        println!("{json_data}");
        dm
    }

    #[test]
    fn db_store_smoke_test() {
        let tmp_dir_path = TempDir::new("db_store_test").expect("create temp dir");
        let addr = gen_address();
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let db_mutation = build_database_mutation(&addr);
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = DbStore::apply_mutation(db_m, &addr, 1, &TxId::zero(), &db_mutation, 1000, 1);
        assert!(result.is_ok());
        if let Ok(ops) = DbStore::get_databases(db.as_ref()) {
            assert_eq!(1, ops.len());
        } else {
            assert!(false);
        }

        let dbId = DbId::try_from((&addr, 1)).unwrap();
        if let Ok(Some(res)) = DbStore::get_database(db.as_ref(), &dbId) {
            assert_eq!(1, res.collections.len());
            println!("{:?}", res);
        } else {
            assert!(false);
        }
        let db_mutation = build_document_mutation(dbId.address());
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        assert!(DbStore::add_document(db_m, &addr, &TxId::zero(), &db_mutation, 1000, 2).is_ok());

        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let document_id = DocumentId::create(1000, 2, 0).unwrap();
        if let Ok(Some(document_vec)) = DbStore::get_document(db_m, &document_id) {
            let db3_document = DB3Document::try_from(document_vec).unwrap();
            assert_eq!("John Doe", db3_document.as_ref().get_str("name").unwrap());
            assert_eq!(addr.to_vec(), db3_document.get_owner().unwrap().to_vec());
        } else {
            assert!(false);
        }
    }
}
