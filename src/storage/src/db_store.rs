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
use db3_crypto::{db3_address::DB3Address, id::DbId, id::TxId};
use db3_error::{DB3Error, Result};
use db3_proto::db3_database_proto::{Collection, Database};
use db3_proto::db3_mutation_proto::DatabaseMutation;
use merkdb::proofs::{query::Query, Op as ProofOp};
use merkdb::{BatchEntry, Merk, Op};
use prost::Message;
use std::collections::LinkedList;
use std::ops::Range;
use std::pin::Pin;

pub struct DbStore {}

impl DbStore {
    pub fn new() -> Self {
        Self {}
    }

    fn from(id: &DbId, sender: &DB3Address, txid: &TxId, mutation: &DatabaseMutation) -> Database {
        //TODO check the duplicated collection id
        let collections: Vec<Collection> = mutation
            .index_mutations
            .iter()
            .map(move |x| Collection {
                name: x.collection_id.to_string(),
                index_list: x.index.to_vec(),
            })
            .collect();
        Database {
            address: id.as_ref().to_vec(),
            sender: sender.as_ref().to_vec(),
            tx: txid.as_ref().to_vec(),
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
        let key = DbKey(dbid);
        let encoded_key = key.encode()?;
        let db = Self::from(&dbid, sender, tx, mutation);
        //TODO limit the key length
        let mut buf = BytesMut::with_capacity(1024 * 2);
        db.encode(&mut buf)
            .map_err(|e| DB3Error::ApplyDatabaseError(format!("{e}")))?;
        let buf = buf.freeze();
        let total_in_bytes = encoded_key.len() + buf.as_ref().len();
        Ok((
            (encoded_key, Op::Put(buf.as_ref().to_vec())),
            total_in_bytes,
        ))
    }

    pub fn apply_mutation(
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

    pub fn get_databases(db: Pin<&Merk>, addr: &DB3Address) -> Result<LinkedList<ProofOp>> {
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
    use db3_proto::db3_base_proto::{Erc20Token, Price};
    use db3_proto::db3_database_proto::QueryPrice;
    use std::boxed::Box;
    use tempdir::TempDir;

    fn gen_address() -> DB3Address {
        let seed: [u8; 32] = [0; 32];
        let (address, _) =
            key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::ED25519).unwrap();
        address
    }

    #[test]
    fn db_store_smoke_test() {
        let tmp_dir_path = TempDir::new("assign_partition").expect("create temp dir");
        let addr = gen_address();
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let ns = Database {
            name: "test1".to_string(),
            price: Some(query_price),
            ts: 1000,
            description: "test".to_string(),
        };

        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = DbStore::apply_add(db_m, &addr, &ns);
        assert!(result.is_ok());
        if let Ok(ops) = DbStore::get_databases(db.as_ref(), &addr) {
            assert_eq!(1, ops.len());
        } else {
            assert!(false);
        }
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = DbStore::apply_del(db_m, &addr, "test1");
        assert!(result.is_ok());
        let result = DbStore::get_databases(db.as_ref(), &addr);
        assert!(result.is_err());
    }
}
