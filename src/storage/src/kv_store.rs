//
// kv_store.rs
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

use super::key::Key;
use db3_error::{DB3Error, Result};
use db3_proto::db3_base_proto::Units;
use db3_proto::db3_mutation_proto::{KvPair, Mutation, MutationAction};
use db3_proto::db3_node_proto::{BatchGetKey, RangeKey};
use db3_types::cost;
use ethereum_types::Address as AccountAddress;
use merkdb::proofs::{query::Query, Op as ProofOp};
use merkdb::{BatchEntry, Merk, Op};
use std::collections::HashSet;
use std::collections::LinkedList;
use std::pin::Pin;

pub struct KvStore {}
impl KvStore {
    pub fn new() -> Self {
        Self {}
    }

    pub fn is_valid(mutation: &Mutation) -> bool {
        if mutation.ns.len() <= 0 {
            return false;
        }
        let mut keys: HashSet<&[u8]> = HashSet::new();
        for ref kv in &mutation.kv_pairs {
            if keys.contains(&kv.key.as_ref()) {
                return false;
            }
            keys.insert(kv.key.as_ref());
        }
        return true;
    }

    fn convert(
        kp: &KvPair,
        account_addr: &AccountAddress,
        ns: &[u8],
    ) -> Result<(BatchEntry, usize)> {
        let key = Key(*account_addr, ns, kp.key.as_ref());
        let encoded_key = key.encode()?;
        let action = MutationAction::from_i32(kp.action);
        match action {
            Some(MutationAction::InsertKv) => {
                //TODO avoid copying operation
                let total_in_bytes = encoded_key.len() + kp.value.len();
                Ok(((encoded_key, Op::Put(kp.value.to_vec())), total_in_bytes))
            }
            Some(MutationAction::DeleteKv) => Ok(((encoded_key, Op::Delete), 0)),
            Some(MutationAction::Nonce) => todo!(),
            None => Err(DB3Error::ApplyMutationError(
                "invalid action type".to_string(),
            )),
        }
    }

    pub fn apply(
        db: Pin<&mut Merk>,
        account_addr: &AccountAddress,
        mutation: &Mutation,
    ) -> Result<(Units, usize)> {
        let ns = mutation.ns.as_ref();
        //TODO avoid copying operation
        let mut ordered_kv_pairs = mutation.kv_pairs.to_vec();
        ordered_kv_pairs.sort_by(|a, b| a.key.cmp(&b.key));
        let mut entries: Vec<BatchEntry> = Vec::new();
        let mut total_in_bytes: usize = 0;
        for kv in ordered_kv_pairs {
            let (batch_entry, bytes) = Self::convert(&kv, account_addr, ns)?;
            total_in_bytes += bytes;
            entries.push(batch_entry);
        }
        let gas = cost::estimate_gas(mutation);
        unsafe {
            Pin::get_unchecked_mut(db)
                .apply(&entries, &[])
                .map_err(|e| DB3Error::ApplyMutationError(format!("{}", e)))?;
        }
        Ok((gas, total_in_bytes))
    }

    pub fn batch_get(
        db: Pin<&Merk>,
        account_addr: &AccountAddress,
        batch_get_keys: &BatchGetKey,
    ) -> Result<LinkedList<ProofOp>> {
        let mut query = Query::new();
        //
        // return directly if no keys
        // TODO add limit to the keys length
        //
        if batch_get_keys.keys.len() == 0 {
            return Ok(LinkedList::new());
        }

        for k in &batch_get_keys.keys {
            let key = Key(*account_addr, batch_get_keys.ns.as_ref(), k.as_ref());
            let encoded_key = key.encode()?;
            query.insert_key(encoded_key);
        }
        let ops = db
            .execute_query(query)
            .map_err(|e| DB3Error::QueryKvError(format!("{}", e)))?;
        Ok(ops)
    }

    pub fn get_range(
        db: Pin<&Merk>,
        account_addr: &AccountAddress,
        range_key: &RangeKey,
    ) -> Result<LinkedList<ProofOp>> {
        let mut query = Query::new();
        match &range_key.range {
            Some(range) => {
                if range.start.cmp(&range.end) < std::cmp::Ordering::Less {
                    return Err(DB3Error::QueryKvError("bad range order".to_string()));
                }
                let start_key =
                    Key(*account_addr, range_key.ns.as_ref(), range.start.as_ref()).encode()?;
                let end_key =
                    Key(*account_addr, range_key.ns.as_ref(), range.end.as_ref()).encode()?;
                let std_range = std::ops::Range {
                    start: start_key,
                    end: end_key,
                };
                query.insert_range(std_range);
                let ops = db
                    .execute_query(query)
                    .map_err(|e| DB3Error::QueryKvError(format!("{}", e)))?;
                Ok(ops)
            }
            None => Err(DB3Error::QueryKvError("bad input range key".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_base::get_a_static_address;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_node_proto::Range as DB3Range;
    use merkdb::proofs::Node;
    use std::boxed::Box;
    use tempdir::TempDir;

    #[test]
    fn test_range_empty() {
        let tmp_dir_path = TempDir::new("get range").expect("create temp dir");
        let addr = get_a_static_address();
        let merk = Merk::open(tmp_dir_path).unwrap();
        let db = Box::pin(merk);
        let range = DB3Range {
            start: "k0".as_bytes().to_vec(),
            end: "k4".as_bytes().to_vec(),
        };
        let ns: &str = "my_twitter";
        let range_key = RangeKey {
            ns: ns.as_bytes().to_vec(),
            range: Some(range),
            session_token: "token".to_string(),
        };
        let result = KvStore::get_range(db.as_ref(), &addr, &range_key);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_range_smoke() {
        let tmp_dir_path = TempDir::new("get range").expect("create temp dir");
        let addr = get_a_static_address();
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let kv1 = KvPair {
            key: "k1".as_bytes().to_vec(),
            value: "value1".as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let kv2 = KvPair {
            key: "k2".as_bytes().to_vec(),
            value: "value2".as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let kv3 = KvPair {
            key: "k3".as_bytes().to_vec(),
            value: "value3".as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let mutation = Mutation {
            ns: "my_twitter".as_bytes().to_vec(),
            kv_pairs: vec![kv1, kv2, kv3],
            nonce: 1,
            chain_id: ChainId::MainNet.into(),
            chain_role: ChainRole::StorageShardChain.into(),
            gas_price: None,
            gas: 10,
        };
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = KvStore::apply(db_m, &addr, &mutation);
        assert!(result.is_ok());
        let range = DB3Range {
            start: "k0".as_bytes().to_vec(),
            end: "k4".as_bytes().to_vec(),
        };
        let ns: &str = "my_twitter";
        let range_key = RangeKey {
            ns: ns.as_bytes().to_vec(),
            range: Some(range),
            session_token: "token".to_string(),
        };
        let result = KvStore::get_range(db.as_ref(), &addr, &range_key);
        if let Ok(r) = result {
            assert_eq!(3, r.len());
            match r.back() {
                Some(ProofOp::Push(Node::KV(k, v))) => {
                    let new_key = Key::decode(k.as_ref(), ns.as_bytes().as_ref()).unwrap();
                    assert_eq!("k3".as_bytes().as_ref(), new_key.2);
                    assert_eq!("value3".as_bytes(), v);
                }
                _ => {
                    assert!(false);
                }
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn it_batch_get_empty() {
        let tmp_dir_path = TempDir::new("batch get").expect("create temp dir");
        let addr = get_a_static_address();
        let merk = Merk::open(tmp_dir_path).unwrap();
        let db = Box::pin(merk);
        let key = "k1".as_bytes().to_vec();
        let keys = BatchGetKey {
            ns: "my_twitter".as_bytes().to_vec(),
            keys: vec![key],
            session_token: "MOCK_TOKEN".to_string(),
        };
        let result = KvStore::batch_get(db.as_ref(), &addr, &keys);
        assert!(!result.is_ok());
    }

    #[test]
    fn it_apply_mutation() {
        let tmp_dir_path = TempDir::new("assign_partition").expect("create temp dir");
        let addr = get_a_static_address();
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let kv1 = KvPair {
            key: "k1".as_bytes().to_vec(),
            value: "value1".as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let kv2 = KvPair {
            key: "k2".as_bytes().to_vec(),
            value: "value1".as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let mutation = Mutation {
            ns: "my_twitter".as_bytes().to_vec(),
            kv_pairs: vec![kv1, kv2],
            nonce: 1,
            chain_id: ChainId::MainNet.into(),
            chain_role: ChainRole::StorageShardChain.into(),
            gas_price: None,
            gas: 10,
        };
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = KvStore::apply(db_m, &addr, &mutation);
        assert!(result.is_ok());
        let key = "k1".as_bytes().to_vec();
        let ns = "my_twitter";
        let keys = BatchGetKey {
            ns: ns.as_bytes().to_vec(),
            keys: vec![key.to_vec()],
            session_token: "MOCK_TOKEN".to_string(),
        };
        let result = KvStore::batch_get(db.as_ref(), &addr, &keys);
        assert!(result.is_ok());
        if let Ok(r) = result {
            assert_eq!(1, r.len());
            match r.back() {
                Some(ProofOp::Push(Node::KV(k, v))) => {
                    let new_key = Key::decode(k.as_ref(), ns.as_bytes().as_ref()).unwrap();
                    assert_eq!(key, new_key.2);
                    assert_eq!("value1".as_bytes(), v);
                }
                _ => {
                    assert!(false);
                }
            }
        } else {
            assert!(false);
        }
    }
}
