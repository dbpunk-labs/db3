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
use db3_proto::db3_mutation_proto::{KvPair, Mutation, MutationAction};
use db3_types::cost;
use ethereum_types::Address as AccountAddress;
use merk::{BatchEntry, Merk, Op};
use std::sync::{Arc, Mutex};

pub struct KvStore {}

impl KvStore {
    pub fn new() -> Self {
        Self {}
    }

    fn convert(kp: &KvPair, account_addr: &AccountAddress, ns: &[u8]) -> Result<BatchEntry> {
        let key = Key(*account_addr, ns, kp.key.as_ref());
        let encoded_key = key.encode()?;
        let action = MutationAction::from_i32(kp.action);
        match action {
            Some(MutationAction::InsertKv) => {
                //TODO avoid copying operation
                Ok((encoded_key, Op::Put(kp.value.to_vec())))
            }
            Some(MutationAction::DeleteKv) => Ok((encoded_key, Op::Delete)),
            None => Err(DB3Error::ApplyMutationError(
                "invalid action type".to_string(),
            )),
        }
    }

    pub fn apply(db: &mut Merk, account_addr: &AccountAddress, mutation: &Mutation) -> Result<u64> {
        let ns = mutation.ns.as_ref();
        //TODO avoid copying operation
        let mut ordered_kv_pairs = mutation.kv_pairs.to_vec();
        ordered_kv_pairs.sort_by(|a, b| a.key.cmp(&b.key));
        let mut entries: Vec<BatchEntry> = Vec::new();
        for kv in ordered_kv_pairs {
            let batch_entry = Self::convert(&kv, account_addr, ns)?;
            entries.push(batch_entry);
        }
        let gas = cost::estimate_gas(mutation);
        db.apply(&entries, &[])
            .map_err(|e| DB3Error::ApplyMutationError(format!("{}", e)))?;
        Ok(gas)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_base::get_a_static_address;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use std::thread;
    #[test]
    fn it_apply_mutation() {
        let path = thread::current().name().unwrap().to_owned();
        let addr = get_a_static_address();
        let mut merk = Merk::open(path).unwrap();
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
            gas_price: 1,
            gas: 10,
        };
        let result = KvStore::apply(&mut merk, &addr, &mutation);
        assert!(result.is_ok());
    }
}
