//
// auth_storage.rs
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

use db3_error::Result;
use db3_proto::db3_account_proto::Account;
use db3_proto::db3_base_proto::Units;
use db3_proto::db3_bill_proto::{Bill, BillType};
use db3_proto::db3_mutation_proto::{KvPair, Mutation, MutationAction};
use db3_proto::db3_node_proto::{BatchGetKey, BatchGetValue};
use db3_storage::account_store::AccountStore;
use db3_storage::bill_store::BillStore;
use db3_storage::commit_store::CommitStore;
use db3_storage::key::Key;
use db3_storage::kv_store::KvStore;
use db3_types::gas;
use ethereum_types::Address as AccountAddress;
use hex;
use merkdb::proofs::{Node, Op as ProofOp};
use merkdb::Merk;
use prost::Message;
use std::boxed::Box;
use std::pin::Pin;
use tracing::info;
pub const HASH_LENGTH: usize = 32;
pub type Hash = [u8; HASH_LENGTH];

// the block state for db3
#[derive(Debug, Clone)]
pub struct BlockState {
    pub block_height: i64,
    pub abci_hash: Hash,
    //TODO remove and use hash of bill as it's id
    pub bill_id_counter: u64,
    pub block_time: u64,
}

impl BlockState {
    pub fn reset(&mut self) {
        self.block_height = 0;
        self.abci_hash = [0; 32];
        self.bill_id_counter = 0;
        self.block_time = 0;
    }
}

impl BlockState {
    fn new() -> Self {
        Self {
            block_height: 0,
            abci_hash: [0; HASH_LENGTH],
            bill_id_counter: 0,
            block_time: 0,
        }
    }
}

// bill store, data store and account store
pub struct AuthStorage {
    last_block_state: BlockState,
    current_block_state: BlockState,
    db: Pin<Box<Merk>>,
    //TODO add chain id and chain role
}

impl AuthStorage {
    pub fn new(merk: Merk) -> Self {
        Self {
            last_block_state: BlockState::new(),
            current_block_state: BlockState::new(),
            db: Box::pin(merk),
        }
    }

    pub fn init(&mut self) -> Result<()> {
        if let Ok(Some(height)) = self.get_latest_height() {
            self.last_block_state.block_height = height as i64;
            self.last_block_state.abci_hash = self.db.root_hash().clone();
            info!(
                "recover state with height {} and hash {}",
                height,
                hex::encode_upper(self.last_block_state.abci_hash)
            );
        } else {
            info!("a new node started");
        }
        Ok(())
    }

    pub fn get_latest_height(&self) -> Result<Option<u64>> {
        CommitStore::get_applied_height(self.db.as_ref())
    }

    #[inline]
    pub fn get_last_block_state(&self) -> &BlockState {
        &self.last_block_state
    }

    pub fn batch_get(
        &self,
        addr: &AccountAddress,
        batch_get_keys: &BatchGetKey,
    ) -> Result<BatchGetValue> {
        let proofs_ops = KvStore::batch_get(self.db.as_ref(), addr, batch_get_keys)?;
        let ns = batch_get_keys.ns.as_ref();
        let mut kv_pairs: Vec<KvPair> = Vec::new();
        for op in proofs_ops {
            match op {
                ProofOp::Push(Node::KV(k, v)) => {
                    let new_key = Key::decode(k.as_ref(), ns)?;
                    kv_pairs.push(KvPair {
                        key: new_key.2.to_owned(),
                        value: v,
                        action: MutationAction::Nonce.into(),
                    });
                }
                _ => {}
            }
        }
        Ok(BatchGetValue {
            values: kv_pairs.to_owned(),
            session_token: batch_get_keys.session_token.clone(),
            ns: ns.to_vec(),
        })
    }

    pub fn get_account(&self, addr: &AccountAddress) -> Result<Account> {
        AccountStore::get_account(self.db.as_ref(), addr)
    }

    pub fn get_bills(&self, height: u64, start_id: u64, end_id: u64) -> Result<Vec<Bill>> {
        let proofs_ops = BillStore::scan(self.db.as_ref(), height, start_id, end_id)?;
        let mut bills: Vec<Bill> = Vec::new();
        for op in proofs_ops {
            match op {
                ProofOp::Push(Node::KV(_, v)) => {
                    if let Ok(b) = Bill::decode(v.as_ref()) {
                        bills.push(b);
                    }
                }
                _ => {}
            }
        }
        Ok(bills)
    }

    pub fn begin_block(&mut self, height: u64, time: u64) {
        self.current_block_state.block_time = time;
        self.current_block_state.block_height = height as i64;
        self.current_block_state.bill_id_counter = 0;
    }

    pub fn apply_mutation(
        &mut self,
        addr: &AccountAddress,
        mutation_id: &Hash,
        mutation: &Mutation,
    ) -> Result<(Units, u64)> {
        let mut account = AccountStore::get_account(self.db.as_ref(), &addr)?;
        let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
        let (gas_fee, total_bytes) = KvStore::apply(db, &addr, &mutation)?;
        let accumulate_gas = gas::gas_add(&gas_fee, &account.total_bills.unwrap());
        account.total_bills = Some(accumulate_gas);
        account.total_mutation_count = account.total_mutation_count + 1;
        account.total_storage_in_bytes = account.total_storage_in_bytes + total_bytes as u64;
        self.current_block_state.bill_id_counter = self.current_block_state.bill_id_counter + 1;
        let bill = Bill {
            gas_fee: Some(gas_fee.clone()),
            block_height: self.current_block_state.block_height as u64,
            bill_id: self.current_block_state.bill_id_counter,
            bill_type: BillType::BillForMutation.into(),
            time: self.current_block_state.block_time,
            bill_target_id: mutation_id.to_vec(),
            owner: addr.as_bytes().to_vec(),
            query_addr: vec![],
        };
        let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
        BillStore::apply(db, &bill)?;
        let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
        AccountStore::apply(db, &addr, &account)?;
        Ok((gas_fee, total_bytes as u64))
    }

    /// return the root hash
    pub fn commit(&mut self) -> Result<Hash> {
        let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
        CommitStore::apply(db, self.current_block_state.block_height as u64)?;
        let hash = self.db.root_hash().clone();
        self.current_block_state.abci_hash = hash.clone();
        self.last_block_state = self.current_block_state.clone();
        self.current_block_state.reset();
        Ok(hash)
    }

    pub fn root_hash(&self) -> Hash {
        self.last_block_state.abci_hash.clone()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
