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

use db3_error::{DB3Error, Result};
use db3_proto::db3_base_proto::Units;
use db3_proto::db3_bill_proto::{Bill, BillQueryRequest, BillType};
use db3_proto::db3_mutation_proto::{Mutation, WriteRequest};
use db3_storage::account_store::AccountStore;
use db3_storage::bill_store::BillStore;
use db3_storage::kv_store::KvStore;
use db3_types::{cost, gas};
use ethereum_types::Address as AccountAddress;
use merk::proofs::{Decoder, Node, Op as ProofOp};
use merk::{proofs::encode_into, Merk};
use prost::Message;
use std::boxed::Box;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

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

    #[inline]
    pub fn get_last_block_state(&self) -> &BlockState {
        &self.last_block_state
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
        info!("change current block height to {}, time {}", height, time);
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
        let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
        let (gas_fee, total_bytes) = KvStore::apply(db, &addr, &mutation)?;
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
        Ok((gas_fee, total_bytes as u64))
    }

    /// return the root hash
    pub fn commit(&mut self) -> Hash {
        let hash = self.db.root_hash().clone();
        self.current_block_state.abci_hash = hash.clone();
        self.last_block_state = self.current_block_state.clone();
        self.current_block_state.reset();
        hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
