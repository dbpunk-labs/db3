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

use db3_crypto::id::{BillId, CollectionId, DbId, DocumentId};
use db3_crypto::{db3_address::DB3Address, id::TxId};
use db3_error::Result;
use db3_proto::db3_account_proto::Account;
use db3_proto::db3_bill_proto::{Bill, BillType};
use db3_proto::db3_database_proto::{Database, Document, StructuredQuery};
use db3_proto::db3_mutation_proto::{DatabaseMutation, MintCreditsMutation};
use db3_proto::db3_session_proto::QuerySessionInfo;
use db3_storage::account_store::AccountStore;
use db3_storage::bill_store::BillStore;
use db3_storage::commit_store::CommitStore;
use db3_storage::db_store::DbStore;
use db3_types::cost;
use db3_types::cost::DbStoreOp;
use hex;
use merkdb::proofs::{Node, Op as ProofOp};
use merkdb::Merk;
use prost::Message;
use std::boxed::Box;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::info;

pub const HASH_LENGTH: usize = 32;
pub type Hash = [u8; HASH_LENGTH];
#[derive(Debug)]
pub struct NetworkState {
    pub total_storage_bytes: Arc<AtomicU64>,
    pub total_mutation_count: Arc<AtomicU64>,
    pub total_session_count: Arc<AtomicU64>,
    pub total_database_count: Arc<AtomicU64>,
    pub total_collection_count: Arc<AtomicU64>,
    pub total_index_count: Arc<AtomicU64>,
    pub total_document_count: Arc<AtomicU64>,
    pub total_account_count: Arc<AtomicU64>,
}

// the block state for db3
#[derive(Debug, Clone)]
pub struct BlockState {
    pub block_height: i64,
    pub abci_hash: Hash,
    pub tx_counter: u16,
    pub block_time: u64,
}

impl BlockState {
    pub fn reset(&mut self) {
        self.block_height = 0;
        self.abci_hash = [0; 32];
        self.block_time = 0;
        self.tx_counter = 0;
    }
}

impl BlockState {
    fn new() -> Self {
        Self {
            block_height: 0,
            abci_hash: [0; HASH_LENGTH],
            block_time: 0,
            tx_counter: 0,
        }
    }
}

// bill store, data store and account store
pub struct AuthStorage {
    last_block_state: BlockState,
    current_block_state: BlockState,
    db: Pin<Box<Merk>>,
    network_state: Arc<NetworkState>, //TODO add chain id and chain role
}

impl AuthStorage {
    pub fn new(merk: Merk) -> Self {
        Self {
            last_block_state: BlockState::new(),
            current_block_state: BlockState::new(),
            db: Box::pin(merk),
            network_state: Arc::new(NetworkState {
                total_storage_bytes: Arc::new(AtomicU64::new(0)),
                total_mutation_count: Arc::new(AtomicU64::new(0)),
                total_session_count: Arc::new(AtomicU64::new(0)),
                total_database_count: Arc::new(AtomicU64::new(0)),
                total_collection_count: Arc::new(AtomicU64::new(0)),
                total_index_count: Arc::new(AtomicU64::new(0)),
                total_document_count: Arc::new(AtomicU64::new(0)),
                total_account_count: Arc::new(AtomicU64::new(0)),
            }),
        }
    }

    pub fn get_state(&self) -> Arc<NetworkState> {
        self.network_state.clone()
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

    pub fn get_account(&self, addr: &DB3Address) -> Result<Option<Account>> {
        AccountStore::get_account(self.db.as_ref(), addr)
    }

    pub fn get_database(&self, id: &DbId) -> Result<Option<Database>> {
        DbStore::get_database(self.db.as_ref(), id)
    }

    pub fn get_my_database(&self, owner: &DB3Address) -> Result<Vec<Database>> {
        DbStore::get_my_database(self.db.as_ref(), owner)
    }

    pub fn get_documents(&self, id: &CollectionId) -> Result<Vec<Document>> {
        // TODO(chanjing): support get documents with limit
        DbStore::get_documents(self.db.as_ref(), id, None)
    }
    pub fn get_document(&self, id: &DocumentId) -> Result<Option<Document>> {
        DbStore::get_document(self.db.as_ref(), id)
    }
    pub fn run_query(&self, db_id: &DbId, query: &StructuredQuery) -> Result<Vec<Document>> {
        DbStore::run_query(self.db.as_ref(), db_id, query)
    }
    pub fn get_bills(&self, height: u64) -> Result<Vec<Bill>> {
        let proofs_ops = BillStore::get_block_bills(self.db.as_ref(), height)?;
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
        self.current_block_state.tx_counter = 0;
    }

    pub fn apply_query_session(
        &mut self,
        addr: &DB3Address,
        query_addr: &DB3Address,
        tx_id: &TxId,
        query_session_info: &QuerySessionInfo,
    ) -> Result<u64> {
        let mut account = match AccountStore::get_account(self.db.as_ref(), addr)? {
            Some(account) => Ok(account),
            None => {
                //TODO remove the action for adding a new user
                let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
                self.network_state
                    .total_account_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                AccountStore::new_account(db, addr, 10)
            }
        }?;
        self.current_block_state.tx_counter = self.current_block_state.tx_counter + 1;
        let gas_fee = cost::estimate_query_session_gas(query_session_info);
        if account.credits >= gas_fee {
            account.credits = account.credits - gas_fee;
            account.bills = account.bills + gas_fee;
            account.total_session_count += 1;
            let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
            AccountStore::update_account(db, addr, &account)?;
        } else {
            // TODO throw out of gas error
            account.credits = 0;
            account.bills = account.bills + gas_fee;
            account.total_session_count += 1;
            let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
            AccountStore::update_account(db, addr, &account)?;
        }

        let bill_id = BillId::new(
            self.current_block_state.block_height as u64,
            self.current_block_state.tx_counter as u16,
        )?;

        let bill = Bill {
            gas_fee,
            block_id: self.current_block_state.block_height as u64,
            bill_type: BillType::BillForQuery.into(),
            time: self.current_block_state.block_time,
            tx_id: tx_id.as_ref().to_vec(),
            owner: addr.to_vec(),
            to: query_addr.to_vec(),
        };
        //TODO account query service gas fee
        self.network_state
            .total_session_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
        BillStore::apply(db, &bill_id, &bill)?;
        Ok(gas_fee)
    }

    fn update_metric(&self, ops: &DbStoreOp) {
        match ops {
            DbStoreOp::DbOp {
                create_db_ops,
                create_collection_ops,
                create_index_ops,
                data_in_bytes,
            } => {
                self.network_state
                    .total_database_count
                    .fetch_add(*create_db_ops, std::sync::atomic::Ordering::Relaxed);
                self.network_state
                    .total_storage_bytes
                    .fetch_add(*data_in_bytes, std::sync::atomic::Ordering::Relaxed);
                self.network_state
                    .total_collection_count
                    .fetch_add(*create_collection_ops, std::sync::atomic::Ordering::Relaxed);
                self.network_state
                    .total_index_count
                    .fetch_add(*create_index_ops, std::sync::atomic::Ordering::Relaxed);
            }
            DbStoreOp::DocOp {
                add_doc_ops,
                del_doc_ops,
                data_in_bytes,
                ..
            } => {
                self.network_state
                    .total_document_count
                    .fetch_add(*add_doc_ops, std::sync::atomic::Ordering::Relaxed);
                self.network_state
                    .total_storage_bytes
                    .fetch_add(*data_in_bytes, std::sync::atomic::Ordering::Relaxed);
                self.network_state
                    .total_document_count
                    .fetch_sub(*del_doc_ops, std::sync::atomic::Ordering::Relaxed);
            }
        }
        self.network_state
            .total_mutation_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn apply_mint_credits(
        &mut self,
        sender: &DB3Address,
        _nonce: u64,
        tx: &TxId,
        mint: &MintCreditsMutation,
    ) -> Result<()> {
        //TODO the sender address must be limited
        let _account = match AccountStore::get_account(self.db.as_ref(), sender)? {
            Some(account) => Ok(account),
            None => {
                //TODO remove the action for adding a new user
                let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
                self.network_state
                    .total_account_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountStore::new_account(db, sender, 10)
            }
        }?;
        let to_address_ref: &[u8] = mint.to.as_ref();
        let to_address = DB3Address::try_from(to_address_ref)?;
        match AccountStore::get_account(self.db.as_ref(), &to_address)? {
            Some(mut account) => {
                account.credits += mint.amount;
                let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
                AccountStore::update_account(db, &to_address, &account)?;
            }
            None => {
                //TODO remove the action for adding a new user
                let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
                self.network_state
                    .total_account_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountStore::new_account(db, &to_address, mint.amount / 1000_000_000)?;
            }
        };
        let bill_id = BillId::new(
            self.current_block_state.block_height as u64,
            self.current_block_state.tx_counter as u16,
        )?;

        let bill = Bill {
            //TODO update the gas calculator
            gas_fee: 10000,
            block_id: self.current_block_state.block_height as u64,
            bill_type: BillType::BillForMint.into(),
            time: self.current_block_state.block_time,
            tx_id: tx.as_ref().to_vec(),
            owner: sender.to_vec(),
            to: vec![],
        };
        let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
        BillStore::apply(db, &bill_id, &bill)?;
        Ok(())
    }

    pub fn apply_database(
        &mut self,
        sender: &DB3Address,
        nonce: u64,
        tx: &TxId,
        mutation: &DatabaseMutation,
    ) -> Result<u64> {
        //
        let mut account = match AccountStore::get_account(self.db.as_ref(), sender)? {
            Some(account) => Ok(account),
            None => {
                //TODO remove the action for adding a new user
                let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
                self.network_state
                    .total_account_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                AccountStore::new_account(db, sender, 10)
            }
        }?;

        //TODO make sure the account has enough credits
        let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
        self.current_block_state.tx_counter += 1;
        let ops = DbStore::apply_mutation(
            db,
            sender,
            nonce,
            tx,
            mutation,
            self.current_block_state.block_height as u64,
            self.current_block_state.tx_counter.into(),
        )?;
        self.update_metric(&ops);
        let gas_fee = cost::estimate_gas(&ops);
        if account.credits >= gas_fee {
            account.credits = account.credits - gas_fee;
            account.bills = account.bills + gas_fee;
            account.total_mutation_count += 1;
            account.total_storage_in_bytes += ops.get_data_size();
            let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
            AccountStore::update_account(db, sender, &account)?;
        } else {
            // TODO throw out of gas error
            account.credits = 0;
            account.bills = account.bills + gas_fee;
            account.total_mutation_count += 1;
            account.total_storage_in_bytes += ops.get_data_size();
            let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
            AccountStore::update_account(db, sender, &account)?;
        }

        let bill_id = BillId::new(
            self.current_block_state.block_height as u64,
            self.current_block_state.tx_counter as u16,
        )?;

        let bill = Bill {
            gas_fee,
            block_id: self.current_block_state.block_height as u64,
            bill_type: BillType::BillForMutation.into(),
            time: self.current_block_state.block_time,
            tx_id: tx.as_ref().to_vec(),
            owner: sender.to_vec(),
            to: vec![],
        };
        let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
        BillStore::apply(db, &bill_id, &bill)?;
        Ok(gas_fee)
    }

    /// return the root hash
    pub fn commit(&mut self) -> Result<Hash> {
        let db: Pin<&mut Merk> = Pin::as_mut(&mut self.db);
        CommitStore::apply(db, self.current_block_state.block_height as u64)?;
        let hash = self.db.root_hash().clone();
        self.current_block_state.abci_hash = hash.clone();
        self.last_block_state = self.current_block_state.clone();
        self.current_block_state.reset();
        info!(
            "commit hash {} block {}",
            hex::encode_upper(hash),
            self.last_block_state.block_height
        );
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
