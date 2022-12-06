//
// abci_impl.rs
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

use super::auth_storage::Hash;
use crate::node_storage::NodeStorage;
use bytes::Bytes;
use db3_crypto::verifier;
use db3_proto::db3_mutation_proto::{Mutation, WriteRequest};
use db3_storage::kv_store::KvStore;
use ethereum_types::Address as AccountAddress;
use hex;
use prost::Message;
use rust_secp256k1::Message as HashMessage;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use tendermint_abci::Application;
use tendermint_proto::abci::{
    Event, RequestBeginBlock, RequestCheckTx, RequestDeliverTx, RequestInfo, RequestQuery,
    ResponseBeginBlock, ResponseCheckTx, ResponseCommit, ResponseDeliverTx, ResponseInfo,
    ResponseQuery,
};
use tracing::{debug, info, span, warn, Level};

#[derive(Clone)]
pub struct NodeState {
    total_storage_bytes: Arc<AtomicU64>,
    total_mutations: Arc<AtomicU64>,
}
#[derive(Clone)]
pub struct AbciImpl {
    node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>,
    pending_mutation: Arc<Mutex<Vec<(AccountAddress, Hash, Mutation)>>>,
    node_state: Arc<NodeState>,
}

impl AbciImpl {
    pub fn new(node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>) -> Self {
        Self {
            node_store,
            pending_mutation: Arc::new(Mutex::new(Vec::new())),
            node_state: Arc::new(NodeState {
                total_storage_bytes: Arc::new(AtomicU64::new(0)),
                total_mutations: Arc::new(AtomicU64::new(0)),
            }),
        }
    }

    #[inline]
    pub fn get_node_state(&self) -> &Arc<NodeState> {
        &self.node_state
    }
}

impl Application for AbciImpl {
    fn info(&self, _request: RequestInfo) -> ResponseInfo {
        // the store must be ready when using it
        match self.node_store.lock() {
            Ok(mut store) => {
                let s = store.get_auth_store();
                info!(
                    "height {} hash {}",
                    s.get_last_block_state().block_height,
                    hex::encode_upper(s.get_last_block_state().abci_hash)
                );
                ResponseInfo {
                    data: "db3".to_string(),
                    version: "0.1.0".to_string(),
                    app_version: 1,
                    last_block_height: s.get_last_block_state().block_height,
                    last_block_app_hash: Bytes::copy_from_slice(
                        &s.get_last_block_state().abci_hash,
                    ),
                }
            }

            Err(_) => todo!(),
        }
    }

    fn begin_block(&self, request: RequestBeginBlock) -> ResponseBeginBlock {
        match self.node_store.lock() {
            Ok(mut store) => {
                let s = store.get_auth_store();
                if let Some(header) = request.header {
                    if let Some(time) = header.time {
                        s.begin_block(header.height as u64, time.seconds as u64);
                    } else {
                        todo!();
                    }
                } else {
                    todo!();
                }
            }
            Err(_) => todo!(),
        }
        Default::default()
    }

    fn query(&self, _request: RequestQuery) -> ResponseQuery {
        Default::default()
    }

    fn check_tx(&self, request: RequestCheckTx) -> ResponseCheckTx {
        // decode the request
        match WriteRequest::decode(request.tx.as_ref()) {
            Ok(request) => match verifier::Verifier::verify(
                request.mutation.as_ref(),
                request.signature.as_ref(),
                request.public_key.as_ref(),
            ) {
                Ok(_) => match Mutation::decode(request.mutation.as_ref()) {
                    Ok(mutation) => {
                        if KvStore::is_valid(&mutation) {
                            return ResponseCheckTx {
                                code: 0,
                                data: Bytes::new(),
                                log: "".to_string(),
                                info: "".to_string(),
                                gas_wanted: 1,
                                gas_used: 0,
                                events: vec![],
                                codespace: "".to_string(),
                                ..Default::default()
                            };
                        } else {
                            warn!("invalid mutation for kv store");
                        }
                    }
                    Err(e) => {
                        warn!("invalid transaction has been checked for error {}", e);
                    }
                },
                Err(e) => {
                    warn!("invalid transaction has been checked for error {}", e);
                }
            },
            Err(e) => {
                warn!("invalid transaction has been checked for error {}", e);
            }
        }
        // the tx should be removed from mempool
        return ResponseCheckTx {
            code: 1,
            data: Bytes::new(),
            log: "bad request".to_string(),
            info: "".to_string(),
            gas_wanted: 1,
            gas_used: 0,
            events: vec![],
            codespace: "".to_string(),
            ..Default::default()
        };
    }

    fn deliver_tx(&self, request: RequestDeliverTx) -> ResponseDeliverTx {
        //TODO match the hash fucntion with tendermint
        let mutation_id = HashMessage::from_hashed_data::<rust_secp256k1::hashes::sha256::Hash>(
            request.tx.as_ref(),
        );
        if let Ok(wrequest) = WriteRequest::decode(request.tx.as_ref()) {
            if let Ok(account_id) = verifier::Verifier::verify(
                wrequest.mutation.as_ref(),
                wrequest.signature.as_ref(),
                wrequest.public_key.as_ref(),
            ) {
                if let Ok(mutation) = Mutation::decode(wrequest.mutation.as_ref()) {
                    match self.pending_mutation.lock() {
                        Ok(mut s) => {
                            //TODO add gas check
                            s.push((account_id.addr, mutation_id.as_ref().clone(), mutation));
                            return ResponseDeliverTx {
                                code: 0,
                                data: Bytes::new(),
                                log: "".to_string(),
                                info: "".to_string(),
                                gas_wanted: 0,
                                gas_used: 0,
                                events: vec![Event {
                                    r#type: "deliver".to_string(),
                                    attributes: vec![],
                                }],
                                codespace: "".to_string(),
                            };
                        }
                        Err(_) => todo!(),
                    }
                }
            }
        }
        warn!("invalid transaction has been checked");
        ResponseDeliverTx {
            code: 1,
            data: Bytes::new(),
            log: "".to_string(),
            info: "".to_string(),
            gas_wanted: 0,
            gas_used: 0,
            events: vec![Event {
                r#type: "deliver".to_string(),
                attributes: vec![],
            }],
            codespace: "".to_string(),
        }
    }

    fn commit(&self) -> ResponseCommit {
        let pending_mutation: Vec<(AccountAddress, Hash, Mutation)> =
            match self.pending_mutation.lock() {
                Ok(mut q) => {
                    let clone_q = q.drain(..).collect();
                    clone_q
                }
                Err(_) => {
                    todo!();
                }
            };
        match self.node_store.lock() {
            Ok(mut store) => {
                let s = store.get_auth_store();
                let span = span!(Level::INFO, "commit").entered();
                let pending_mutation_len = pending_mutation.len();
                for item in pending_mutation {
                    match s.apply_mutation(&item.0, &item.1, &item.2) {
                        Ok((_gas, total_bytes)) => {
                            self.node_state
                                .total_mutations
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            self.node_state.total_storage_bytes.fetch_add(
                                total_bytes as u64,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                        }
                        Err(e) => {
                            info!("fail to apply mutation for {}", e);
                            todo!();
                        }
                    }
                }
                if pending_mutation_len > 0 {
                    if let Ok(hash) = s.commit() {
                        span.exit();
                        ResponseCommit {
                            data: Bytes::copy_from_slice(&hash),
                            retain_height: 0,
                        }
                    } else {
                        todo!();
                    }
                } else {
                    let hash = s.root_hash();
                    debug!("commit hash {}", hex::encode_upper(hash));
                    ResponseCommit {
                        data: Bytes::copy_from_slice(&hash),
                        retain_height: 0,
                    }
                }
            }
            Err(_) => {
                todo!();
            }
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {}
}
