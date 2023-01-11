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
use db3_proto::db3_mutation_proto::{DatabaseRequest, Mutation, PayloadType, WriteRequest};
use db3_proto::db3_session_proto::{QuerySession, QuerySessionInfo};
use db3_session::query_session_verifier;
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
    total_query_sessions: Arc<AtomicU64>,
}
#[derive(Clone)]
pub struct AbciImpl {
    node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>,
    pending_mutation: Arc<Mutex<Vec<(AccountAddress, Hash, Mutation)>>>,
    pending_query_session:
        Arc<Mutex<Vec<(AccountAddress, AccountAddress, Hash, QuerySessionInfo)>>>,
    node_state: Arc<NodeState>,
    pending_databases: Arc<Mutex<Vec<(AccountAddress, DatabaseRequest)>>>,
}

impl AbciImpl {
    pub fn new(node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>) -> Self {
        Self {
            node_store,
            pending_mutation: Arc::new(Mutex::new(Vec::new())),
            pending_query_session: Arc::new(Mutex::new(Vec::new())),
            node_state: Arc::new(NodeState {
                total_storage_bytes: Arc::new(AtomicU64::new(0)),
                total_mutations: Arc::new(AtomicU64::new(0)),
                total_query_sessions: Arc::new(AtomicU64::new(0)),
            }),
            pending_databases: Arc::new(Mutex::new(Vec::new())),
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
                request.payload.as_ref(),
                request.signature.as_ref(),
                request.public_key.as_ref(),
            ) {
                Ok(_) => {
                    let payload_type = PayloadType::from_i32(request.payload_type);
                    match payload_type {
                        Some(PayloadType::DatabasePayload) => {
                            match DatabaseRequest::decode(request.payload.as_ref()) {
                                Ok(_) => {
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
                                }
                                Err(_) => {
                                    //TODO add event ?
                                    warn!("invalid database byte data");
                                }
                            }
                        }
                        Some(PayloadType::MutationPayload) => {
                            match Mutation::decode(request.payload.as_ref()) {
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
                            }
                        }
                        Some(PayloadType::QuerySessionPayload) => {
                            match QuerySession::decode(request.payload.as_ref()) {
                                Ok(query_session) => {
                                    match query_session_verifier::verify_query_session(
                                        &query_session,
                                    ) {
                                        Ok(_) => {
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
                                        }
                                        Err(e) => {
                                            warn!(
                                                "invalid transaction has been checked for error {}",
                                                e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("invalid transaction has been checked for error {}", e);
                                }
                            }
                        }
                        _ => {
                            warn!("invalid transaction with null payload type");
                        }
                    }
                }
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
                wrequest.payload.as_ref(),
                wrequest.signature.as_ref(),
                wrequest.public_key.as_ref(),
            ) {
                let payload_type = PayloadType::from_i32(wrequest.payload_type);
                match payload_type {
                    Some(PayloadType::DatabasePayload) => {
                        if let Ok(dr) = DatabaseRequest::decode(wrequest.payload.as_ref()) {
                            match self.pending_databases.lock() {
                                Ok(mut s) => {
                                    s.push((account_id.addr, dr));
                                    return ResponseDeliverTx {
                                        code: 0,
                                        data: Bytes::new(),
                                        log: "".to_string(),
                                        info: "apply_database".to_string(),
                                        gas_wanted: 0,
                                        gas_used: 0,
                                        events: vec![Event {
                                            r#type: "apply".to_string(),
                                            attributes: vec![],
                                        }],
                                        codespace: "".to_string(),
                                    };
                                }
                                _ => {
                                    todo!();
                                }
                            }
                        }
                    }
                    Some(PayloadType::MutationPayload) => {
                        if let Ok(mutation) = Mutation::decode(wrequest.payload.as_ref()) {
                            match self.pending_mutation.lock() {
                                Ok(mut s) => {
                                    //TODO add gas check
                                    s.push((
                                        account_id.addr,
                                        mutation_id.as_ref().clone(),
                                        mutation,
                                    ));
                                    return ResponseDeliverTx {
                                        code: 0,
                                        data: Bytes::new(),
                                        log: "".to_string(),
                                        info: "deliver_mutation".to_string(),
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
                    Some(PayloadType::QuerySessionPayload) => {
                        if let Ok(query_session) = QuerySession::decode(wrequest.payload.as_ref()) {
                            if let Ok((client_account_id, _)) =
                                query_session_verifier::verify_query_session(&query_session)
                            {
                                match self.pending_query_session.lock() {
                                    Ok(mut s) => {
                                        s.push((
                                            client_account_id.addr,
                                            account_id.addr,
                                            mutation_id.as_ref().clone(),
                                            query_session.node_query_session_info.unwrap(),
                                        ));
                                        return ResponseDeliverTx {
                                            code: 0,
                                            data: Bytes::new(),
                                            log: "".to_string(),
                                            info: "deliver_query_session".to_string(),
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
                    _ => {
                        warn!("invalid transaction with null payload type");
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
        let pending_query_session: Vec<(AccountAddress, AccountAddress, Hash, QuerySessionInfo)> =
            match self.pending_query_session.lock() {
                Ok(mut q) => {
                    let clone_q = q.drain(..).collect();
                    clone_q
                }
                Err(_) => {
                    todo!();
                }
            };
        let pending_databases: Vec<(AccountAddress, DatabaseRequest)> =
            match self.pending_databases.lock() {
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
                            warn!("fail to apply mutation for {}", e);
                            todo!();
                        }
                    }
                }
                let pending_query_session_len = pending_query_session.len();
                for item in pending_query_session {
                    match s.apply_query_session(&item.0, &item.1, &item.2, &item.3) {
                        Ok(_) => {
                            self.node_state
                                .total_query_sessions
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(e) => {
                            warn!("fail to apply mutation for {}", e);
                            todo!();
                        }
                    }
                }
                let pending_databases_len = pending_databases.len();
                for item in pending_databases {
                    match s.apply_database(&item.0, &item.1) {
                        Ok(_) => {}
                        Err(_) => {
                            todo!()
                        }
                    }
                }

                if pending_mutation_len > 0
                    || pending_query_session_len > 0
                    || pending_databases_len > 0
                {
                    //TODO how to revert
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
