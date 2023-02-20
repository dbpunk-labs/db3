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

use shadow_rs::shadow;
shadow!(build);
use crate::node_storage::NodeStorage;
use bytes::Bytes;
use db3_crypto::{db3_address::DB3Address as AccountAddress, db3_verifier, id::TxId};
use db3_proto::db3_mutation_proto::{
    DatabaseMutation, MintCreditsMutation, PayloadType, WriteRequest,
};
use db3_proto::db3_session_proto::{QuerySession, QuerySessionInfo};
use db3_session::query_session_verifier;
use fastcrypto::encoding::{Base64, Encoding};
use hex;
use prost::Message;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tendermint_abci::Application;
use tendermint_proto::abci::{
    Event, RequestBeginBlock, RequestCheckTx, RequestDeliverTx, RequestInfo, RequestQuery,
    ResponseBeginBlock, ResponseCheckTx, ResponseCommit, ResponseDeliverTx, ResponseInfo,
    ResponseQuery,
};
use tracing::{debug, info, span, warn, Level};

#[derive(Clone)]
pub struct AbciImpl {
    node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>,
    pending_query_session:
        Arc<Mutex<Vec<(AccountAddress, AccountAddress, TxId, QuerySessionInfo)>>>,
    pending_databases: Arc<Mutex<Vec<(AccountAddress, DatabaseMutation, TxId)>>>,
    pending_credits: Arc<Mutex<Vec<(AccountAddress, MintCreditsMutation, TxId)>>>,
}

impl AbciImpl {
    pub fn new(node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>) -> Self {
        Self {
            node_store,
            pending_query_session: Arc::new(Mutex::new(Vec::new())),
            pending_databases: Arc::new(Mutex::new(Vec::new())),
            pending_credits: Arc::new(Mutex::new(Vec::new()))
        }
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
                    version: shadow_rs::tag(),
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
            Ok(request) => match db3_verifier::DB3Verifier::verify(
                request.payload.as_ref(),
                request.signature.as_ref(),
            ) {
                Ok(_) => {
                    let payload_type = PayloadType::from_i32(request.payload_type);
                    match payload_type {
                        Some(PayloadType::DatabasePayload) => {
                            match DatabaseMutation::decode(request.payload.as_ref()) {
                                Ok(dm) => match &dm.meta {
                                    Some(_) => {
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
                                    None => {
                                        //TODO add event
                                        warn!("no meta for database mutation");
                                    }
                                },
                                Err(_) => {
                                    //TODO add event ?
                                    warn!("invalid database byte data");
                                }
                            }
                        }

                        Some(PayloadType::MintCreditsPayload) => {
                            match MintCreditsMutation::decode(request.payload.as_ref()) {
                                Ok(mint_credits) => {}
                                Err(e) => {
                                    warn!("invalid mint credist mutation has been checked for error {}", e);
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
                    let payload: &[u8] = request.payload.as_ref();
                    let signature: &[u8] = request.signature.as_ref();
                    warn!("invalid transaction has been checked for error {}", e);
                    warn!(
                        "payload {}, signature {}",
                        Base64::encode(payload),
                        Base64::encode(signature)
                    );
                }
            },
            Err(e) => {
                warn!("fail to decode WriteRequest for error {}", e);
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
        let tx_id = TxId::from(request.tx.as_ref());
        if let Ok(wrequest) = WriteRequest::decode(request.tx.as_ref()) {
            if let Ok(account_id) = db3_verifier::DB3Verifier::verify(
                wrequest.payload.as_ref(),
                wrequest.signature.as_ref(),
            ) {
                let payload_type = PayloadType::from_i32(wrequest.payload_type);
                match payload_type {
                    Some(PayloadType::MintCreditsPayload) => {
                        if let Ok(mint_credits) =
                            MintCreditsMutation::decode(wrequest.payload.as_ref())
                        {
                            match self.pending_credits.lock() {
                                Ok(mut s) => {
                                    s.push((account_id.addr, mint_credits, tx_id));
                                    return ResponseDeliverTx {
                                        code: 0,
                                        data: Bytes::new(),
                                        log: "".to_string(),
                                        info: "apply_mint_credits".to_string(),
                                        gas_wanted: 0,
                                        gas_used: 0,
                                        events: vec![Event {
                                            r#type: "apply".to_string(),
                                            attributes: vec![],
                                        }],
                                        codespace: "".to_string(),
                                    };
                                }
                                _ => {}
                            }
                        }
                    }
                    Some(PayloadType::DatabasePayload) => {
                        if let Ok(dr) = DatabaseMutation::decode(wrequest.payload.as_ref()) {
                            match self.pending_databases.lock() {
                                Ok(mut s) => {
                                    s.push((account_id.addr, dr, tx_id));
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
                    Some(PayloadType::QuerySessionPayload) => {
                        if let Ok(query_session) = QuerySession::decode(wrequest.payload.as_ref()) {
                            if let Ok((client_account_id, _)) =
                                query_session_verifier::verify_query_session(&query_session)
                            {
                                match self.pending_query_session.lock() {
                                    Ok(mut s) => {
                                        //TODO  check the node query session info
                                        s.push((
                                            client_account_id.addr,
                                            account_id.addr,
                                            tx_id,
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
        let pending_query_session: Vec<(AccountAddress, AccountAddress, TxId, QuerySessionInfo)> =
            match self.pending_query_session.lock() {
                Ok(mut q) => {
                    let clone_q = q.drain(..).collect();
                    clone_q
                }
                Err(_) => {
                    todo!();
                }
            };
        let pending_databases: Vec<(AccountAddress, DatabaseMutation, TxId)> =
            match self.pending_databases.lock() {
                Ok(mut q) => {
                    let clone_q = q.drain(..).collect();
                    clone_q
                }
                Err(_) => {
                    todo!();
                }
            };
        let pending_mint_credits: Vec<(AccountAddress, MintCreditsMutation, TxId)> =
            match self.pending_credits.lock() {
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
                let pending_credits_lens = pending_mint_credits.len();

                let pending_query_session_len = pending_query_session.len();
                for item in pending_query_session {
                    match s.apply_query_session(&item.0, &item.1, &item.2, &item.3) {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("fail to apply mutation for {}", e);
                            todo!();
                        }
                    }
                }
                let pending_databases_len = pending_databases.len();
                for item in pending_databases {
                    let nonce: u64 = match &item.1.meta {
                        Some(m) => m.nonce,
                        //TODO will not go to here
                        None => 1,
                    };
                    match s.apply_database(&item.0, nonce, &item.2, &item.1) {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("fail to apply database for {e}");
                            todo!()
                        }
                    }
                }
                span.exit();
                if pending_query_session_len > 0 || pending_databases_len > 0 {
                    //TODO how to revert
                    if let Ok(hash) = s.commit() {
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
