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
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_proto::{
    DatabaseMutation, MintCreditsMutation, PayloadType, WriteRequest,
};
use db3_proto::db3_session_proto::{QuerySession, QuerySessionInfo};
use db3_session::query_session_verifier;
use ethers::core::types::transaction::eip712::{Eip712, TypedData};
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

macro_rules! parse_mutation {
    ($func:ident, $type:ident) => {
        fn $func(&self, payload: &[u8]) -> Result<$type> {
            match $type::decode(payload) {
                Ok(dm) => match &dm.meta {
                    Some(_) => Ok(dm),
                    None => {
                        warn!("no meta for mutation");
                        Err(DB3Error::ApplyMutationError("meta is none".to_string()))
                    }
                },
                Err(e) => {
                    //TODO add event ?
                    warn!("invalid mutation data {e}");
                    Err(DB3Error::ApplyMutationError(
                        "invalid mutation data".to_string(),
                    ))
                }
            }
        }
    };
}
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
            pending_credits: Arc::new(Mutex::new(Vec::new())),
        }
    }
    parse_mutation!(parse_database_mutation, DatabaseMutation);
    parse_mutation!(parse_mint_credits_mutation, MintCreditsMutation);
    parse_mutation!(parse_query_session, QuerySession);
    fn check_payload(&self, payload: &[u8], payload_type: PayloadType) -> bool {
        match payload_type {
            Some(PayloadType::DatabasePayload) | Some(PayloadType::TypedDataDatabasePayload) => {
                return self.parse_database_mutation(payload).is_ok();
            }

            Some(PayloadType::MintCreditsPayload)
            | Some(PayloadType::TypedDataMintCreditsPayload) => {
                return self.parse_mint_credits_mutation(payload).is_ok();
            }

            Some(PayloadType::QuerySessionPayload)
            | Some(PayloadType::TypedDataQuerySessionPayload) => {
                return self.parse_query_session(payload).is_ok();
            }
            _ => {
                warn!("invalid transaction with null payload type");
                return false;
            }
        }
    }

    fn unwrap_and_verify(&self, req: WriteRequest) -> Result<(Bytes, PayloadType, AccountId)> {
        if req.payload_type == 3 {
            // typed data
            match serde_json::from_slice::<TypedData>(req.payload.as_ref()) {
                Ok(data) => {
                    let hashed_message =
                        data.encode_eip712()
                            .map_err(DB3Error::ApplyMutationError(format!(
                                "invalid payload type {e}"
                            )))?;
                    let account_id = db3_verifier::DB3Verifier::verify_hashed(
                        &hashed_message,
                        req.signature.as_ref(),
                    )?;
                    if let (Some(payload), Some(payloadType)) =
                        (data.message.get("payload"), data.message.get("payloadType"))
                    {
                        let data: Bytes = serde_json::from_value(payload)?;
                        let dataType: PayloadType = PayloadType.from_i32(payloadType).ok_or(
                            DB3Error::ApplyMutationError("invalid payload type".to_string()),
                        )?;
                        Ok((data, dataType, account_id))
                    } else {
                        Err(DB3Error::ApplyMutationError("bad typed data".to_string()))
                    }
                }
                Err(e) => Err(DB3Error::ApplyMutationError(format!(
                    "bad typed data for err {e}"
                ))),
            }
        } else {
            let account_id =
                db3_verifier::DB3Verifier::verify(req.payload.as_ref(), req.signature.as_ref())?;
            let dataType: PayloadType =
                PayloadType
                    .from_i32(req.payload_type)
                    .ok_or(DB3Error::ApplyMutationError(
                        "invalid payload type".to_string(),
                    ))?;
            let data = Bytes::from(req.payload);
            Ok((data, dataType, account_id))
        }
    }

    fn build_check_response(&self, ok: bool, msg: &str) -> RequestCheckTx {
        if ok {
            ResponseCheckTx {
                code: 0,
                ..Default::default()
            }
        } else {
            ResponseCheckTx {
                code: 1,
                log: msg.to_string(),
                ..Default::default()
            }
        }
    }

    fn build_delivered_response(&self, ok: bool, msg: &str) -> ResponseDeliverTx {
        if ok {
            return ResponseDeliverTx {
                code: 0,
                ..Default::default()
            };
        } else {
            return ResponseDeliverTx {
                code: 1,
                log: msg.to_string(),
                ..Default::default()
            };
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
        let wrequest = WriteRequest::decode(request.tx.as_ref());
        match wrequest {
            Ok(req) => match self.unwrap_request(req) {
                Ok((data, data_type, _)) => match data_type {
                    PayloadType::DatabasePayload => {
                        match self.parse_database_mutation(data.as_ref()) {
                            Ok(_) => {
                                return self.build_check_response(true, "");
                            }
                            Err(e) => {
                                let msg = format!("{e}");
                                return self.build_check_response(false, msg.as_str());
                            }
                        }
                    }
                    PayloadType::QuerySessionPayload => {
                        match self.parse_query_session(data.as_ref()) {
                            Ok(_) => {
                                return self.build_check_response(true, "");
                            }
                            Err(e) => {
                                let msg = format!("{e}");
                                return self.build_check_response(false, msg.as_str());
                            }
                        }
                    }
                    PayloadType::MintCreditsPayload => {
                        match self.parse_mint_credits_mutation(data.as_ref()) {
                            Ok(_) => {
                                return self.build_check_response(true, "");
                            }
                            Err(e) => {
                                let msg = format!("{e}");
                                return self.build_check_response(false, msg.as_str());
                            }
                        }
                    }
                    _ => {
                        return self.build_check_response(false, "bad mutation payload");
                    }
                },
                Err(e) => {
                    let msg = format!("{e}");
                    return self.build_check_response(false, msg.as_str());
                }
            },
            Err(e) => {
                let msg = format!("{e}");
                return self.build_check_response(false, msg.as_str());
            }
        }
    }

    fn deliver_tx(&self, request: RequestDeliverTx) -> ResponseDeliverTx {
        //TODO match the hash fucntion with tendermint
        let tx_id = TxId::from(request.tx.as_ref());
        let wrequest = WriteRequest::decode(request.tx.as_ref());
        match wrequest {
            Ok(req) => match self.unwrap_request(req) {
                Ok((data, data_type, account_id)) => match data_type {
                    PayloadType::DatabasePayload => {
                        match self.parse_database_mutation(data.as_ref()) {
                            Ok(dm) => match self.pending_databases.lock() {
                                Ok(mut s) => {
                                    s.push((account_id.addr, dm, tx_id));
                                    return self.build_delivered_response(true, "");
                                }
                                _ => {
                                    todo!();
                                }
                            },
                            Err(e) => {
                                let msg = format!("{e}");
                                return self.build_delivered_response(false, msg.as_str());
                            }
                        }
                    }
                    PayloadType::QuerySessionPayload => {
                        match self.parse_query_session(data.as_ref()) {
                            Ok(qm) => match self.pending_databases.lock() {
                                Ok(mut s) => {
                                    s.push((
                                        client_account_id.addr,
                                        account_id.addr,
                                        tx_id,
                                        query_session.node_query_session_info.unwrap(),
                                    ));
                                }
                                Err(e) => {}
                            },
                            Err(e) => {
                                let msg = format!("{e}");
                                return self.build_delivered_response(false, msg.as_str());
                            }
                        }
                    }
                    PayloadType::MintCreditsPayload => {
                        match self.parse_mint_credits_mutation(data.as_ref()) {
                            Ok(mm) => match self.pending_credits.lock() {
                                Ok(mut s) => {
                                    s.push((account_id.addr, mm, tx_id));
                                    return self.build_delivered_response(true, "");
                                }
                                Err(e) => {
                                    let msg = format!("{e}");
                                    return self.build_delivered_response(false, msg.as_str());
                                }
                            },
                            Err(e) => {
                                let msg = format!("{e}");
                                return self.build_delivered_response(false, msg.as_str());
                            }
                        }
                    }
                    _ => {
                        return self.build_check_response(false, "bad mutation payload");
                    }
                },
                Err(e) => {
                    let msg = format!("{e}");
                    return self.build_check_response(false, msg.as_str());
                }
            },
            Err(e) => {
                let msg = format!("{e}");
                return self.build_check_response(false, msg.as_str());
            }
        }
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
                                    info!("put mint credits request to queue");
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
                        } else {
                            warn!("fail to decode mint credits");
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
                let mut mutation_size: usize = 0;
                mutation_size += pending_mint_credits.len();
                for item in pending_mint_credits {
                    let nonce: u64 = match &item.1.meta {
                        Some(m) => m.nonce,
                        //TODO will not go to here
                        None => 1,
                    };
                    match s.apply_mint_credits(&item.0, nonce, &item.2, &item.1) {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("fail to apply mint credits  for {}", e);
                            todo!();
                        }
                    }
                }

                mutation_size += pending_query_session.len();
                for item in pending_query_session {
                    match s.apply_query_session(&item.0, &item.1, &item.2, &item.3) {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("fail to apply mutation for {}", e);
                            todo!();
                        }
                    }
                }
                mutation_size += pending_databases.len();
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
                if mutation_size > 0 {
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
