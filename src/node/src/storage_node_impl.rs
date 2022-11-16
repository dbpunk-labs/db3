//
// stroage_node_impl.rs
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

use super::auth_storage::AuthStorage;
use crate::node_context::NodeContext;
use db3_crypto::account_id::AccountId;
use db3_crypto::verifier::Verifier;
use db3_proto::db3_account_proto::Account;
use db3_proto::db3_node_proto::{
    storage_node_server::StorageNode, BatchGetKey, CloseSessionRequest, CloseSessionResponse,
    GetAccountRequest, GetKeyRequest, GetKeyResponse, GetSessionInfoRequest,
    GetSessionInfoResponse, OpenSessionRequest, OpenSessionResponse, QueryBillKey,
    QueryBillRequest, QueryBillResponse, QuerySessionInfo, SessionIdentifier,
};
use db3_session::session_manager::SessionManager;
use db3_session::session_manager::DEFAULT_SESSION_PERIOD;
use db3_session::session_manager::DEFAULT_SESSION_QUERY_LIMIT;
use ethereum_types::Address as AccountAddress;
use ethereum_types::Address;
use prost::Message;
use std::borrow::BorrowMut;
use std::boxed::Box;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

pub struct StorageNodeImpl {
    node_ctx: Arc<Mutex<Pin<Box<NodeContext>>>>,
}

impl StorageNodeImpl {
    pub fn new(node_ctx: Arc<Mutex<Pin<Box<NodeContext>>>>) -> Self {
        Self { node_ctx }
    }
}

#[tonic::async_trait]
impl StorageNode for StorageNodeImpl {
    async fn open_query_session(
        &self,
        request: Request<OpenSessionRequest>,
    ) -> std::result::Result<Response<OpenSessionResponse>, Status> {
        let r = request.into_inner();
        let account_id = Verifier::verify(r.addr.as_ref(), r.signature.as_ref())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;
        match self.node_ctx.lock() {
            Ok(mut ctx) => {
                let sess_store = ctx.get_session_store();
                match sess_store.add_new_session(account_id.addr) {
                    Ok(session_id) => {
                        // Takes a reference and returns Option<&V>
                        Ok(Response::new(OpenSessionResponse {
                            // session id --> i64
                            session_id,
                            session_timeout_second: DEFAULT_SESSION_PERIOD,
                            max_query_limit: DEFAULT_SESSION_QUERY_LIMIT,
                        }))
                    }
                    Err(e) => Err(Status::internal(format!("{}", e))),
                }
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }
    async fn close_query_session(
        &self,
        request: Request<CloseSessionRequest>,
    ) -> std::result::Result<Response<CloseSessionResponse>, Status> {
        let r = request.into_inner();
        let account_id = Verifier::verify(r.query_session_info.as_ref(), r.signature.as_ref())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;
        let query_session_info = QuerySessionInfo::decode(r.query_session_info.as_ref())
            .map_err(|_| Status::internal("fail to decode query_session_info ".to_string()))?;
        match self.node_ctx.lock() {
            Ok(mut ctx) => {
                let mut sess_store = ctx.get_session_store();
                // Takes a reference and returns Option<&V>
                match sess_store.remove_session(account_id.addr, query_session_info.id) {
                    Ok(id) => {
                        Ok(Response::new(CloseSessionResponse {
                            // session id --> i64
                            session_id: id,
                        }))
                    }
                    Err(e) => Err(Status::internal(format!("{}", e))),
                }
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }

    async fn query_bill(
        &self,
        request: Request<QueryBillRequest>,
    ) -> std::result::Result<Response<QueryBillResponse>, Status> {
        let r = request.into_inner();
        let account_id = Verifier::verify(r.query_bill_key.as_ref(), r.signature.as_ref())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;
        let query_bill_key = QueryBillKey::decode(r.query_bill_key.as_ref())
            .map_err(|_| Status::internal("fail to decode query_bill_key ".to_string()))?;
        match self.node_ctx.lock() {
            Ok(mut ctx) => {
                match ctx
                    .get_session_store()
                    .get_session_mut(account_id.addr, query_bill_key.session_id)
                {
                    Some(session) => {
                        if !session.check_session_running() {
                            return Err(Status::permission_denied(
                                "Fail to query in this session. Please restart query session",
                            ));
                        }
                    }
                    None => {
                        return Err(Status::internal("Fail to create session"));
                    }
                }
                let bills = ctx
                    .borrow_mut()
                    .get_auth_store()
                    .get_bills(
                        query_bill_key.height,
                        query_bill_key.start_id,
                        query_bill_key.end_id,
                    )
                    .map_err(|e| Status::internal(format!("{:?}", e)))?;
                ctx.get_session_store()
                    .get_session_mut(account_id.addr, query_bill_key.session_id)
                    .unwrap()
                    .increase_query(1);
                Ok(Response::new(QueryBillResponse { bills }))
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }

    // Batch query with keys
    async fn get_key(
        &self,
        request: Request<GetKeyRequest>,
    ) -> std::result::Result<Response<GetKeyResponse>, Status> {
        let r = request.into_inner();
        let account_id = Verifier::verify(r.batch_get.as_ref(), r.signature.as_ref())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;
        match self.node_ctx.lock() {
            Ok(mut ctx) => {
                let batch_get_key = BatchGetKey::decode(r.batch_get.as_ref())
                    .map_err(|_| Status::internal("fail to decode batch get key".to_string()))?;
                match ctx
                    .get_session_store()
                    .get_session_mut(account_id.addr, batch_get_key.session)
                {
                    Some(session) => {
                        if !session.check_session_running() {
                            return Err(Status::permission_denied(
                                "Fail to query in this session. Please restart query session",
                            ));
                        }
                    }
                    None => return Err(Status::internal("Fail to create session")),
                }
                let values = ctx
                    .get_auth_store()
                    .batch_get(&account_id.addr, &batch_get_key)
                    .map_err(|e| Status::internal(format!("{:?}", e)))?;

                // TODO(chenjing): evaluate query ops based on keys size
                ctx.get_session_store()
                    .get_session_mut(account_id.addr, batch_get_key.session)
                    .unwrap()
                    .increase_query(1);
                Ok(Response::new(GetKeyResponse {
                    signature: vec![],
                    batch_get_values: Some(values.to_owned()),
                }))
            }
            Err(e) => Err(Status::internal(format!("Fail to get key {}", e))),
        }
    }
    async fn get_account(
        &self,
        request: Request<GetAccountRequest>,
    ) -> std::result::Result<Response<Account>, Status> {
        let r = request.into_inner();
        let addr = AccountAddress::from_str(r.addr.as_str())
            .map_err(|e| Status::internal(format!("{}", e)))?;
        match self.node_ctx.lock() {
            Ok(mut ctx) => {
                let account = ctx
                    .get_auth_store()
                    .get_account(&addr)
                    .map_err(|e| Status::internal(format!("{:?}", e)))?;
                Ok(Response::new(account))
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }
    async fn get_session_info(
        &self,
        request: Request<GetSessionInfoRequest>,
    ) -> std::result::Result<Response<GetSessionInfoResponse>, Status> {
        let r = request.into_inner();
        let account_id = Verifier::verify(r.session_identifier.as_ref(), r.signature.as_ref())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;
        let session_identifier = SessionIdentifier::decode(r.session_identifier.as_ref())
            .map_err(|_| Status::internal("fail to decode session_identifier".to_string()))?;
        let session_id = session_identifier.session_id;
        match self.node_ctx.lock() {
            Ok(mut ctx) => {
                if let Some(sess) = ctx
                    .get_session_store()
                    .get_session_mut(account_id.addr, session_id)
                {
                    sess.check_session_status();
                    Ok(Response::new(GetSessionInfoResponse {
                        signature: vec![],
                        session_info: Some(sess.get_session_info()),
                    }))
                } else {
                    Err(Status::not_found("not found query session"))
                }
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
