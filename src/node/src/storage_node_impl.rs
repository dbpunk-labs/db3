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
use db3_crypto::account_id::AccountId;
use db3_crypto::verifier::Verifier;
use db3_proto::db3_account_proto::Account;
use db3_proto::db3_node_proto::{
    storage_node_server::StorageNode, BatchGetKey, GetAccountRequest, GetKeyRequest,
    GetKeyResponse, GetSessionInfoRequest, GetSessionInfoResponse, QueryBillRequest,
    QueryBillResponse, RestartSessionRequest, RestartSessionResponse,
};
use db3_sdk::session_sdk::{SessionManager, SessionStatus};
use ethereum_types::Address as AccountAddress;
use ethereum_types::Address;
use prost::Message;
use std::boxed::Box;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

pub struct StorageNodeImpl {
    store: Arc<Mutex<Pin<Box<AuthStorage>>>>,
    sessions: Arc<Mutex<Pin<Box<HashMap<Address, SessionManager>>>>>,
}

impl StorageNodeImpl {
    pub fn new(store: Arc<Mutex<Pin<Box<AuthStorage>>>>) -> Self {
        Self {
            store,
            sessions: Arc::new(Mutex::new(Box::pin(HashMap::new()))),
        }
    }
}

#[tonic::async_trait]
impl StorageNode for StorageNodeImpl {
    async fn restart_query_session(
        &self,
        request: Request<RestartSessionRequest>,
    ) -> std::result::Result<Response<RestartSessionResponse>, Status> {
        let r = request.into_inner();
        let account_id = Verifier::verify(r.query_session_info.as_ref(), r.signature.as_ref())
            .map_err(|e| Status::internal(format!("{}", e)))?;
        match self.sessions.lock() {
            Ok(mut sess_map) => {
                // Takes a reference and returns Option<&V>
                let session_id = match sess_map.get_mut(&account_id.addr) {
                    Some(sess) => {
                        match sess.get_session_status() {
                            SessionStatus::READY => {
                                // no need to apply query session bill
                                sess.get_session_id()
                            }
                            SessionStatus::RUNNING | SessionStatus::BLOCKED => {
                                // TODO(chenjing): apply query session bill
                                sess.reset_session();
                                sess.get_session_id()
                            }
                        }
                    }
                    _ => {
                        // no need to apply query session bill
                        sess_map
                            .insert(account_id.addr, SessionManager::new())
                            .unwrap()
                            .get_session_id()
                    }
                };

                Ok(Response::new(RestartSessionResponse {
                    // session id --> i64
                    session: session_id,
                }))
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }

    async fn query_bill(
        &self,
        request: Request<QueryBillRequest>,
    ) -> std::result::Result<Response<QueryBillResponse>, Status> {
        let r = request.into_inner();
        match self.store.lock() {
            Ok(s) => {
                let bills = s
                    .get_bills(r.height, r.start_id, r.end_id)
                    .map_err(|e| Status::internal(format!("{}", e)))?;
                Ok(Response::new(QueryBillResponse { bills }))
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }

    async fn get_key(
        &self,
        request: Request<GetKeyRequest>,
    ) -> std::result::Result<Response<GetKeyResponse>, Status> {
        let r = request.into_inner();
        let account_id = Verifier::verify(r.batch_get.as_ref(), r.signature.as_ref())
            .map_err(|e| Status::internal(format!("{}", e)))?;
        match self.sessions.lock() {
            Ok(mut sess_map) => {
                // Takes a reference and returns Option<&V>
                if !sess_map.contains_key(&account_id.addr) {
                    sess_map.insert(account_id.addr, SessionManager::new());
                }
                let session = sess_map.get_mut(&account_id.addr).unwrap();
                if let SessionStatus::RUNNING = session.check_session_status() {
                    let batch_get_key =
                        BatchGetKey::decode(r.batch_get.as_ref()).map_err(|_| {
                            Status::internal("fail to decode batch get key".to_string())
                        })?;
                    match self.store.lock() {
                        Ok(s) => {
                            let values = s
                                .batch_get(&account_id.addr, &batch_get_key)
                                .map_err(|e| Status::internal(format!("{}", e)))?;

                            session.increase_query(1);
                            Ok(Response::new(GetKeyResponse {
                                signature: vec![],
                                batch_get_values: Some(values.to_owned()),
                            }))
                        }
                        Err(e) => Err(Status::internal(format!("{}", e))),
                    }
                } else {
                    return Err(Status::permission_denied(
                        "Fail to query bill in this session. Please restart query session",
                    ));
                }
            }
            Err(e) => {
                return Err(Status::internal(format!("{}", e)));
            }
        }
    }
    async fn get_account(
        &self,
        request: Request<GetAccountRequest>,
    ) -> std::result::Result<Response<Account>, Status> {
        let r = request.into_inner();
        let addr = AccountAddress::from_str(r.addr.as_str())
            .map_err(|e| Status::internal(format!("{}", e)))?;
        match self.store.lock() {
            Ok(s) => {
                let account = s
                    .get_account(&addr)
                    .map_err(|e| Status::internal(format!("{}", e)))?;
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
        let addr = AccountAddress::from_str(r.addr.as_str())
            .map_err(|e| Status::internal(format!("{}", e)))?;
        match self.sessions.lock() {
            Ok(mut sess_map) => {
                if let Some(mut sess) = sess_map.get_mut(&addr) {
                    Ok(Response::new(GetSessionInfoResponse {
                        id: sess.get_session_id(),
                        start_time: sess.get_start_time(),
                        query_count: sess.get_session_query_count(),
                        status: format!("{:?}", sess.check_session_status()),
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
