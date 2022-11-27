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

use super::context::Context;
use db3_crypto::verifier::Verifier;
use db3_proto::db3_account_proto::Account;
use db3_proto::db3_node_proto::{
    storage_node_server::StorageNode, BroadcastRequest, BroadcastResponse,
    CloseSessionPayload, CloseSessionRequest, CloseSessionResponse, GetAccountRequest,
    GetKeyRequest, GetKeyResponse, GetSessionInfoRequest, GetSessionInfoResponse,
    OpenSessionRequest, OpenSessionResponse, QueryBillRequest, QueryBillResponse,
};
use db3_session::session_manager::DEFAULT_SESSION_PERIOD;
use db3_session::session_manager::DEFAULT_SESSION_QUERY_LIMIT;
use ethereum_types::Address as AccountAddress;
use prost::Message;
use std::boxed::Box;
use std::str::FromStr;
use tendermint_rpc::Client;
use tonic::{Request, Response, Status};

pub struct StorageNodeImpl {
    context: Context,
}

impl StorageNodeImpl {
    pub fn new(context: Context) -> Self {
        Self { context }
    }
}

#[tonic::async_trait]
impl StorageNode for StorageNodeImpl {
    async fn open_query_session(
        &self,
        request: Request<OpenSessionRequest>,
    ) -> std::result::Result<Response<OpenSessionResponse>, Status> {
        let r = request.into_inner();
        let account_id = Verifier::verify(r.header.as_ref(), r.signature.as_ref())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;
        match self.context.node_store.lock() {
            Ok(mut node_store) => {
                let sess_store = node_store.get_session_store();
                match sess_store.add_new_session(account_id.addr) {
                    Ok((session_token, query_session_info)) => {
                        // Takes a reference and returns Option<&V>
                        Ok(Response::new(OpenSessionResponse {
                            query_session_info: Some(query_session_info),
                            session_token,
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
        Verifier::verify(r.payload.as_ref(), r.signature.as_ref())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;
        let payload = CloseSessionPayload::decode(r.payload.as_ref())
            .map_err(|_| Status::internal("fail to decode query_session_info ".to_string()))?;
        match self.context.node_store.lock() {
            Ok(mut node_store) => {
                let sess_store = node_store.get_session_store();

                // Verify query session sdk
                match sess_store.get_session_mut(&payload.session_token) {
                    Some(sess) => {
                        let query_session_info = &payload.session_info.unwrap();
                        if sess.get_session_query_count() != query_session_info.query_count {
                            return Err(Status::invalid_argument(format!(
                                "query session verify fail. expect query count {} but {}",
                                sess.get_session_query_count(),
                                query_session_info.query_count
                            )));
                        }
                    }
                    None => {
                        return Err(Status::not_found(format!(
                            "session {} not found in the session store",
                            payload.session_token
                        )));
                    }
                }

                // Submit query session

                // Takes a reference and returns Option<&V>
                let sess = sess_store
                    .remove_session(&payload.session_token)
                    .map_err(|e| Status::internal(format!("{}", e)))
                    .unwrap();
                // TODO(chenjing): sign
                Ok(Response::new(CloseSessionResponse {
                    query_session_info: Some(sess.get_session_info()),
                }))
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }

    async fn query_bill(
        &self,
        request: Request<QueryBillRequest>,
    ) -> std::result::Result<Response<QueryBillResponse>, Status> {
        let query_bill_key = request.into_inner().query_bill_key.unwrap();
        match self.context.node_store.lock() {
            Ok(mut node_store) => {
                match node_store
                    .get_session_store()
                    .get_session_mut(&query_bill_key.session_token)
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
                let bills = node_store
                    .get_auth_store()
                    .get_bills(
                        query_bill_key.height,
                        query_bill_key.start_id,
                        query_bill_key.end_id,
                    )
                    .map_err(|e| Status::internal(format!("{:?}", e)))?;
                node_store
                    .get_session_store()
                    .get_session_mut(&query_bill_key.session_token)
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
        let batch_get_key = request.into_inner().batch_get.unwrap();
        match self.context.node_store.lock() {
            Ok(mut node_store) => {
                match node_store
                    .get_session_store()
                    .get_session_mut(&batch_get_key.session_token)
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
                let addr = node_store
                    .get_session_store()
                    .get_address(&batch_get_key.session_token);

                if addr.is_none() {
                    return Err(Status::internal(format!(
                        "not address found related to current token {}",
                        &batch_get_key.session_token
                    )));
                }
                let values = node_store
                    .get_auth_store()
                    .batch_get(&addr.unwrap(), &batch_get_key)
                    .map_err(|e| Status::internal(format!("{:?}", e)))?;

                // TODO(chenjing): evaluate query ops based on keys size
                node_store
                    .get_session_store()
                    .get_session_mut(&batch_get_key.session_token)
                    .unwrap()
                    .increase_query(1);
                Ok(Response::new(GetKeyResponse {
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
        match self.context.node_store.lock() {
            Ok(mut node_store) => {
                let account = node_store
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
        let session_identifier = request.into_inner().session_identifier.unwrap();
        let session_token = session_identifier.session_token;
        match self.context.node_store.lock() {
            Ok(mut node_store) => {
                if let Some(sess) = node_store
                    .get_session_store()
                    .get_session_mut(&session_token)
                {
                    sess.check_session_status();
                    Ok(Response::new(GetSessionInfoResponse {
                        session_info: Some(sess.get_session_info()),
                    }))
                } else {
                    Err(Status::not_found("not found query session"))
                }
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }

    /// handle broadcast mutations and query sessionss
    async fn broadcast(
        &self,
        request: Request<BroadcastRequest>,
    ) -> std::result::Result<Response<BroadcastResponse>, Status> {
        let r = request.into_inner();
        let response = self
            .context
            .client
            .broadcast_tx_async(r.body.into())
            .await
            .map_err(|e| Status::internal(format!("{}", e)))?;
        Ok(Response::new(BroadcastResponse {
            hash: response.hash.as_ref().to_vec(),
        }))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
