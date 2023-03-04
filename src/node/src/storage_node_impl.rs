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
use db3_crypto::db3_address::DB3Address;
use db3_crypto::db3_signer::Db3MultiSchemeSigner;
use db3_crypto::{db3_verifier::DB3Verifier, id::DbId, id::DocumentId};
use ethers::core::types::transaction::eip712::{Eip712, TypedData};

use bytes::BytesMut;
use db3_proto::db3_base_proto::{BroadcastMeta, ChainId, ChainRole};
use db3_proto::db3_event_proto::EventMessage;
use db3_proto::db3_mutation_proto::{PayloadType, WriteRequest};
use db3_proto::db3_node_proto::{
    storage_node_server::StorageNode, BroadcastRequest, BroadcastResponse, CloseSessionRequest,
    CloseSessionResponse, GetAccountRequest, GetAccountResponse, GetDocumentRequest,
    GetDocumentResponse, GetSessionInfoRequest, GetSessionInfoResponse, NetworkStatus,
    OpenSessionRequest, OpenSessionResponse, QueryBillRequest, QueryBillResponse, RunQueryRequest,
    RunQueryResponse, ShowDatabaseRequest, ShowDatabaseResponse, ShowNetworkStatusRequest,
    SubscribeRequest,
};
use db3_proto::db3_session_proto::{OpenSessionPayload, QuerySession, QuerySessionInfo};
use db3_session::query_session_verifier;
use db3_session::session_manager::DEFAULT_SESSION_PERIOD;
use db3_session::session_manager::DEFAULT_SESSION_QUERY_LIMIT;
use ethers::types::Bytes;
use prost::Message;
use std::boxed::Box;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use tendermint_rpc::Client;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

pub struct StorageNodeImpl {
    context: Context,
    signer: Db3MultiSchemeSigner,
}

impl StorageNodeImpl {
    pub fn new(context: Context, singer: Db3MultiSchemeSigner) -> Self {
        Self {
            context,
            signer: singer,
        }
    }
}

#[tonic::async_trait]
impl StorageNode for StorageNodeImpl {
    type SubscribeStream = ReceiverStream<std::result::Result<EventMessage, Status>>;
    async fn show_database(
        &self,
        request: Request<ShowDatabaseRequest>,
    ) -> std::result::Result<Response<ShowDatabaseResponse>, Status> {
        let show_database_req = request.into_inner();
        match self.context.node_store.lock() {
            Ok(mut node_store) => {
                // get database id
                let address_ref: &str = show_database_req.address.as_ref();
                let db_id = DbId::try_from(address_ref)
                    .map_err(|e| Status::internal(format!("invalid database address {e}")))?;
                // validate the session id
                match node_store
                    .get_session_store()
                    .get_session_mut(&show_database_req.session_token)
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
                let db = node_store
                    .get_auth_store()
                    .get_database(&db_id)
                    .map_err(|e| Status::internal(format!("{:?}", e)))?;
                node_store
                    .get_session_store()
                    .get_session_mut(&show_database_req.session_token)
                    .unwrap()
                    .increase_query(1);
                Ok(Response::new(ShowDatabaseResponse { db }))
            }
            Err(e) => Err(Status::internal(format!("Fail to get lock {}", e))),
        }
    }

    async fn get_document(
        &self,
        request: Request<GetDocumentRequest>,
    ) -> std::result::Result<Response<GetDocumentResponse>, Status> {
        let get_document_request = request.into_inner();
        let id = DocumentId::try_from_base64(get_document_request.id.as_str())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;
        match self.context.node_store.lock() {
            Ok(mut node_store) => {
                // get database id
                // validate the session id
                match node_store
                    .get_session_store()
                    .get_session_mut(&get_document_request.session_token)
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
                match node_store.get_auth_store().get_document(&id) {
                    Ok(document) => Ok(Response::new(GetDocumentResponse { document })),
                    Err(e) => Err(Status::internal(format!("fail to get document {:?}", e))),
                }
            }
            Err(e) => Err(Status::internal(format!("Fail to get lock {}", e))),
        }
    }

    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> std::result::Result<Response<RunQueryResponse>, Status> {
        let run_query_req = request.into_inner();
        match self.context.node_store.lock() {
            Ok(mut node_store) => {
                // get database id
                let address_ref: &str = run_query_req.address.as_ref();
                let db_id = DbId::try_from(address_ref)
                    .map_err(|e| Status::internal(format!("invalid database address {e}")))?;
                // validate the session id
                match node_store
                    .get_session_store()
                    .get_session_mut(&run_query_req.session_token)
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

                match &run_query_req.query {
                    Some(query) => {
                        let documents = node_store
                            .get_auth_store()
                            .run_query(&db_id, &query)
                            .map_err(|e| Status::internal(format!("{:?}", e)))?;
                        node_store
                            .get_session_store()
                            .get_session_mut(&run_query_req.session_token)
                            .unwrap()
                            .increase_query(1);
                        Ok(Response::new(RunQueryResponse { documents }))
                    }
                    None => return Err(Status::internal("Fail to run with none query")),
                }
            }
            Err(e) => Err(Status::internal(format!("Fail to get lock {}", e))),
        }
    }

    async fn open_query_session(
        &self,
        request: Request<OpenSessionRequest>,
    ) -> std::result::Result<Response<OpenSessionResponse>, Status> {
        let r = request.into_inner();
        let (account_id, session) = match r.payload_type {
            // Typeddatapayload
            3 => {
                info!("get open session request");
                let typed_data = serde_json::from_slice::<TypedData>(r.payload.as_ref())
                    .map_err(|e| Status::internal(format!("bad typed data format for {e}")))?;
                let hashed_message = typed_data.encode_eip712().map_err(|e| {
                    Status::internal(format!("encode typed data to hash error {e}"))
                })?;
                let account_id = DB3Verifier::verify_hashed(&hashed_message, r.signature.as_ref())
                    .map_err(|e| Status::internal(format!("bad typed data signature for {e}")))?;
                let typed_payload = typed_data
                    .message
                    .get("payload")
                    .ok_or(Status::internal("no typed payload was found".to_string()))?;
                let binary_payload: Bytes = serde_json::from_value(typed_payload.to_owned())
                    .map_err(|e| Status::internal(format!("invalid payload  for err {e}")))?;
                let session = OpenSessionPayload::decode(binary_payload.as_ref()).map_err(|e| {
                    Status::internal(format!("fail to decode open session request for {e} "))
                })?;
                info!("session account {}", account_id.to_hex());
                (account_id, session)
            }
            // Querysessionpayload
            _ => {
                let account_id = DB3Verifier::verify(r.payload.as_ref(), r.signature.as_ref())
                    .map_err(|e| Status::internal(format!("bad signature for {e}")))?;
                let payload = OpenSessionPayload::decode(r.payload.as_ref()).map_err(|e| {
                    Status::internal(format!("fail to decode open session request for {e} "))
                })?;
                (account_id, payload)
            }
        };
        let header = session.header;
        match self.context.node_store.lock() {
            Ok(mut node_store) => {
                let sess_store = node_store.get_session_store();
                match sess_store.add_new_session(&header, session.start_time, account_id.addr) {
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
        let (_, session) = match r.payload_type {
            // Typeddatapayload
            3 => {
                let typed_data = serde_json::from_slice::<TypedData>(r.payload.as_ref())
                    .map_err(|e| Status::internal(format!("bad typed data format for {e}")))?;
                let hashed_message = typed_data.encode_eip712().map_err(|e| {
                    Status::internal(format!("encode typed data to hash error {e}"))
                })?;
                let account_id = DB3Verifier::verify_hashed(&hashed_message, r.signature.as_ref())
                    .map_err(|e| Status::internal(format!("bad typed data signature for {e}")))?;
                let typed_payload = typed_data
                    .message
                    .get("payload")
                    .ok_or(Status::internal("no typed payload was found".to_string()))?;
                let binary_payload: Bytes = serde_json::from_value(typed_payload.to_owned())
                    .map_err(|e| Status::internal(format!("invalid payload  for err {e}")))?;
                let session = QuerySessionInfo::decode(binary_payload.as_ref()).map_err(|e| {
                    Status::internal(format!("fail to decode open session request for {e} "))
                })?;
                (account_id, session)
            }
            // Querysessionpayload
            _ => {
                let account_id = DB3Verifier::verify(r.payload.as_ref(), r.signature.as_ref())
                    .map_err(|e| Status::internal(format!("bad signature for {e}")))?;
                let payload = QuerySessionInfo::decode(r.payload.as_ref()).map_err(|e| {
                    Status::internal(format!("fail to decode open session request for {e} "))
                })?;
                (account_id, payload)
            }
        };
        let node_query_session_info = match self.context.node_store.lock() {
            Ok(mut node_store) => {
                let sess_store = node_store.get_session_store();
                // Verify query session sdk
                match sess_store.get_session_mut(&r.session_token) {
                    Some(sess) => {
                        if !query_session_verifier::check_query_session_info(
                            &sess.get_session_info(),
                            &session,
                        ) {
                            return Err(Status::invalid_argument(format!(
                                "query session verify fail. expect query count {} but {}",
                                sess.get_session_query_count(),
                                session.query_count
                            )));
                        }
                    }
                    None => {
                        return Err(Status::not_found(format!(
                            "session {} not found in the session store",
                            r.session_token
                        )));
                    }
                }
                // Takes a reference and returns Option<&V>
                let sess = sess_store
                    .remove_session(&r.session_token)
                    .map_err(|e| Status::internal(format!("{}", e)))
                    .unwrap();
                Some(sess.get_session_info())
            }
            Err(e) => return Err(Status::internal(format!("{}", e))),
        };
        // Generate Nonce
        let nonce = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(_) => 0,
        };
        let meta = BroadcastMeta {
            //TODO get from network
            nonce,
            //TODO use config
            chain_id: ChainId::DevNet.into(),
            //TODO use config
            chain_role: ChainRole::StorageShardChain.into(),
        };
        let query_session = QuerySession {
            payload: r.payload.to_vec(),
            client_signature: r.signature.to_vec(),
            meta: Some(meta),
            payload_type: r.payload_type,
        };
        // Submit query session
        let mut mbuf = BytesMut::with_capacity(1024 * 4);
        query_session.encode(&mut mbuf).map_err(|e| {
            Status::internal(format!("fail to submit query session with error {}", e))
        })?;
        let mbuf = mbuf.freeze();
        let signature = self.signer.sign(mbuf.as_ref()).map_err(|e| {
            Status::internal(format!("fail to submit query session with error {e}"))
        })?;
        let request = WriteRequest {
            signature: signature.as_ref().to_vec().to_owned(),
            payload: mbuf.as_ref().to_vec().to_owned(),
            payload_type: PayloadType::QuerySessionPayload.into(),
        };
        //TODO add the capacity to mutation sdk configuration
        let mut buf = BytesMut::with_capacity(1024 * 4);
        request.encode(&mut buf).map_err(|e| {
            Status::internal(format!("fail to submit query session with error {e}"))
        })?;
        let buf = buf.freeze();
        let r = BroadcastRequest {
            body: buf.as_ref().to_vec(),
        };
        let request = tonic::Request::new(r);
        let response = self
            .broadcast(request)
            .await
            .map_err(|e| Status::internal(format!("fail to submit query session with error {e}")))?
            .into_inner();
        // let base64_byte = base64::encode(response.hash);
        // let hash = String::from_utf8_lossy(base64_byte.as_ref()).to_string();
        // TODO(chenjing): sign
        Ok(Response::new(CloseSessionResponse {
            query_session_info: node_query_session_info,
            hash: response.hash,
        }))
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
                    .get_bills(query_bill_key.height)
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

    async fn get_account(
        &self,
        request: Request<GetAccountRequest>,
    ) -> std::result::Result<Response<GetAccountResponse>, Status> {
        let r: GetAccountRequest = request.into_inner();
        if r.addr.len() <= 0 {
            info!("empty account");
            return Err(Status::invalid_argument("empty address".to_string()));
        }
        let addr_ref: &[u8] = r.addr.as_ref();
        let addr = DB3Address::try_from(addr_ref).map_err(|e| Status::internal(format!("{e}")))?;
        match self.context.node_store.lock() {
            Ok(mut node_store) => {
                let account = node_store
                    .get_auth_store()
                    .get_account(&addr)
                    .map_err(|e| Status::internal(format!("{e}")))?;
                let response = GetAccountResponse { account };
                Ok(Response::new(response))
            }
            Err(e) => Err(Status::internal(format!("{e}"))),
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
                        session_status: sess.get_session_status_as_i32(),
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
            .broadcast_tx_async(r.body)
            .await
            .map_err(|e| Status::internal(format!("{}", e)))?;
        Ok(Response::new(BroadcastResponse {
            hash: response.hash.as_ref().to_vec(),
        }))
    }
    async fn show_network_status(
        &self,
        _request: Request<ShowNetworkStatusRequest>,
    ) -> std::result::Result<Response<NetworkStatus>, Status> {
        match self.context.node_store.lock() {
            Ok(node_store) => {
                let state = node_store.get_state();
                let status = NetworkStatus {
                    total_database_count: state.total_database_count.load(Ordering::Relaxed),
                    total_collection_count: state.total_collection_count.load(Ordering::Relaxed),
                    total_document_count: state.total_document_count.load(Ordering::Relaxed),
                    total_account_count: state.total_account_count.load(Ordering::Relaxed),
                    total_mutation_count: state.total_mutation_count.load(Ordering::Relaxed),
                    total_session_count: state.total_session_count.load(Ordering::Relaxed),
                    total_storage_in_bytes: state.total_storage_bytes.load(Ordering::Relaxed),
                };

                Ok(Response::new(status))
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }
    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> std::result::Result<Response<Self::SubscribeStream>, Status> {
        Err(Status::internal(format!("")))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
