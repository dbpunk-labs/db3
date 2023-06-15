//
// bill_sdk.rs
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

use bytes::BytesMut;
use chrono::Utc;
use db3_crypto::{db3_address::DB3Address, db3_signer::Db3MultiSchemeSigner};
use db3_proto::db3_account_proto::Account;
use db3_proto::db3_base_proto::{BroadcastMeta, ChainId, ChainRole};
use db3_proto::db3_bill_proto::Bill;
use db3_proto::db3_database_proto::structured_query::{Limit, Projection};
use db3_proto::db3_database_proto::{Database, Document, StructuredQuery};
use db3_proto::db3_mutation_proto::PayloadType;
use db3_proto::db3_node_proto::{
    storage_node_client::StorageNodeClient, BlockRequest, BlockResponse, BlockType,
    CloseSessionRequest, GetAccountRequest, GetDocumentRequest, GetSessionInfoRequest,
    NetworkStatus, OpenSessionRequest, OpenSessionResponse, QueryBillKey, QueryBillRequest,
    RunQueryRequest, RunQueryResponse, SessionIdentifier, ShowDatabaseRequest,
    ShowNetworkStatusRequest, SubscribeRequest,
};

use db3_proto::db3_event_proto::{
    event_filter, event_message, BlockEventFilter, EventFilter, EventMessage, EventType,
    MutationEventFilter, Subscription,
};
use db3_proto::db3_session_proto::{OpenSessionPayload, QuerySessionInfo};
use db3_session::session_manager::{SessionPool, SessionStatus};
use ethers::core::types::{
    transaction::eip712::{EIP712Domain, TypedData, Types},
    Bytes,
};

use hex;
use num_traits::cast::FromPrimitive;
use prost::Message;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tonic::{Status, Streaming};
use uuid::Uuid;

pub struct StoreSDK {
    client: Arc<StorageNodeClient<tonic::transport::Channel>>,
    signer: Db3MultiSchemeSigner,
    session_pool: SessionPool,
    types: Types,
    use_typed_format: bool,
    query_session_enabled: Arc<AtomicBool>,
}

impl StoreSDK {
    pub fn new(
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        signer: Db3MultiSchemeSigner,
        use_typed_format: bool,
    ) -> Self {
        let json = serde_json::json!({
          "EIP712Domain": [
          ],
          "Message":[
          {"name":"payload", "type":"bytes"},
          {"name":"payloadType", "type":"string"}
          ]
        });
        let types: Types = serde_json::from_value(json).unwrap();
        Self {
            client,
            signer,
            session_pool: SessionPool::new(),
            types,
            use_typed_format,
            query_session_enabled: Arc::new(AtomicBool::new(true)),
        }
    }

    pub async fn check_node(&self) -> std::result::Result<(), Status> {
        let state = self.get_state().await?;
        self.query_session_enabled.store(
            state.query_session_enabled,
            std::sync::atomic::Ordering::Relaxed,
        );
        Ok(())
    }

    async fn keep_session(&mut self, force: bool) -> std::result::Result<String, Status> {
        //TODO remove force parameter
        if !self
            .query_session_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
            && !force
        {
            return Ok("".to_string());
        }

        if let Some(token) = self.session_pool.get_last_token() {
            match self.session_pool.get_session_mut(token.as_ref()) {
                Some(session) => {
                    if session.get_session_query_count() > 2000 {
                        // close session
                        self.close_session_internal(&token, self.use_typed_format)
                            .await?;
                        let response = self.open_session().await?;
                        Ok(response.session_token)
                    } else {
                        Ok(token)
                    }
                }
                None => Err(Status::not_found(format!(
                    "Fail to query, session with token {token} not found"
                ))),
            }
        } else {
            let response = self.open_session().await?;
            Ok(response.session_token)
        }
    }

    /// show document with given db addr and collection name
    pub async fn list_documents(
        &mut self,
        addr: &str,
        collection_name: &str,
        limit: Option<i32>,
    ) -> std::result::Result<RunQueryResponse, Status> {
        self.run_query(
            addr,
            StructuredQuery {
                collection_name: collection_name.to_string(),
                limit: match limit {
                    Some(v) => Some(Limit { limit: v }),
                    None => None,
                },
                select: Some(Projection { fields: vec![] }),
                r#where: None,
            },
        )
        .await
    }

    /// get the document with a base64 format id
    pub async fn get_document(
        &mut self,
        id: &str,
    ) -> std::result::Result<Option<Document>, Status> {
        if self
            .query_session_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            let token = self.keep_session(false).await?;
            match self.session_pool.get_session_mut(token.as_ref()) {
                Some(session) => {
                    if session.check_session_running() {
                        let r = GetDocumentRequest {
                            session_token: token.to_string(),
                            id: id.to_string(),
                        };
                        let request = tonic::Request::new(r);
                        let mut client = self.client.as_ref().clone();
                        let response = client.get_document(request).await?.into_inner();
                        session.increase_query(1);
                        Ok(response.document)
                    } else {
                        Err(Status::permission_denied(
                            "Fail to query in this session. Please restart query session",
                        ))
                    }
                }
                None => Err(Status::not_found(format!(
                    "Fail to query, session with token {token} not found"
                ))),
            }
        } else {
            let r = GetDocumentRequest {
                session_token: "".to_string(),
                id: id.to_string(),
            };

            let request = tonic::Request::new(r);
            let mut client = self.client.as_ref().clone();
            let response = client.get_document(request).await?.into_inner();
            Ok(response.document)
        }
    }

    ///
    /// get the information of database with a hex format address
    ///
    pub async fn get_database(
        &mut self,
        addr: &str,
    ) -> std::result::Result<Option<Database>, Status> {
        if self
            .query_session_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            let token = self.keep_session(false).await?;
            match self.session_pool.get_session_mut(token.as_ref()) {
                Some(session) => {
                    if session.check_session_running() {
                        let r = ShowDatabaseRequest {
                            session_token: token.to_string(),
                            address: addr.to_string(),
                            owner_address: "".to_string(),
                        };
                        let request = tonic::Request::new(r);
                        let mut client = self.client.as_ref().clone();
                        let response = client.show_database(request).await?.into_inner();
                        session.increase_query(1);
                        if response.dbs.len() > 0 {
                            Ok(Some(response.dbs[0].clone()))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Err(Status::permission_denied(
                            "Fail to query in this session. Please restart query session",
                        ))
                    }
                }
                None => Err(Status::not_found(format!(
                    "Fail to query, session with token {token} not found"
                ))),
            }
        } else {
            let r = ShowDatabaseRequest {
                session_token: "".to_string(),
                address: addr.to_string(),
                owner_address: "".to_string(),
            };
            let request = tonic::Request::new(r);
            let mut client = self.client.as_ref().clone();
            let response = client.show_database(request).await?.into_inner();
            if response.dbs.len() > 0 {
                Ok(Some(response.dbs[0].clone()))
            } else {
                Ok(None)
            }
        }
    }

    ///
    /// get the information of database with a hex format address
    ///
    pub async fn get_my_database(
        &mut self,
        addr: &str,
    ) -> std::result::Result<Vec<Database>, Status> {
        if self
            .query_session_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            let token = self.keep_session(false).await?;
            match self.session_pool.get_session_mut(token.as_ref()) {
                Some(session) => {
                    if session.check_session_running() {
                        let r = ShowDatabaseRequest {
                            session_token: token.to_string(),
                            address: "".to_string(),
                            owner_address: addr.to_string(),
                        };
                        let request = tonic::Request::new(r);
                        let mut client = self.client.as_ref().clone();
                        let response = client.show_database(request).await?.into_inner();
                        session.increase_query(1);
                        Ok(response.dbs)
                    } else {
                        Err(Status::permission_denied(
                            "Fail to query in this session. Please restart query session",
                        ))
                    }
                }
                None => Err(Status::not_found(format!(
                    "Fail to query, session with token {token} not found"
                ))),
            }
        } else {
            let r = ShowDatabaseRequest {
                session_token: "".to_string(),
                address: "".to_string(),
                owner_address: addr.to_string(),
            };
            let request = tonic::Request::new(r);
            let mut client = self.client.as_ref().clone();
            let response = client.show_database(request).await?.into_inner();
            Ok(response.dbs)
        }
    }

    /// query the document with structure query
    pub async fn run_query(
        &mut self,
        addr: &str,
        query: StructuredQuery,
    ) -> std::result::Result<RunQueryResponse, Status> {
        if self
            .query_session_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            let token = self.keep_session(false).await?;
            match self.session_pool.get_session_mut(token.as_ref()) {
                Some(session) => {
                    if session.check_session_running() {
                        let r = RunQueryRequest {
                            session_token: token.to_string(),
                            address: addr.to_string(),
                            query: Some(query),
                        };
                        let request = tonic::Request::new(r);
                        let mut client = self.client.as_ref().clone();
                        let response = client.run_query(request).await?.into_inner();
                        session.increase_query(1);
                        Ok(response)
                    } else {
                        Err(Status::permission_denied(
                            "Fail to query in this session. Please restart query session",
                        ))
                    }
                }
                None => Err(Status::not_found(format!(
                    "Fail to query, session with token {token} not found"
                ))),
            }
        } else {
            let r = RunQueryRequest {
                session_token: "".to_string(),
                address: addr.to_string(),
                query: Some(query),
            };
            let request = tonic::Request::new(r);
            let mut client = self.client.as_ref().clone();
            let response = client.run_query(request).await?.into_inner();
            Ok(response)
        }
    }

    pub async fn subscribe_event_message(
        &self,
        all: bool,
    ) -> Result<tonic::Response<Streaming<EventMessage>>, Status> {
        let session_token = self.get_token().await?;
        let m_filter = match all {
            true => MutationEventFilter {
                sender: "".to_string(),
            },
            false => {
                let hex_addr = self.signer.get_address().unwrap().to_hex();
                MutationEventFilter { sender: hex_addr }
            }
        };
        let b_filter = BlockEventFilter {};
        let sub = Subscription {
            topics: vec![EventType::Block.into(), EventType::Mutation.into()],
            filters: vec![
                EventFilter {
                    filter: Some(event_filter::Filter::Bfilter(b_filter)),
                },
                EventFilter {
                    filter: Some(event_filter::Filter::Mfilter(m_filter)),
                },
            ],
        };
        let req = SubscribeRequest {
            session_token,
            sub: Some(sub),
        };
        let mut client = self.client.as_ref().clone();
        client.subscribe(req).await
    }
    /// open a console to subscribe the event
    pub async fn open_console(&mut self, all: bool) -> Result<(), Status> {
        let mut stream = self.subscribe_event_message(all).await?.into_inner();
        while let Some(event) = stream.message().await? {
            match event.event {
                Some(event_message::Event::MutationEvent(me)) => {
                    println!(
                        "Mutation\t{}\t{}\t{}\t{}\t{:?}",
                        me.height, me.sender, me.to, me.hash, me.collections
                    );
                }
                Some(event_message::Event::BlockEvent(be)) => {
                    println!(
                        "Block\t{}\t0x{}\t0x{}\t{}",
                        be.height,
                        hex::encode(be.block_hash),
                        hex::encode(be.app_hash),
                        be.gas
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub async fn open_session(&mut self) -> std::result::Result<OpenSessionResponse, Status> {
        let payload = OpenSessionPayload {
            header: Uuid::new_v4().to_string(),
            start_time: Utc::now().timestamp(),
        };

        let r = match self.use_typed_format {
            true => {
                let r = self.wrap_typed_open_session(&payload)?;
                r
            }
            false => {
                let r = self.wrap_proto_open_session(&payload)?;
                r
            }
        };
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();
        let response = client.open_query_session(request).await?.into_inner();
        let result = response.clone();
        match self.session_pool.insert_session_with_token(
            &result.query_session_info.unwrap(),
            &result.session_token,
            SessionStatus::Running,
        ) {
            Ok(_) => Ok(response.clone()),
            Err(e) => Err(Status::internal(format!("Fail to open session {e}"))),
        }
    }

    async fn get_token(&self) -> std::result::Result<String, Status> {
        let payload = OpenSessionPayload {
            header: Uuid::new_v4().to_string(),
            start_time: Utc::now().timestamp(),
        };

        let r = match self.use_typed_format {
            true => {
                let r = self.wrap_typed_open_session(&payload)?;
                r
            }
            false => {
                let r = self.wrap_proto_open_session(&payload)?;
                r
            }
        };
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();
        let response = client.open_query_session(request).await?.into_inner();
        Ok(response.session_token)
    }

    fn wrap_proto_open_session(
        &self,
        payload: &OpenSessionPayload,
    ) -> std::result::Result<OpenSessionRequest, Status> {
        let mut buf = BytesMut::with_capacity(1024 * 8);
        payload
            .encode(&mut buf)
            .map_err(|e| Status::internal(format!("{e}")))?;
        let buf = buf.freeze();
        let signature = self
            .signer
            .sign(buf.as_ref())
            .map_err(|e| Status::internal(format!("{e}")))?;
        let r = OpenSessionRequest {
            payload: buf.as_ref().to_vec(),
            signature: signature.as_ref().to_vec(),
            payload_type: PayloadType::QuerySessionPayload.into(),
        };
        Ok(r)
    }

    fn wrap_typed_open_session(
        &self,
        payload: &OpenSessionPayload,
    ) -> std::result::Result<OpenSessionRequest, Status> {
        let mut mbuf = BytesMut::with_capacity(1024 * 4);
        payload
            .encode(&mut mbuf)
            .map_err(|e| Status::internal(format!("{e}")))?;
        let mbuf = Bytes(mbuf.freeze());
        let mut message: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        message.insert(
            "payload".to_string(),
            serde_json::Value::from(format!("{mbuf}")),
        );
        message.insert("payloadType".to_string(), serde_json::Value::from("0"));
        let typed_data = TypedData {
            domain: EIP712Domain {
                name: None,
                version: None,
                chain_id: None,
                verifying_contract: None,
                salt: None,
            },
            types: self.types.clone(),
            primary_type: "Message".to_string(),
            message,
        };
        let signature = self
            .signer
            .sign_typed_data(&typed_data)
            .map_err(|e| Status::internal(format!("{e}")))?;
        let buf = serde_json::to_vec(&typed_data).map_err(|e| Status::internal(format!("{e}")))?;
        let r = OpenSessionRequest {
            payload: buf,
            signature,
            payload_type: PayloadType::TypedDataPayload.into(),
        };
        Ok(r)
    }

    fn wrap_proto_close_session(
        &self,
        session: &QuerySessionInfo,
        token: &str,
    ) -> std::result::Result<CloseSessionRequest, Status> {
        let mut buf = BytesMut::with_capacity(1024 * 8);
        session
            .encode(&mut buf)
            .map_err(|e| Status::internal(format!("{e}")))?;
        let buf = buf.freeze();
        let signature = self
            .signer
            .sign(buf.as_ref())
            .map_err(|e| Status::internal(format!("{e}")))?;
        // protobuf payload
        let r = CloseSessionRequest {
            payload: buf.as_ref().to_vec(),
            signature: signature.as_ref().to_vec(),
            session_token: token.to_string(),
            payload_type: PayloadType::QuerySessionPayload.into(),
        };
        Ok(r)
    }
    fn wrap_typed_close_session(
        &self,
        session: &QuerySessionInfo,
        token: &str,
    ) -> std::result::Result<CloseSessionRequest, Status> {
        let mut mbuf = BytesMut::with_capacity(1024 * 4);
        session
            .encode(&mut mbuf)
            .map_err(|e| Status::internal(format!("{e}")))?;
        let mbuf = Bytes(mbuf.freeze());
        let mut message: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        message.insert(
            "payload".to_string(),
            serde_json::Value::from(format!("{mbuf}")),
        );
        message.insert("payloadType".to_string(), serde_json::Value::from("0"));
        let typed_data = TypedData {
            domain: EIP712Domain {
                name: None,
                version: None,
                chain_id: None,
                verifying_contract: None,
                salt: None,
            },
            types: self.types.clone(),
            primary_type: "Message".to_string(),
            message,
        };
        let signature = self
            .signer
            .sign_typed_data(&typed_data)
            .map_err(|e| Status::internal(format!("{e}")))?;
        let buf = serde_json::to_vec(&typed_data).map_err(|e| Status::internal(format!("{e}")))?;
        let r = CloseSessionRequest {
            payload: buf,
            signature,
            session_token: token.to_string(),
            payload_type: PayloadType::TypedDataPayload.into(),
        };
        Ok(r)
    }

    async fn close_session_internal(
        &mut self,
        token: &str,
        use_typed_format: bool,
    ) -> std::result::Result<QuerySessionInfo, Status> {
        if token.len() == 0 {
            return Err(Status::internal(format!("Session {} not exist", token)));
        }
        match self.session_pool.get_session(token) {
            Some(sess) => {
                let query_session_info = sess.get_session_info();
                let meta = BroadcastMeta {
                    //TODO get from network
                    nonce: Utc::now().timestamp() as u64,
                    //TODO use config
                    chain_id: ChainId::DevNet.into(),
                    //TODO use config
                    chain_role: ChainRole::StorageShardChain.into(),
                };
                let session = QuerySessionInfo {
                    meta: Some(meta),
                    id: query_session_info.id,
                    start_time: query_session_info.start_time,
                    query_count: query_session_info.query_count,
                };
                let r = match use_typed_format {
                    true => {
                        let close_request = self.wrap_typed_close_session(&session, token)?;
                        close_request
                    }
                    false => {
                        let close_request = self.wrap_proto_close_session(&session, token)?;
                        close_request
                    }
                };
                let request = tonic::Request::new(r);
                let mut client = self.client.as_ref().clone();
                match client.close_query_session(request).await {
                    Ok(response) => match self.session_pool.remove_session(token) {
                        Ok(_) => {
                            let response = response.into_inner();
                            Ok(response.query_session_info.unwrap())
                        }
                        Err(e) => Err(Status::internal(format!("{}", e))),
                    },
                    Err(e) => Err(e),
                }
            }
            None => Err(Status::internal(format!("Session {} not exist", token))),
        }
    }

    /// close session
    /// 1. verify Account
    /// 2. request close_query_session
    /// 3. return node's CloseSessionResponse(query session info and signature) and client's CloseSessionResponse (query session info and signature)
    pub async fn close_session(&mut self) -> std::result::Result<(), Status> {
        if let Some(token) = self.session_pool.get_last_token() {
            self.close_session_internal(token.as_str(), self.use_typed_format)
                .await?;
        }
        Ok(())
    }

    pub async fn get_block_bills(&mut self, height: u64) -> std::result::Result<Vec<Bill>, Status> {
        if self
            .query_session_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            let token = self.keep_session(false).await?;
            match self.session_pool.get_session_mut(token.as_str()) {
                Some(session) => {
                    if session.check_session_running() {
                        let mut client = self.client.as_ref().clone();
                        let query_bill_key = Some(QueryBillKey {
                            height,
                            session_token: token.clone(),
                        });
                        let q_req = QueryBillRequest { query_bill_key };
                        let request = tonic::Request::new(q_req);
                        let response = client.query_bill(request).await?.into_inner();
                        session.increase_query(1);
                        Ok(response.bills)
                    } else {
                        Err(Status::permission_denied(
                            "Fail to query bill in this session. Please restart query session",
                        ))
                    }
                }
                None => Err(Status::not_found(format!(
                    "Fail to query, session with token {token} not found"
                ))),
            }
        } else {
            let mut client = self.client.as_ref().clone();
            let query_bill_key = Some(QueryBillKey {
                height,
                session_token: "".to_string(),
            });
            let q_req = QueryBillRequest { query_bill_key };
            let request = tonic::Request::new(q_req);
            let response = client.query_bill(request).await?.into_inner();
            Ok(response.bills)
        }
    }

    pub async fn get_state(&self) -> std::result::Result<NetworkStatus, Status> {
        let r = ShowNetworkStatusRequest {};
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();
        let status = client.show_network_status(request).await?.into_inner();
        Ok(status)
    }

    pub async fn get_account(&self, addr: &DB3Address) -> std::result::Result<Account, Status> {
        let r = GetAccountRequest {
            addr: addr.to_vec(),
        };
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();
        let response = client.get_account(request).await?.into_inner();
        Ok(response.account.unwrap())
    }

    pub async fn get_session_info(
        &self,
        session_token: &String,
    ) -> std::result::Result<(QuerySessionInfo, SessionStatus), Status> {
        let session_identifier = Some(SessionIdentifier {
            session_token: session_token.clone(),
        });
        let r = GetSessionInfoRequest { session_identifier };
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();

        let response = client.get_session_info(request).await?.into_inner();
        Ok((
            response.session_info.unwrap(),
            SessionStatus::from_i32(response.session_status).unwrap(),
        ))
    }

    pub async fn fetch_block_by_height(&self, height: u64) -> Result<BlockResponse, Status> {
        let request = tonic::Request::new(BlockRequest {
            block_height: height,
            block_hash: vec![],
            block_type: BlockType::BlockByHeight.into(),
        });
        let mut client = self.client.as_ref().clone();
        let response = client
            .get_block(request)
            .await
            .map_err(|e| Status::internal(format!("fail to get block from node service: {e}")))?
            .into_inner();
        Ok(response)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::mutation_sdk::MutationSDK;
    use crate::sdk_test;
    use bytes::BytesMut;

    use chrono::Utc;
    use db3_proto::db3_database_proto::structured_query::field_filter::Operator;
    use db3_proto::db3_database_proto::structured_query::filter::FilterType;
    use db3_proto::db3_database_proto::structured_query::value::ValueType;
    use db3_proto::db3_database_proto::structured_query::{FieldFilter, Filter, Projection, Value};
    use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
    use db3_proto::db3_node_proto::OpenSessionRequest;
    use db3_proto::db3_session_proto::OpenSessionPayload;
    use std::sync::Arc;
    use std::time;
    use tendermint::block;
    use tonic::transport::Endpoint;
    use uuid::Uuid;
    async fn run_get_bills_flow(
        use_typed_format: bool,
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        counter: i64,
    ) {
        let mclient = client.clone();
        {
            let (_, signer) = sdk_test::gen_secp256k1_signer(counter);
            let msdk = MutationSDK::new(mclient, signer, use_typed_format);
            let dm = sdk_test::create_a_database_mutation();
            let result = msdk.submit_database_mutation(&dm).await;
            assert!(result.is_ok(), "{:?}", result.err());
            let ten_millis = time::Duration::from_millis(2000);
            std::thread::sleep(ten_millis);
        }
        let (_, signer) = sdk_test::gen_secp256k1_signer(counter);
        let mut sdk = StoreSDK::new(client, signer, use_typed_format);
        let result = sdk.get_block_bills(1).await;
        if let Err(ref e) = result {
            println!("{}", e);
            assert!(false);
        }
        assert!(result.is_ok());
        let result = sdk.close_session().await;
        assert!(result.is_ok());
    }

    async fn run_fetch_block_flow(
        use_typed_format: bool,
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        counter: i64,
    ) {
        let (_, signer) = sdk_test::gen_secp256k1_signer(counter);
        let sdk = StoreSDK::new(client, signer, use_typed_format);
        let res = sdk.fetch_block_by_height(1).await;
        assert!(res.is_ok(), "{:?}", res);
        let block: block::Block = serde_json::from_slice(res.unwrap().block.as_slice()).unwrap();
        assert_eq!(block.header.height.value(), 1);
    }
    #[tokio::test]
    async fn get_bills_smoke_test() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        run_get_bills_flow(false, client.clone(), 3).await;
        run_get_bills_flow(true, client.clone(), 5).await;
    }

    async fn run_doc_crud_happy_path(
        use_typed_format: bool,
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        counter: i64,
    ) {
        let (addr1, signer) = sdk_test::gen_secp256k1_signer(counter);
        let msdk = MutationSDK::new(client.clone(), signer, use_typed_format);
        let dm = sdk_test::create_a_database_mutation();
        let result = msdk.submit_database_mutation(&dm).await;
        assert!(result.is_ok(), "{:?}", result.err());
        let two_seconds = time::Duration::from_millis(2000);
        std::thread::sleep(two_seconds);
        // add a collection
        let (db_id, _) = result.unwrap();
        println!("db id {}", db_id.to_hex());
        let cm = sdk_test::create_a_collection_mutataion("collection1", db_id.address());
        let result = msdk.submit_database_mutation(&cm).await;
        assert!(result.is_ok());
        std::thread::sleep(two_seconds);
        let (addr, signer) = sdk_test::gen_secp256k1_signer(counter);
        let mut sdk = StoreSDK::new(client.clone(), signer, use_typed_format);
        let my_dbs = sdk.get_my_database(addr1.to_hex().as_str()).await.unwrap();
        assert_eq!(true, my_dbs.len() > 0);
        let database = sdk.get_database(db_id.to_hex().as_str()).await;
        if let Ok(Some(db)) = database {
            assert_eq!(&db.address, db_id.address().as_ref());
            assert_eq!(&db.sender, addr.as_ref());
            assert_eq!(db.tx.len(), 2);
            assert_eq!(db.collections.len(), 1);
        } else {
            assert!(false);
        }
        // add 4 documents
        let docm = sdk_test::add_documents(
            "collection1",
            db_id.address(),
            &vec![
                r#"{"name": "John Doe","age": 43,"phones": ["+44 1234567","+44 2345678"]}"#,
                r#"{"name": "Mike","age": 44,"phones": ["+44 1234567","+44 2345678"]}"#,
                r#"{"name": "Bill","age": 44,"phones": ["+44 1234567","+44 2345678"]}"#,
                r#"{"name": "Bill","age": 45,"phones": ["+44 1234567","+44 2345678"]}"#,
            ],
        );
        let result = msdk.submit_database_mutation(&docm).await;
        assert!(result.is_ok());
        std::thread::sleep(two_seconds);

        // show all documents
        let documents = sdk
            .list_documents(db_id.to_hex().as_str(), "collection1", None)
            .await
            .unwrap();
        assert_eq!(documents.documents.len(), 4);

        // list documents with limit=3
        let documents = sdk
            .list_documents(db_id.to_hex().as_str(), "collection1", Some(3))
            .await
            .unwrap();
        assert_eq!(documents.documents.len(), 3);

        // run query equivalent to SQL: select * from collection1 where name = "Bill"
        let query = StructuredQuery {
            collection_name: "collection1".to_string(),
            select: Some(Projection { fields: vec![] }),
            r#where: Some(Filter {
                filter_type: Some(FilterType::FieldFilter(FieldFilter {
                    field: "name".to_string(),
                    op: Operator::Equal.into(),
                    value: Some(Value {
                        value_type: Some(ValueType::StringValue("Bill".to_string())),
                    }),
                })),
            }),
            limit: None,
        };
        println!("{}", serde_json::to_string(&query).unwrap());

        let documents = sdk.run_query(db_id.to_hex().as_str(), query).await.unwrap();
        assert_eq!(documents.documents.len(), 2);

        let result = sdk.close_session().await;
        println!("{:?}", result);
        assert!(result.is_ok());

        std::thread::sleep(two_seconds);
        let account_ret = sdk.get_account(&addr).await;
        assert!(account_ret.is_ok());
        let account = account_ret.unwrap();
        assert_eq!(account.total_mutation_count, 3);
        assert_eq!(account.total_session_count, 1);
    }
    #[tokio::test]
    async fn proto_doc_curd_happy_path_smoke_test() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        run_doc_crud_happy_path(false, client.clone(), 32).await;
    }

    #[tokio::test]
    async fn typed_data_doc_curd_happy_path_smoke_test() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        run_doc_crud_happy_path(true, client.clone(), 31).await;
    }

    #[tokio::test]
    async fn open_session_replay_attack() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let mut client = StorageNodeClient::new(channel);
        let (_, signer) = sdk_test::gen_ed25519_signer(40);
        let payload = OpenSessionPayload {
            header: Uuid::new_v4().to_string(),
            start_time: Utc::now().timestamp(),
        };
        let mut buf = BytesMut::with_capacity(1024 * 8);
        payload.encode(&mut buf).unwrap();
        let buf = buf.freeze();
        let signature = signer
            .sign(buf.as_ref())
            .map_err(|e| Status::internal(format!("{:?}", e)))
            .unwrap();
        let r = OpenSessionRequest {
            payload: buf.as_ref().to_vec(),
            signature: signature.as_ref().to_vec(),
            payload_type: PayloadType::QuerySessionPayload.into(),
        };
        let request = tonic::Request::new(r.clone());
        let response = client.open_query_session(request).await;
        assert!(response.is_ok());
        // duplicate header
        std::thread::sleep(time::Duration::from_millis(1000));
        let request = tonic::Request::new(r.clone());
        let response = client.open_query_session(request).await;
        assert!(response.is_err());
    }

    #[tokio::test]
    async fn open_session_ttl_expiered() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let mut client = StorageNodeClient::new(channel);
        let (_, signer) = sdk_test::gen_ed25519_signer(20);
        let payload = OpenSessionPayload {
            header: Uuid::new_v4().to_string(),
            start_time: Utc::now().timestamp() - 6,
        };
        let mut buf = BytesMut::with_capacity(1024 * 8);
        payload.encode(&mut buf).unwrap();
        let buf = buf.freeze();
        let signature = signer
            .sign(buf.as_ref())
            .map_err(|e| Status::internal(format!("{:?}", e)))
            .unwrap();
        let r = OpenSessionRequest {
            payload: buf.as_ref().to_vec(),
            signature: signature.as_ref().to_vec(),
            payload_type: PayloadType::QuerySessionPayload.into(),
        };
        let request = tonic::Request::new(r.clone());
        let response = client.open_query_session(request).await;
        assert!(response.is_err());
    }

    #[tokio::test]
    async fn network_status_test() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let (_addr, signer) = sdk_test::gen_ed25519_signer(50);
        let sdk = StoreSDK::new(client.clone(), signer, false);
        let result = sdk.get_state().await;
        assert!(result.is_ok());
    }

    /// write a test case for method get_my_database
    #[tokio::test]
    async fn test_get_my_database() {}

    #[tokio::test]
    async fn fetch_block_by_height() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));

        run_fetch_block_flow(false, client.clone(), 200).await;
    }
}
