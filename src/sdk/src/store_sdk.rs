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
use db3_proto::db3_bill_proto::Bill;
use db3_proto::db3_database_proto::{Database, Document};
use db3_proto::db3_node_proto::{
    storage_node_client::StorageNodeClient, BatchGetKey, BatchGetValue, CloseSessionRequest,
    GetAccountRequest, GetDocumentRequest, GetKeyRequest, GetRangeRequest, GetSessionInfoRequest,
    ListDocumentsRequest, ListDocumentsResponse, OpenSessionRequest, OpenSessionResponse,
    QueryBillKey, QueryBillRequest, Range as DB3Range, RangeKey, RangeValue, SessionIdentifier,
    ShowDatabaseRequest,
};
use db3_proto::db3_session_proto::{CloseSessionPayload, OpenSessionPayload, QuerySessionInfo};
use db3_session::session_manager::{SessionPool, SessionStatus};
use num_traits::cast::FromPrimitive;
use prost::Message;
use std::sync::Arc;
use tonic::Status;
use uuid::Uuid;

pub struct StoreSDK {
    client: Arc<StorageNodeClient<tonic::transport::Channel>>,
    signer: Db3MultiSchemeSigner,
    session_pool: SessionPool,
}

impl StoreSDK {
    pub fn new(
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        signer: Db3MultiSchemeSigner,
    ) -> Self {
        Self {
            client,
            signer,
            session_pool: SessionPool::new(),
        }
    }

    async fn keep_session(&mut self) -> std::result::Result<String, Status> {
        if let Some(token) = self.session_pool.get_last_token() {
            match self.session_pool.get_session_mut(token.as_ref()) {
                Some(session) => {
                    if session.get_session_query_count() > 2000 {
                        // close session
                        self.close_session(&token).await?;
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

    pub async fn list_documents(
        &mut self,
        addr: &str,
        collection_name: &str,
    ) -> std::result::Result<ListDocumentsResponse, Status> {
        let token = self.keep_session().await?;
        match self.session_pool.get_session_mut(token.as_ref()) {
            Some(session) => {
                if session.check_session_running() {
                    let r = ListDocumentsRequest {
                        session_token: token.to_string(),
                        address: addr.to_string(),
                        collection_name: collection_name.to_string(),
                    };
                    let request = tonic::Request::new(r);
                    let mut client = self.client.as_ref().clone();
                    let response = client.list_documents(request).await?.into_inner();
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
    }

    /// get the document with a base64 format id
    pub async fn get_document(
        &mut self,
        id: &str,
    ) -> std::result::Result<Option<Document>, Status> {
        let token = self.keep_session().await?;
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
    }
    ///
    /// get the information of database with a hex format address
    ///
    pub async fn get_database(
        &mut self,
        addr: &str,
    ) -> std::result::Result<Option<Database>, Status> {
        let token = self.keep_session().await?;
        match self.session_pool.get_session_mut(token.as_ref()) {
            Some(session) => {
                if session.check_session_running() {
                    let r = ShowDatabaseRequest {
                        session_token: token.to_string(),
                        address: addr.to_string(),
                    };
                    let request = tonic::Request::new(r);
                    let mut client = self.client.as_ref().clone();
                    let response = client.show_database(request).await?.into_inner();
                    session.increase_query(1);
                    Ok(response.db)
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
    }

    pub async fn open_session(&mut self) -> std::result::Result<OpenSessionResponse, Status> {
        let payload = OpenSessionPayload {
            header: Uuid::new_v4().to_string(),
            start_time: Utc::now().timestamp(),
        };
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

    /// close session
    /// 1. verify Account
    /// 2. request close_query_session
    /// 3. return node's CloseSessionResponse(query session info and signature) and client's CloseSessionResponse (query session info and signature)
    pub async fn close_session(
        &mut self,
        token: &String,
    ) -> std::result::Result<(QuerySessionInfo, QuerySessionInfo), Status> {
        match self.session_pool.get_session(token) {
            Some(sess) => {
                let query_session_info = sess.get_session_info();
                let payload = CloseSessionPayload {
                    session_info: Some(query_session_info.clone()),
                    session_token: token.clone(),
                };

                let mut buf = BytesMut::with_capacity(1024 * 8);
                payload
                    .encode(&mut buf)
                    .map_err(|e| Status::internal(format!("{e}")))?;

                let buf = buf.freeze();

                let signature = self
                    .signer
                    .sign(buf.as_ref())
                    .map_err(|e| Status::internal(format!("{e}")))?;

                let r = CloseSessionRequest {
                    payload: buf.as_ref().to_vec(),
                    signature: signature.as_ref().to_vec(),
                };

                let request = tonic::Request::new(r);
                let mut client = self.client.as_ref().clone();
                match client.close_query_session(request).await {
                    Ok(response) => match self.session_pool.remove_session(token) {
                        Ok(_) => {
                            let response = response.into_inner();
                            Ok((response.query_session_info.unwrap(), query_session_info))
                        }
                        Err(e) => Err(Status::internal(format!("{}", e))),
                    },
                    Err(e) => Err(e),
                }
            }
            None => Err(Status::internal(format!("Session {} not exist", token))),
        }
    }

    pub async fn get_bills_by_block(
        &mut self,
        height: u64,
        start: u64,
        end: u64,
        token: &String,
    ) -> std::result::Result<Vec<Bill>, Status> {
        match self.session_pool.get_session_mut(token) {
            Some(session) => {
                if session.check_session_running() {
                    let mut client = self.client.as_ref().clone();
                    let query_bill_key = Some(QueryBillKey {
                        height,
                        start_id: start,
                        end_id: end,
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

    pub async fn get_range(
        &mut self,
        ns: &[u8],
        range: &std::ops::Range<Vec<u8>>,
        token: &str,
    ) -> std::result::Result<Option<RangeValue>, Status> {
        match self.session_pool.get_session_mut(token) {
            Some(session) => {
                if session.check_session_running() {
                    let db3_range = DB3Range {
                        start: range.start.to_vec(),
                        end: range.end.to_vec(),
                    };
                    let range_keys = Some(RangeKey {
                        ns: ns.to_vec(),
                        range: Some(db3_range),
                        session_token: token.to_string(),
                    });
                    let r = GetRangeRequest { range_keys };
                    let request = tonic::Request::new(r);
                    let mut client = self.client.as_ref().clone();
                    let response = client.get_range(request).await?.into_inner();
                    // TODO(cj): batch keys query should be count as a query or multi queries?
                    session.increase_query(1);
                    Ok(response.range_value)
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
    }

    pub async fn batch_get(
        &mut self,
        ns: &[u8],
        keys: Vec<Vec<u8>>,
        token: &str,
    ) -> std::result::Result<Option<BatchGetValue>, Status> {
        match self.session_pool.get_session_mut(token) {
            Some(session) => {
                if session.check_session_running() {
                    let batch_get = Some(BatchGetKey {
                        ns: ns.to_vec(),
                        keys,
                        session_token: token.to_string(),
                    });
                    let r = GetKeyRequest { batch_get };
                    let request = tonic::Request::new(r);
                    let mut client = self.client.as_ref().clone();
                    let response = client.get_key(request).await?.into_inner();
                    // TODO(cj): batch keys query should be count as a query or multi queries?
                    session.increase_query(1);
                    Ok(response.batch_get_values)
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
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::mutation_sdk::MutationSDK;
    use crate::sdk_test;
    use bytes::BytesMut;
    use chrono::Utc;
    use db3_base::get_a_random_nonce;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::KvPair;
    use db3_proto::db3_mutation_proto::{Mutation, MutationAction};
    use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
    use db3_proto::db3_node_proto::OpenSessionRequest;
    use db3_proto::db3_session_proto::OpenSessionPayload;
    use std::sync::Arc;
    use std::time;
    use tonic::transport::Endpoint;
    use uuid::Uuid;

    #[tokio::test]
    async fn it_get_bills() {
        let nonce = get_a_random_nonce();
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let mclient = client.clone();
        {
            let (_, signer) = sdk_test::gen_ed25519_signer();
            let msdk = MutationSDK::new(mclient, signer);
            let kv = KvPair {
                key: format!("kkkkk_tt{}", 1).as_bytes().to_vec(),
                value: format!("vkalue_tt{}", 1).as_bytes().to_vec(),
                action: MutationAction::InsertKv.into(),
            };
            let mutation = Mutation {
                ns: "my_twitter".as_bytes().to_vec(),
                kv_pairs: vec![kv],
                nonce,
                chain_id: ChainId::MainNet.into(),
                chain_role: ChainRole::StorageShardChain.into(),
                gas_price: None,
                gas: 10,
            };
            let result = msdk.submit_mutation(&mutation).await;
            assert!(result.is_ok());
            let ten_millis = time::Duration::from_millis(11000);
            std::thread::sleep(ten_millis);
        }
        let (_, signer) = sdk_test::gen_ed25519_signer();
        let mut sdk = StoreSDK::new(client, signer);
        let res = sdk.open_session().await;
        assert!(res.is_ok());
        let session_info = res.unwrap();
        assert_eq!(session_info.session_token.len(), 36);
        let result = sdk
            .get_bills_by_block(1, 0, 10, &session_info.session_token)
            .await;
        if let Err(ref e) = result {
            println!("{}", e);
            assert!(false);
        }
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_range() {
        let nonce = get_a_random_nonce();
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let mclient = client.clone();
        let ns_vec = "my_data".as_bytes().to_vec();
        let (_, signer) = sdk_test::gen_ed25519_signer();
        let msdk = MutationSDK::new(mclient, signer);
        let k1 = KvPair {
            key: "k1".as_bytes().to_vec(),
            value: "v1".as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let k2 = KvPair {
            key: "k2".as_bytes().to_vec(),
            value: "v2".as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let k3 = KvPair {
            key: "k3".as_bytes().to_vec(),
            value: "v3".as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let mutation = Mutation {
            ns: ns_vec.clone(),
            kv_pairs: vec![k1, k2, k3],
            nonce,
            chain_id: ChainId::MainNet.into(),
            chain_role: ChainRole::StorageShardChain.into(),
            gas_price: None,
            gas: 10,
        };
        let result = msdk.submit_mutation(&mutation).await;
        assert!(result.is_ok(), "{}", result.err().unwrap());
        let two_sec = time::Duration::from_millis(2000);
        std::thread::sleep(two_sec);
        let (_, signer) = sdk_test::gen_ed25519_signer();
        let mut sdk = StoreSDK::new(client, signer);
        let res = sdk.open_session().await;
        assert!(res.is_ok());
        let session_info = res.unwrap();
        let range = std::ops::Range {
            start: "k0".as_bytes().to_vec(),
            end: "k4".as_bytes().to_vec(),
        };
        let range_result = sdk
            .get_range(ns_vec.as_ref(), &range, &session_info.session_token)
            .await;
        if let Ok(Some(range_value)) = range_result {
            assert_eq!(3, range_value.values.len());
            assert_eq!(range_value.values[2].value, "v3".as_bytes());
        } else {
            assert!(false);
        }
    }

    #[tokio::test]
    async fn close_session_happy_path() {
        let nonce = get_a_random_nonce();
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let mclient = client.clone();
        let key_vec = format!("kkkkk_tt{}", 10).as_bytes().to_vec();
        let value_vec = format!("vkalue_tt{}", 10).as_bytes().to_vec();
        let ns_vec = "my_twitter".as_bytes().to_vec();
        {
            let (_, signer) = sdk_test::gen_ed25519_signer();
            let msdk = MutationSDK::new(mclient, signer);
            let kv = KvPair {
                key: key_vec.clone(),
                value: value_vec.clone(),
                action: MutationAction::InsertKv.into(),
            };
            let mutation = Mutation {
                ns: ns_vec.clone(),
                kv_pairs: vec![kv],
                nonce,
                chain_id: ChainId::MainNet.into(),
                chain_role: ChainRole::StorageShardChain.into(),
                gas_price: None,
                gas: 10,
            };
            let result = msdk.submit_mutation(&mutation).await;
            assert!(result.is_ok(), "{}", result.err().unwrap());
            let two_sec = time::Duration::from_millis(2000);
            std::thread::sleep(two_sec);
        }
        let (addr, signer) = sdk_test::gen_ed25519_signer();
        let mut sdk = StoreSDK::new(client, signer);
        let res = sdk.open_session().await;
        assert!(res.is_ok());
        let session_info = res.unwrap();
        assert_eq!(session_info.session_token.len(), 36);
        let account_res = sdk.get_account(&addr).await;
        assert!(account_res.is_ok());
        let account1 = account_res.unwrap();
        for _ in 0..10 {
            if let Ok(Some(values)) = sdk
                .batch_get(&ns_vec, vec![key_vec.clone()], &session_info.session_token)
                .await
            {
                assert_eq!(values.values.len(), 1);
                assert_eq!(values.values[0].key.to_vec(), key_vec);
                assert_eq!(values.values[0].value.to_vec(), value_vec);
            } else {
                assert!(false);
            }
        }

        let res = sdk.close_session(&session_info.session_token).await;
        std::thread::sleep(time::Duration::from_millis(2000));

        let account_res = sdk.get_account(&addr).await;
        assert!(account_res.is_ok());
        let account2 = account_res.unwrap();
        assert!(res.is_ok());
        println!("account1: {:?}", account1);
        println!("account2: {:?}", account2);
        assert_eq!(
            account2.total_session_count - account1.total_session_count,
            10
        );
    }

    #[tokio::test]
    async fn close_session_wrong_path() {
        let nonce = get_a_random_nonce();
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let mclient = client.clone();
        let key_vec = format!("kkkkk_tt{}", 20).as_bytes().to_vec();
        let value_vec = format!("vkalue_tt{}", 20).as_bytes().to_vec();
        let ns_vec = "my_twitter".as_bytes().to_vec();
        {
            let (_, signer) = sdk_test::gen_ed25519_signer();
            let msdk = MutationSDK::new(mclient, signer);
            let kv = KvPair {
                key: key_vec.clone(),
                value: value_vec.clone(),
                action: MutationAction::InsertKv.into(),
            };
            let mutation = Mutation {
                ns: ns_vec.clone(),
                kv_pairs: vec![kv],
                nonce,
                chain_id: ChainId::MainNet.into(),
                chain_role: ChainRole::StorageShardChain.into(),
                gas_price: None,
                gas: 10,
            };
            let result = msdk.submit_mutation(&mutation).await;
            assert!(result.is_ok(), "{}", result.err().unwrap());
            let two_sec = time::Duration::from_millis(2000);
            std::thread::sleep(two_sec);
        }

        let (_, signer) = sdk_test::gen_ed25519_signer();
        let mut sdk = StoreSDK::new(client, signer);
        let res = sdk.open_session().await;
        assert!(res.is_ok());
        let session_info = res.unwrap();
        assert_eq!(session_info.session_token.len(), 36);
        if let Ok(Some(values)) = sdk
            .batch_get(&ns_vec, vec![key_vec.clone()], &session_info.session_token)
            .await
        {
            assert_eq!(values.values.len(), 1);
            assert_eq!(values.values[0].key.to_vec(), key_vec);
            assert_eq!(values.values[0].value.to_vec(), value_vec);
        } else {
            assert!(false);
        }

        sdk.session_pool
            .get_session_mut(&session_info.session_token)
            .unwrap()
            .increase_query(100);
        let res = sdk.close_session(&session_info.session_token).await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().message(),
            "query session verify fail. expect query count 1 but 101"
        );
    }

    #[tokio::test]
    async fn open_session_replay_attach() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let mut client = StorageNodeClient::new(channel);
        let (_, signer) = sdk_test::gen_ed25519_signer();
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
        let (_, signer) = sdk_test::gen_ed25519_signer();
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
        };
        let request = tonic::Request::new(r.clone());
        let response = client.open_query_session(request).await;
        assert!(response.is_err());
    }
}
