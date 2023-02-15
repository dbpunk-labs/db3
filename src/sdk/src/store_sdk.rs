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
    storage_node_client::StorageNodeClient, CloseSessionRequest, GetAccountRequest,
    GetDocumentRequest, GetSessionInfoRequest, ListDocumentsRequest, ListDocumentsResponse,
    OpenSessionRequest, OpenSessionResponse, QueryBillKey, QueryBillRequest, SessionIdentifier,
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
                        self.close_session_internal(&token).await?;
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

    async fn close_session_internal(
        &mut self,
        token: &str,
    ) -> std::result::Result<QuerySessionInfo, Status> {
        match self.session_pool.get_session(token) {
            Some(sess) => {
                let query_session_info = sess.get_session_info();
                let payload = CloseSessionPayload {
                    session_info: Some(query_session_info.clone()),
                    session_token: token.to_string(),
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
            self.close_session_internal(token.as_str()).await?;
        }
        Ok(())
    }

    pub async fn get_block_bills(&mut self, height: u64) -> std::result::Result<Vec<Bill>, Status> {
        let token = self.keep_session().await?;
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
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::mutation_sdk::MutationSDK;
    use crate::sdk_test;
    use bytes::BytesMut;

    use chrono::Utc;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
    use db3_proto::db3_node_proto::OpenSessionRequest;
    use db3_proto::db3_session_proto::OpenSessionPayload;
    use rand::random;
    use std::sync::Arc;
    use std::time;
    use tonic::transport::Endpoint;
    use uuid::Uuid;

    #[tokio::test]
    async fn it_get_bills() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let mclient = client.clone();
        let seed_u8: u8 = random();
        {
            let (_, signer) = sdk_test::gen_ed25519_signer(seed_u8);
            let msdk = MutationSDK::new(mclient, signer);
            let dm = sdk_test::create_a_database_mutation();
            let result = msdk.submit_database_mutation(&dm).await;
            assert!(result.is_ok(), "{:?}", result.err());
            let ten_millis = time::Duration::from_millis(2000);
            std::thread::sleep(ten_millis);
        }
        let (_, signer) = sdk_test::gen_ed25519_signer(seed_u8);
        let mut sdk = StoreSDK::new(client, signer);
        let result = sdk.get_block_bills(1).await;
        if let Err(ref e) = result {
            println!("{}", e);
            assert!(false);
        }
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn doc_curd_happy_path_smoke_test() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let seed_u8: u8 = random();

        let (_, signer) = sdk_test::gen_ed25519_signer(seed_u8);
        let msdk = MutationSDK::new(client.clone(), signer);
        // create a database
        let dm = sdk_test::create_a_database_mutation();
        let result = msdk.submit_database_mutation(&dm).await;
        assert!(result.is_ok(), "{:?}", result.err());
        let ten_millis = time::Duration::from_millis(2000);
        std::thread::sleep(ten_millis);
        // add a collection
        let (db_id, tx_id) = result.unwrap();
        println!("db id {}", db_id.to_hex());
        let cm = sdk_test::create_a_collection_mutataion("collection1", db_id.address());
        let result = msdk.submit_database_mutation(&cm).await;
        assert!(result.is_ok());
        std::thread::sleep(ten_millis);
        let (addr, signer) = sdk_test::gen_ed25519_signer(seed_u8);
        let mut sdk = StoreSDK::new(client.clone(), signer);
        let database = sdk.get_database(db_id.to_hex().as_str()).await;
        if let Ok(Some(db)) = database {
            assert_eq!(&db.address, db_id.address().as_ref());
            assert_eq!(&db.sender, addr.as_ref());
            assert_eq!(db.tx.len(), 2);
            assert_eq!(db.collections.len(), 1);
        } else {
            assert!(false);
        }
        let docm = sdk_test::add_a_document("collection1", db_id.address());
        let result = msdk.submit_database_mutation(&docm).await;
        assert!(result.is_ok());
        std::thread::sleep(ten_millis);
        let documents = sdk
            .list_documents(db_id.to_hex().as_str(), "collection1")
            .await
            .unwrap();
        assert_eq!(documents.documents.len(), 1);
        let result = sdk.close_session().await;
        assert!(result.is_ok());

        std::thread::sleep(ten_millis);
        let account_ret = sdk.get_account(&addr).await;
        assert!(account_ret.is_ok());
        let account = account_ret.unwrap();
        assert_eq!(account.total_mutation_count, 3);
        assert_eq!(account.total_session_count, 1);
    }

    #[tokio::test]
    async fn open_session_replay_attack() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let mut client = StorageNodeClient::new(channel);
        let seed_u8: u8 = random();
        let (_, signer) = sdk_test::gen_ed25519_signer(seed_u8);
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
        let seed_u8: u8 = random();
        let (_, signer) = sdk_test::gen_ed25519_signer(seed_u8);
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
