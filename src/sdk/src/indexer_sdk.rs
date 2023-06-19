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

//use db3_proto::db3_database_proto::structured_query::{Limit, Projection};
//use db3_proto::db3_database_proto::{Database, Document, StructuredQuery};
//use db3_proto::db3_indexer_proto::{
//    indexer_node_client::IndexerNodeClient, GetDocumentRequest, IndexerStatus, RunQueryRequest,
//    RunQueryResponse, ShowDatabaseRequest, ShowIndexerStatusRequest,
//};
//use std::sync::Arc;
//use tonic::Status;
//
// pub struct IndexerSDK {
//     client: Arc<IndexerNodeClient<tonic::transport::Channel>>,
// }
//
// impl IndexerSDK {
//     pub fn new(client: Arc<IndexerNodeClient<tonic::transport::Channel>>) -> Self {
//         Self { client }
//     }
//
//     /// show document with given db addr and collection name
//     pub async fn list_documents(
//         &mut self,
//         addr: &str,
//         collection_name: &str,
//         limit: Option<i32>,
//     ) -> std::result::Result<RunQueryResponse, Status> {
//         self.run_query(
//             addr,
//             StructuredQuery {
//                 collection_name: collection_name.to_string(),
//                 limit: match limit {
//                     Some(v) => Some(Limit { limit: v }),
//                     None => None,
//                 },
//                 select: Some(Projection { fields: vec![] }),
//                 r#where: None,
//             },
//         )
//         .await
//     }
//
//     /// get the document with a base64 format id
//     pub async fn get_document(
//         &mut self,
//         id: &str,
//     ) -> std::result::Result<Option<Document>, Status> {
//         let r = GetDocumentRequest { id: id.to_string() };
//
//         let request = tonic::Request::new(r);
//         let mut client = self.client.as_ref().clone();
//         let response = client.get_document(request).await?.into_inner();
//         Ok(response.document)
//     }
//
//     ///
//     /// get the information of database with a hex format address
//     ///
//     pub async fn get_database(
//         &mut self,
//         addr: &str,
//     ) -> std::result::Result<Option<Database>, Status> {
//         let r = ShowDatabaseRequest {
//             address: addr.to_string(),
//             owner_address: "".to_string(),
//         };
//         let request = tonic::Request::new(r);
//         let mut client = self.client.as_ref().clone();
//         let response = client.show_database(request).await?.into_inner();
//         if response.dbs.len() > 0 {
//             Ok(Some(response.dbs[0].clone()))
//         } else {
//             Ok(None)
//         }
//     }
//
//     ///
//     /// get the information of database with a hex format address
//     ///
//     pub async fn get_my_database(
//         &mut self,
//         addr: &str,
//     ) -> std::result::Result<Vec<Database>, Status> {
//         let r = ShowDatabaseRequest {
//             address: "".to_string(),
//             owner_address: addr.to_string(),
//         };
//         let request = tonic::Request::new(r);
//         let mut client = self.client.as_ref().clone();
//         let response = client.show_database(request).await?.into_inner();
//         Ok(response.dbs)
//     }
//
//     /// query the document with structure query
//     pub async fn run_query(
//         &mut self,
//         addr: &str,
//         query: StructuredQuery,
//     ) -> std::result::Result<RunQueryResponse, Status> {
//         let r = RunQueryRequest {
//             address: addr.to_string(),
//             query: Some(query),
//         };
//         let request = tonic::Request::new(r);
//         let mut client = self.client.as_ref().clone();
//         let response = client.run_query(request).await?.into_inner();
//         Ok(response)
//     }
//
//     pub async fn get_state(&self) -> std::result::Result<IndexerStatus, Status> {
//         let r = ShowIndexerStatusRequest {};
//         let request = tonic::Request::new(r);
//         let mut client = self.client.as_ref().clone();
//         let status = client.show_indexer_status(request).await?.into_inner();
//         Ok(status)
//     }
// }
//
// #[cfg(test)]
// mod tests {
//
//     use super::*;
//     use crate::mutation_sdk::MutationSDK;
//     use crate::sdk_test;
//
//     use db3_proto::db3_database_proto::structured_query::field_filter::Operator;
//     use db3_proto::db3_database_proto::structured_query::filter::FilterType;
//     use db3_proto::db3_database_proto::structured_query::value::ValueType;
//     use db3_proto::db3_database_proto::structured_query::{FieldFilter, Filter, Projection, Value};
//     use db3_proto::db3_indexer_proto::indexer_node_client::IndexerNodeClient;
//     use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
//     use std::sync::Arc;
//     use std::time;
//     use tonic::transport::Endpoint;
//
//     async fn run_doc_crud_happy_path(
//         storage_client: Arc<StorageNodeClient<tonic::transport::Channel>>,
//         indexer_client: Arc<IndexerNodeClient<tonic::transport::Channel>>,
//         counter: i64,
//     ) {
//         let (addr1, signer) = sdk_test::gen_secp256k1_signer(counter);
//         let msdk = MutationSDK::new(storage_client.clone(), signer, true);
//         let dm = sdk_test::create_a_database_mutation();
//         let result = msdk.submit_database_mutation(&dm).await;
//         assert!(result.is_ok(), "{:?}", result.err());
//         let sleep_seconds = time::Duration::from_millis(3000);
//         std::thread::sleep(sleep_seconds);
//         // add a collection
//         let (db_id, _) = result.unwrap();
//         println!("db id {}", db_id.to_hex());
//         let cm = sdk_test::create_a_collection_mutataion("collection1", db_id.address());
//         let result = msdk.submit_database_mutation(&cm).await;
//         assert!(result.is_ok());
//         std::thread::sleep(sleep_seconds);
//         let (addr, _signer) = sdk_test::gen_secp256k1_signer(counter);
//         let mut sdk = IndexerSDK::new(indexer_client.clone());
//         let my_dbs = sdk.get_my_database(addr1.to_hex().as_str()).await.unwrap();
//         assert_eq!(true, my_dbs.len() > 0);
//         let database = sdk.get_database(db_id.to_hex().as_str()).await;
//         if let Ok(Some(db)) = database {
//             assert_eq!(&db.address, db_id.address().as_ref());
//             assert_eq!(&db.sender, addr.as_ref());
//             assert_eq!(db.tx.len(), 2);
//             assert_eq!(db.collections.len(), 1);
//         } else {
//             assert!(false);
//         }
//         // add 4 documents
//         let docm = sdk_test::add_documents(
//             "collection1",
//             db_id.address(),
//             &vec![
//                 r#"{"name": "John Doe","age": 43,"phones": ["+44 1234567","+44 2345678"]}"#,
//                 r#"{"name": "Mike","age": 44,"phones": ["+44 1234567","+44 2345678"]}"#,
//                 r#"{"name": "Bill","age": 44,"phones": ["+44 1234567","+44 2345678"]}"#,
//                 r#"{"name": "Bill","age": 45,"phones": ["+44 1234567","+44 2345678"]}"#,
//             ],
//         );
//         let result = msdk.submit_database_mutation(&docm).await;
//         assert!(result.is_ok());
//         std::thread::sleep(sleep_seconds);
//
//         // show all documents
//         let documents = sdk
//             .list_documents(db_id.to_hex().as_str(), "collection1", None)
//             .await
//             .unwrap();
//         assert_eq!(documents.documents.len(), 4);
//
//         // list documents with limit=3
//         let documents = sdk
//             .list_documents(db_id.to_hex().as_str(), "collection1", Some(3))
//             .await
//             .unwrap();
//         assert_eq!(documents.documents.len(), 3);
//
//         // run query equivalent to SQL: select * from collection1 where name = "Bill"
//         let query = StructuredQuery {
//             collection_name: "collection1".to_string(),
//             select: Some(Projection { fields: vec![] }),
//             r#where: Some(Filter {
//                 filter_type: Some(FilterType::FieldFilter(FieldFilter {
//                     field: "name".to_string(),
//                     op: Operator::Equal.into(),
//                     value: Some(Value {
//                         value_type: Some(ValueType::StringValue("Bill".to_string())),
//                     }),
//                 })),
//             }),
//             limit: None,
//         };
//         println!("{}", serde_json::to_string(&query).unwrap());
//
//         let documents = sdk.run_query(db_id.to_hex().as_str(), query).await.unwrap();
//         assert_eq!(documents.documents.len(), 2);
//         std::thread::sleep(sleep_seconds);
//     }
//
//     fn create_storage_node_client() -> Arc<StorageNodeClient<tonic::transport::Channel>> {
//         let ep = "http://127.0.0.1:26659";
//         let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
//         let channel = rpc_endpoint.connect_lazy();
//         Arc::new(StorageNodeClient::new(channel))
//     }
//
//     fn create_indexer_node_client() -> Arc<IndexerNodeClient<tonic::transport::Channel>> {
//         let ep = "http://127.0.0.1:26639";
//         let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
//         let channel = rpc_endpoint.connect_lazy();
//         Arc::new(IndexerNodeClient::new(channel))
//     }
//
//     #[tokio::test]
//     async fn typed_data_doc_curd_happy_path_smoke_test() {
//         run_doc_crud_happy_path(
//             create_storage_node_client(),
//             create_indexer_node_client(),
//             131,
//         )
//         .await;
//     }
//
//     #[tokio::test]
//     async fn indexer_status_test() {
//         let ep = "http://127.0.0.1:26639";
//         let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
//         let channel = rpc_endpoint.connect_lazy();
//         let client = Arc::new(IndexerNodeClient::new(channel));
//         let (_addr, _signer) = sdk_test::gen_ed25519_signer(150);
//         let sdk = IndexerSDK::new(client.clone());
//         let result = sdk.get_state().await;
//         assert!(result.is_ok());
//     }
// }
