use crate::auth_storage::AuthStorage;
use crate::mutation_utils::MutationUtil;
use crate::node_storage::NodeStorage;
use chrono::Utc;
use db3_base::bson_util::{bson_document_into_bytes, bytes_to_bson_document};
use db3_crypto::db3_address::DB3Address;
use db3_crypto::id::{DbId, DocumentId, TxId};
use db3_proto::db3_indexer_proto::indexer_node_server::IndexerNode;
use db3_proto::db3_indexer_proto::{
    GetDocumentRequest, GetDocumentResponse, IndexerStatus, RunQueryRequest, RunQueryResponse,
    ShowDatabaseRequest, ShowDatabaseResponse, ShowIndexerStatusRequest,
};
use db3_proto::db3_mutation_proto::{DatabaseAction, DatabaseMutation, PayloadType, WriteRequest};
use db3_proto::db3_mutation_v2_proto::mutation::body_wrapper::Body;
use db3_proto::db3_mutation_v2_proto::MutationAction;
use db3_proto::db3_storage_proto::block_response::MutationWrapper;
use db3_proto::db3_storage_proto::event_message;
use db3_proto::db3_storage_proto::{
    BlockRequest as BlockRequestV2, BlockResponse as BlockResponseV2,
    EventMessage as EventMessageV2, EventType as EventTypeV2, Subscription as SubscriptionV2,
};
use db3_sdk::store_sdk_v2::StoreSDKV2;
use db3_storage::doc_store::DocStore;
use prost::Message;
use std::fs;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

pub struct IndexerBlockSyncer {
    store_sdk: StoreSDKV2,
    doc_store: Arc<Mutex<Pin<Box<DocStore>>>>,
}

impl IndexerBlockSyncer {
    pub fn new(store_sdk: StoreSDKV2, doc_store: Arc<Mutex<Pin<Box<DocStore>>>>) -> Self {
        Self {
            store_sdk,
            doc_store,
        }
    }

    /// start standalone indexer block syncer
    /// 1. subscribe db3 event
    /// 2. handle event to sync db3 node block
    pub async fn start(&mut self) -> std::result::Result<(), Status> {
        info!("[IndexerBlockSyncer] start indexer ...");
        info!("[IndexerBlockSyncer] subscribe event from db3 network");
        let mut stream = self.store_sdk.subscribe_event_message().await?.into_inner();
        info!("listen and handle event message");
        while let Some(event) = stream.message().await.unwrap() {
            match self.handle_event(event).await {
                Err(e) => {
                    info!("[IndexerBlockSyncer] handle event error: {:?}", e);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// handle event message
    async fn handle_event(&mut self, event: EventMessageV2) -> std::result::Result<(), Status> {
        match event.event {
            Some(event_message::Event::BlockEvent(be)) => {
                info!(
                    "[IndexerBlockSyncer] Receive BlockEvent: Block\t{}MutationCount\t{}",
                    be.block_id, be.mutation_count,
                );
                let response = self
                    .store_sdk
                    .get_block_by_height(be.block_id)
                    .await
                    .map_err(|e| Status::internal(format!("fetch block error: {:?}", e)))?
                    .into_inner();

                let mutations = response.mutations;
                debug!("Block mutations size: {:?}", mutations.len());
                match self.doc_store.lock() {
                    Ok(mut store) => {
                        Self::parse_and_apply_mutations(store.as_mut(), &mutations)?;
                        // TODO: add show indexer state api
                    }
                    Err(e) => {
                        warn!("[IndexerBlockSyncer] get node store error: {:?}", e);
                        return Err(Status::internal(format!("get node store error: {:?}", e)));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn parse_and_apply_mutations(
        doc_store: Pin<&mut DocStore>,
        mutations: &Vec<MutationWrapper>,
    ) -> Result<(), Status> {
        for mutation in mutations.iter() {
            let body = mutation.body.as_ref().unwrap();
            // validate the signature
            let (dm, address, nonce) =
                MutationUtil::unwrap_and_light_verify(&body.payload, body.signature.as_str())
                    .map_err(|e| Status::internal(format!("{e}")))?;
            let action = MutationAction::from_i32(dm.action)
                .ok_or(Status::internal("fail to convert action type".to_string()))?;
            // TODO validate the database mutation
            match action {
                MutationAction::CreateDocumentDb => {
                    for body in dm.bodies {
                        if let Some(Body::DocDatabaseMutation(ref doc_db_mutation)) = &body.body {
                            let db_id = doc_store
                                .create_database(
                                    &address, nonce,
                                    // TODO: pass network id from config or mutation header
                                    0,
                                )
                                .map_err(|e| Status::internal(format!("{e}")))?;
                            let db_id_hex = db_id.to_hex();
                            info!(
                                "add database with addr {} from owner {}",
                                db_id_hex.as_str(),
                                address.to_hex().as_str()
                            );
                            break;
                        }
                    }
                }
                MutationAction::AddCollection => {
                    for (i, body) in dm.bodies.iter().enumerate() {
                        let db_address_ref: &[u8] = body.db_address.as_ref();
                        let db_addr = DB3Address::try_from(db_address_ref)
                            .map_err(|e| Status::internal(format!("{e}")))?;
                        if let Some(Body::CollectionMutation(ref col_mutation)) = &body.body {
                            doc_store
                                .create_collection(&address, col_mutation)
                                .map_err(|e| Status::internal(format!("{e}")))?;
                            info!(
                                    "add collection with db_addr {}, collection_name: {}, from owner {}",
                                    db_addr.to_hex().as_str(),
                                    col_mutation.collection_name.as_str(),
                                    address.to_hex().as_str()
                                );
                        }
                    }
                }
                MutationAction::AddDocument => {
                    for (i, body) in dm.bodies.iter().enumerate() {
                        let db_address_ref: &[u8] = body.db_address.as_ref();
                        let db_addr = DB3Address::try_from(db_address_ref)
                            .map_err(|e| Status::internal(format!("{e}")))?;
                        if let Some(Body::DocumentMutation(ref doc_mutation)) = &body.body {
                            let mut docs = Vec::<String>::new();
                            for buf in doc_mutation.documents.iter() {
                                let document =
                                    bytes_to_bson_document(buf.clone()).map_err(|e| {
                                        Status::internal(format!(
                                            "fail to convert bytes to bson: {:?}",
                                            e
                                        ))
                                    })?;
                                let doc_str = document.to_string();
                                debug!("add document: {}", doc_str);
                                docs.push(doc_str.to_string());
                            }
                            let ids = doc_store
                                .add_str_docs(
                                    &address,
                                    doc_mutation.collection_name.as_str(),
                                    &docs,
                                )
                                .map_err(|e| Status::internal(format!("{e}")))?;
                            info!(
                                    "add documents with db_addr {}, collection_name: {}, from owner {}, ids: {:?}",
                                    db_addr.to_hex().as_str(),
                                    doc_mutation.collection_name.as_str(),
                                    address.to_hex().as_str(), ids,
                                );
                        }
                    }
                }
                _ => {
                    warn!("unsupported mutation action: {:?}", action);
                }
            }
        }
        Ok(())
    }
}
pub struct IndexerNodeImpl {
    doc_store: Arc<Mutex<Pin<Box<DocStore>>>>,
}
impl IndexerNodeImpl {
    pub fn new(db_path: &str, doc_store: Arc<Mutex<Pin<Box<DocStore>>>>) -> Self {
        info!("open indexer store with path {}", db_path);
        let path = Path::new(db_path);
        fs::create_dir(path).unwrap();
        Self { doc_store }
    }
}
#[tonic::async_trait]
impl IndexerNode for IndexerNodeImpl {
    /// show indexer statuc
    async fn show_indexer_status(
        &self,
        _request: Request<ShowIndexerStatusRequest>,
    ) -> Result<Response<IndexerStatus>, Status> {
        match self.doc_store.lock() {
            Ok(_doc_store) => {
                let status = IndexerStatus {
                    total_database_count: 0,
                    total_collection_count: 0,
                    total_document_count: 0,
                    total_account_count: 0,
                    total_mutation_count: 0,
                    total_storage_in_bytes: 0,
                };
                Ok(Response::new(status))
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }

    // /// show databases info
    // async fn show_database(
    //     &self,
    //     request: Request<ShowDatabaseRequest>,
    // ) -> std::result::Result<Response<ShowDatabaseResponse>, Status> {
    //     let show_database_req = request.into_inner();
    //     match self.doc_store.lock() {
    //         Ok(mut doc_store) => {
    //             if show_database_req.address.len() > 0 {
    //                 // get database id
    //                 let address_ref: &str = show_database_req.address.as_ref();
    //                 let db_id = DbId::try_from(address_ref)
    //                     .map_err(|e| Status::internal(format!("invalid database address {e}")))?;
    //                 if let Some(db) = doc_store.get
    //                     .get_auth_store()
    //                     .get_database(&db_id)
    //                     .map_err(|e| Status::internal(format!("{:?}", e)))?
    //                 {
    //                     Ok(Response::new(ShowDatabaseResponse { dbs: vec![db] }))
    //                 } else {
    //                     Ok(Response::new(ShowDatabaseResponse { dbs: vec![] }))
    //                 }
    //             } else {
    //                 let address_ref: &str = show_database_req.owner_address.as_str();
    //                 let owner = DB3Address::try_from(address_ref)
    //                     .map_err(|e| Status::internal(format!("invalid database address {e}")))?;
    //                 let dbs = node_store
    //                     .get_auth_store()
    //                     .get_my_database(&owner)
    //                     .map_err(|e| Status::internal(format!("{:?}", e)))?;
    //                 Ok(Response::new(ShowDatabaseResponse { dbs }))
    //             }
    //         }
    //         Err(e) => Err(Status::internal(format!("Fail to get lock {}", e))),
    //     }
    // }

    // /// get document with given id
    // async fn get_document(
    //     &self,
    //     request: Request<GetDocumentRequest>,
    // ) -> std::result::Result<Response<GetDocumentResponse>, Status> {
    //     let get_document_request = request.into_inner();
    //     let id = DocumentId::try_from_base64(get_document_request.id.as_str())
    //         .map_err(|e| Status::internal(format!("{:?}", e)))?;
    //     match self.doc_store.lock() {
    //         Ok(mut node_store) => {
    //             // get database id
    //             match node_store.get_auth_store().get_document(&id) {
    //                 Ok(document) => Ok(Response::new(GetDocumentResponse { document })),
    //                 Err(e) => Err(Status::internal(format!("fail to get document {:?}", e))),
    //             }
    //         }
    //         Err(e) => Err(Status::internal(format!("Fail to get lock {}", e))),
    //     }
    // }

    // /// run document query with structure query
    // async fn run_query(
    //     &self,
    //     request: Request<RunQueryRequest>,
    // ) -> std::result::Result<Response<RunQueryResponse>, Status> {
    //     let run_query_req = request.into_inner();
    //     match self.doc_store.lock() {
    //         Ok(mut node_store) => {
    //             // get database id
    //             let address_ref: &str = run_query_req.address.as_ref();
    //             let db_id = DbId::try_from(address_ref)
    //                 .map_err(|e| Status::internal(format!("invalid database address {e}")))?;
    //             match &run_query_req.query {
    //                 Some(query) => {
    //                     let documents = node_store
    //                         .get_auth_store()
    //                         .run_query(&db_id, &query)
    //                         .map_err(|e| Status::internal(format!("{:?}", e)))?;
    //                     Ok(Response::new(RunQueryResponse { documents }))
    //                 }
    //                 None => return Err(Status::internal("Fail to run with none query")),
    //             }
    //         }
    //         Err(e) => Err(Status::internal(format!("Fail to get lock {}", e))),
    //     }
    // }
}
#[cfg(test)]
mod tests {}
