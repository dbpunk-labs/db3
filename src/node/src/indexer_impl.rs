use crate::auth_storage::AuthStorage;
use crate::mutation_utils::MutationUtil;
use crate::node_storage::NodeStorage;
use chrono::Utc;
use db3_crypto::db3_address::DB3Address;
use db3_crypto::id::{DbId, DocumentId, TxId};
use db3_proto::db3_event_proto::event_message;
use db3_proto::db3_event_proto::EventMessage;
use db3_proto::db3_indexer_proto::indexer_node_server::IndexerNode;
use db3_proto::db3_indexer_proto::{
    GetDocumentRequest, GetDocumentResponse, IndexerStatus, RunQueryRequest, RunQueryResponse,
    ShowDatabaseRequest, ShowDatabaseResponse, ShowIndexerStatusRequest,
};
use db3_proto::db3_mutation_proto::{DatabaseAction, DatabaseMutation, PayloadType, WriteRequest};
use db3_sdk::store_sdk::StoreSDK;
use prost::Message;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tendermint::block;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

pub struct IndexerBlockSyncer {
    store_sdk: StoreSDK,
    node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>,
}

impl IndexerBlockSyncer {
    pub fn new(store_sdk: StoreSDK, node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>) -> Self {
        Self {
            store_sdk,
            node_store,
        }
    }

    /// start standalone indexer block syncer
    /// 1. subscribe db3 event
    /// 2. handle event to sync db3 node block
    pub async fn start(&mut self) -> std::result::Result<(), Status> {
        info!("[IndexerBlockSyncer] start indexer ...");
        info!("[IndexerBlockSyncer] subscribe event from db3 network");
        let mut stream = self
            .store_sdk
            .subscribe_event_message(true)
            .await?
            .into_inner();
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
    async fn handle_event(&mut self, event: EventMessage) -> std::result::Result<(), Status> {
        match event.event {
            Some(event_message::Event::BlockEvent(be)) => {
                info!(
                    "[IndexerBlockSyncer] Receive BlockEvent: Block\t{}\t0x{}\t0x{}\t{}",
                    be.height,
                    hex::encode(be.block_hash),
                    hex::encode(be.app_hash),
                    be.gas
                );
                let response = self
                    .store_sdk
                    .fetch_block_by_height(be.height)
                    .await
                    .map_err(|e| Status::internal(format!("fetch block error: {:?}", e)))?;

                debug!("Block Id: {:?}", response.block_id);
                let block: block::Block =
                    serde_json::from_slice(response.block.as_slice()).unwrap();
                debug!("Block transaction size: {}", block.data.len());
                match self.node_store.lock() {
                    Ok(mut store) => {
                        store.get_auth_store().begin_block(
                            block.header.height.value(),
                            Utc::now().timestamp() as u64,
                        );
                        let mut pending_databases: Vec<(DB3Address, DatabaseMutation, TxId)> =
                            vec![];
                        Self::parse_and_pending_mutations(&block.data, &mut pending_databases)?;
                        Self::apply_database_mutations(store.get_auth_store(), &pending_databases)?;
                        store.get_auth_store().commit().map_err(|e| {
                            Status::internal(format!("fail to commit database for {e}"))
                        })?;
                        // TODO: add show indexer state api
                        info!(
                            "[IndexerBlockSyncer] last block state: {:?}",
                            store.get_auth_store().get_last_block_state()
                        );
                        info!(
                            "[IndexerBlockSyncer] indexer state: {:?}",
                            store.get_auth_store().get_state()
                        );
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
    fn parse_and_pending_mutations(
        txs: &Vec<Vec<u8>>,
        database_mutations: &mut Vec<(DB3Address, DatabaseMutation, TxId)>,
    ) -> Result<(), Status> {
        for tx in txs {
            let tx_id = TxId::from(tx.as_ref());
            let wrequest = WriteRequest::decode(tx.as_ref());
            match wrequest {
                Ok(req) => match MutationUtil::unwrap_and_verify(req) {
                    Ok((data, data_type, account_id)) => match data_type {
                        PayloadType::DatabasePayload => {
                            match MutationUtil::parse_database_mutation(data.as_ref()) {
                                Ok(dm) => {
                                    let action = DatabaseAction::from_i32(dm.action);
                                    info!(
                                        "Add account: {}, mutation : {:?}, tx: {} into pending queue",
                                        account_id.addr.to_hex(), action, tx_id.to_base64());
                                    database_mutations.push((account_id.addr, dm, tx_id));
                                }
                                Err(e) => {
                                    let msg = format!("{e}");
                                    return Err(Status::internal(msg));
                                }
                            }
                        }
                        PayloadType::QuerySessionPayload => {
                            debug!("[IndexerBlockSyncer] Skip QuerySessionPayload");
                        }
                        PayloadType::MintCreditsPayload => {
                            debug!("[IndexerBlockSyncer] Skip MintCreditsPayload");
                        }
                        _ => {
                            debug!("[IndexerBlockSyncer] Skip other payload type");
                        }
                    },
                    Err(e) => {
                        warn!("[IndexerBlockSyncer] invalid write request: {:?}", e);
                        return Err(Status::internal(format!("invalid write request: {:?}", e)));
                    }
                },
                Err(e) => {
                    warn!("[IndexerBlockSyncer] invalid write request: {:?}", e);
                    return Err(Status::internal(format!("invalid write request: {:?}", e)));
                }
            }
        }
        Ok(())
    }
    fn apply_database_mutations(
        auth_store: &mut AuthStorage,
        pending_databases: &Vec<(DB3Address, DatabaseMutation, TxId)>,
    ) -> std::result::Result<(), Status> {
        info!(
            "Pending database mutations queue size: {}",
            pending_databases.len()
        );

        for (account_addr, mutation, tx_id) in pending_databases {
            let action = DatabaseAction::from_i32(mutation.action);
            info!(
                "apply database mutation transaction tx: {}, mutation: action: {:?}",
                &tx_id.to_base64(),
                action
            );
            let nonce: u64 = match &mutation.meta {
                Some(m) => m.nonce,
                //TODO will not go to here
                None => 1,
            };
            match auth_store.apply_database(&account_addr, nonce, &tx_id, &mutation) {
                Ok(_) => {
                    info!("apply transaction {} success", &tx_id.to_base64());
                }
                Err(e) => {
                    warn!("fail to apply database for {e}");
                    return Err(Status::internal(format!("fail to apply database for {e}")));
                }
            };
        }
        Ok(())
    }
}
pub struct IndexerNodeImpl {
    node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>,
}
impl IndexerNodeImpl {
    pub fn new(node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>) -> Self {
        Self { node_store }
    }
}
#[tonic::async_trait]
impl IndexerNode for IndexerNodeImpl {
    /// show indexer statuc
    async fn show_indexer_status(
        &self,
        _request: Request<ShowIndexerStatusRequest>,
    ) -> Result<Response<IndexerStatus>, Status> {
        match self.node_store.lock() {
            Ok(node_store) => {
                let state = node_store.get_state();
                let status = IndexerStatus {
                    total_database_count: state.total_database_count.load(Ordering::Relaxed),
                    total_collection_count: state.total_collection_count.load(Ordering::Relaxed),
                    total_document_count: state.total_document_count.load(Ordering::Relaxed),
                    total_account_count: state.total_account_count.load(Ordering::Relaxed),
                    total_mutation_count: state.total_mutation_count.load(Ordering::Relaxed),
                    total_storage_in_bytes: state.total_storage_bytes.load(Ordering::Relaxed),
                };
                Ok(Response::new(status))
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }

    /// show databases info
    async fn show_database(
        &self,
        request: Request<ShowDatabaseRequest>,
    ) -> std::result::Result<Response<ShowDatabaseResponse>, Status> {
        let show_database_req = request.into_inner();
        match self.node_store.lock() {
            Ok(mut node_store) => {
                if show_database_req.address.len() > 0 {
                    // get database id
                    let address_ref: &str = show_database_req.address.as_ref();
                    let db_id = DbId::try_from(address_ref)
                        .map_err(|e| Status::internal(format!("invalid database address {e}")))?;
                    if let Some(db) = node_store
                        .get_auth_store()
                        .get_database(&db_id)
                        .map_err(|e| Status::internal(format!("{:?}", e)))?
                    {
                        Ok(Response::new(ShowDatabaseResponse { dbs: vec![db] }))
                    } else {
                        Ok(Response::new(ShowDatabaseResponse { dbs: vec![] }))
                    }
                } else {
                    let address_ref: &str = show_database_req.owner_address.as_str();
                    let owner = DB3Address::try_from(address_ref)
                        .map_err(|e| Status::internal(format!("invalid database address {e}")))?;
                    let dbs = node_store
                        .get_auth_store()
                        .get_my_database(&owner)
                        .map_err(|e| Status::internal(format!("{:?}", e)))?;
                    Ok(Response::new(ShowDatabaseResponse { dbs }))
                }
            }
            Err(e) => Err(Status::internal(format!("Fail to get lock {}", e))),
        }
    }

    /// get document with given id
    async fn get_document(
        &self,
        request: Request<GetDocumentRequest>,
    ) -> std::result::Result<Response<GetDocumentResponse>, Status> {
        let get_document_request = request.into_inner();
        let id = DocumentId::try_from_base64(get_document_request.id.as_str())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;
        match self.node_store.lock() {
            Ok(mut node_store) => {
                // get database id
                match node_store.get_auth_store().get_document(&id) {
                    Ok(document) => Ok(Response::new(GetDocumentResponse { document })),
                    Err(e) => Err(Status::internal(format!("fail to get document {:?}", e))),
                }
            }
            Err(e) => Err(Status::internal(format!("Fail to get lock {}", e))),
        }
    }

    /// run document query with structure query
    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> std::result::Result<Response<RunQueryResponse>, Status> {
        let run_query_req = request.into_inner();
        match self.node_store.lock() {
            Ok(mut node_store) => {
                // get database id
                let address_ref: &str = run_query_req.address.as_ref();
                let db_id = DbId::try_from(address_ref)
                    .map_err(|e| Status::internal(format!("invalid database address {e}")))?;
                match &run_query_req.query {
                    Some(query) => {
                        let documents = node_store
                            .get_auth_store()
                            .run_query(&db_id, &query)
                            .map_err(|e| Status::internal(format!("{:?}", e)))?;
                        Ok(Response::new(RunQueryResponse { documents }))
                    }
                    None => return Err(Status::internal("Fail to run with none query")),
                }
            }
            Err(e) => Err(Status::internal(format!("Fail to get lock {}", e))),
        }
    }
}
#[cfg(test)]
mod tests {}
