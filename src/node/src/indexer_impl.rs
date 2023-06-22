//
// indexer_impl.rs
// Copyright (C) 2023 db3.network Author imotai <codego.me@gmail.com>
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

use crate::mutation_utils::MutationUtil;
use db3_base::bson_util::bytes_to_bson_document;
use db3_crypto::db3_address::DB3Address;
use db3_error::{DB3Error, Result};
use db3_event::event_processor::EventProcessor;
use db3_event::event_processor::EventProcessorConfig;
use db3_proto::db3_indexer_proto::indexer_node_server::IndexerNode;
use db3_proto::db3_indexer_proto::{
    ContractSyncStatus, GetContractSyncStatusRequest, GetContractSyncStatusResponse,
    GetSystemStatusRequest, RunQueryRequest, RunQueryResponse, SetupRequest, SetupResponse,
    SystemStatus,
};
use db3_proto::db3_mutation_v2_proto::mutation::body_wrapper::Body;
use db3_proto::db3_mutation_v2_proto::EventDatabaseMutation;
use db3_proto::db3_mutation_v2_proto::MutationAction;
use db3_proto::db3_storage_proto::block_response::MutationWrapper;
use db3_proto::db3_storage_proto::event_message;
use db3_proto::db3_storage_proto::EventMessage as EventMessageV2;
use db3_sdk::store_sdk_v2::StoreSDKV2;
use db3_storage::db_store_v2::{DBStoreV2, DBStoreV2Config};
use db3_storage::key_store::{KeyStore, KeyStoreConfig};
use db3_storage::meta_store_client::MetaStoreClient;
use ethers::prelude::{LocalWallet, Signer};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::task;
use tokio::time::{sleep, Duration};
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct IndexerNodeImpl {
    db_store: DBStoreV2,
    network_id: Arc<AtomicU64>,
    node_url: String,
    key_root_path: String,
    contract_addr: String,
    evm_node_url: String,
    processor_mapping: Arc<Mutex<HashMap<String, Arc<EventProcessor>>>>,
}

impl IndexerNodeImpl {
    pub fn new(
        config: DBStoreV2Config,
        network_id: u64,
        node_url: String,
        key_root_path: String,
        contract_addr: String,
        evm_node_url: String,
    ) -> Result<Self> {
        let db_store = DBStoreV2::new(config)?;
        Ok(Self {
            db_store,
            network_id: Arc::new(AtomicU64::new(network_id)),
            node_url,
            key_root_path,
            contract_addr,
            evm_node_url,
            //TODO recover from the database
            processor_mapping: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// start standalone indexer block syncer
    /// 1. subscribe db3 event
    /// 2. handle event to sync db3 node block
    pub async fn start(&self, store_sdk: StoreSDKV2) -> Result<()> {
        info!("start subscribe...");
        loop {
            match store_sdk.subscribe_event_message().await {
                Ok(handle) => {
                    info!("listen and handle event message");
                    let mut stream = handle.into_inner();
                    while let Some(event) = stream.message().await.unwrap() {
                        match self.handle_event(event, &store_sdk).await {
                            Err(e) => {
                                warn!("[IndexerBlockSyncer] handle event error: {:?}", e);
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    warn!("fail to subscribe block event for {e} and retry in 5 seconds");
                    sleep(Duration::from_millis(1000 * 5)).await;
                }
            }
        }
    }

    /// handle event message
    async fn handle_event(&self, event: EventMessageV2, store_sdk: &StoreSDKV2) -> Result<()> {
        match event.event {
            Some(event_message::Event::BlockEvent(be)) => {
                debug!(
                    "Receive BlockEvent: Block\t{}\tMutationCount\t{}",
                    be.block_id, be.mutation_count,
                );
                let response = store_sdk
                    .get_block_by_height(be.block_id)
                    .await
                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?
                    .into_inner();

                let mutations = response.mutations;
                debug!("Block mutations size: {:?}", mutations.len());
                self.parse_and_apply_mutations(&mutations).await?;
            }
            _ => {}
        }
        Ok(())
    }
    fn build_wallet(key_root_path: &str) -> Result<LocalWallet> {
        let config = KeyStoreConfig {
            key_root_path: key_root_path.to_string(),
        };
        let key_store = KeyStore::new(config);
        match key_store.has_key("evm") {
            true => {
                let data = key_store.get_key("evm")?;
                let data_ref: &[u8] = &data;
                let wallet = LocalWallet::from_bytes(data_ref)
                    .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
                Ok(wallet)
            }

            false => {
                let mut rng = rand::thread_rng();
                let wallet = LocalWallet::new(&mut rng);
                let data = wallet.signer().to_bytes();
                key_store.write_key("evm", data.deref())?;
                Ok(wallet)
            }
        }
    }

    async fn start_an_event_task(
        &self,
        db: &DB3Address,
        mutation: &EventDatabaseMutation,
    ) -> Result<()> {
        let config = EventProcessorConfig {
            evm_node_url: mutation.evm_node_url.to_string(),
            db_addr: db.to_hex(),
            abi: mutation.events_json_abi.to_string(),
            target_events: mutation
                .tables
                .iter()
                .map(|t| t.collection_name.to_string())
                .collect(),
            contract_addr: mutation.contract_address.to_string(),
        };
        let processor = Arc::new(
            EventProcessor::new(config, self.db_store.clone())
                .await
                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?,
        );
        match self.processor_mapping.lock() {
            Ok(mut mapping) => {
                //TODO limit the total count
                if mapping.contains_key(mutation.contract_address.as_str()) {
                    warn!("contract addr {} exist", mutation.contract_address.as_str());
                    return Err(DB3Error::WriteStoreError(format!(
                        "contract_addr {} exist",
                        mutation.contract_address.as_str()
                    )));
                }
                mapping.insert(mutation.contract_address.to_string(), processor.clone());
            }
            _ => todo!(),
        }

        task::spawn(async move {
            if let Err(e) = processor
                .start()
                .await
                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))
            {
                warn!("fail to start event processor for {e}");
            }
        });
        Ok(())
    }

    async fn parse_and_apply_mutations(&self, mutations: &Vec<MutationWrapper>) -> Result<()> {
        for mutation in mutations.iter() {
            let body = mutation.body.as_ref().unwrap();
            // validate the signature
            let (dm, address, nonce) =
                MutationUtil::unwrap_and_light_verify(&body.payload, body.signature.as_str())
                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
            let action = MutationAction::from_i32(dm.action).ok_or(DB3Error::WriteStoreError(
                "fail to convert action type".to_string(),
            ))?;
            let (block, order) = match &mutation.header {
                Some(header) => Ok((header.block_id, header.order_id)),
                _ => Err(DB3Error::WriteStoreError(
                    "invalid mutation header".to_string(),
                )),
            }?;
            match action {
                MutationAction::CreateEventDb => {
                    for body in dm.bodies {
                        if let Some(Body::EventDatabaseMutation(ref mutation)) = &body.body {
                            let db_id = self
                                .db_store
                                .create_event_database(
                                    &address,
                                    mutation,
                                    nonce,
                                    self.network_id.load(Ordering::Relaxed),
                                    block,
                                    order,
                                )
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            self.start_an_event_task(db_id.address(), mutation)
                                .await
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            let db_id_hex = db_id.to_hex();
                            info!(
                                "add event database with addr {} from owner {}",
                                db_id_hex.as_str(),
                                address.to_hex().as_str()
                            );
                            break;
                        }
                    }
                }
                MutationAction::CreateDocumentDb => {
                    for body in dm.bodies {
                        if let Some(Body::DocDatabaseMutation(ref doc_db_mutation)) = &body.body {
                            let id = self
                                .db_store
                                .create_doc_database(
                                    &address,
                                    doc_db_mutation,
                                    nonce,
                                    self.network_id.load(Ordering::Relaxed),
                                    block,
                                    order,
                                )
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            info!(
                                "add database with addr {} from owner {}",
                                id.to_hex().as_str(),
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
                            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                        if let Some(Body::CollectionMutation(ref col_mutation)) = &body.body {
                            self.db_store
                                .create_collection(
                                    &address,
                                    &db_addr,
                                    col_mutation,
                                    block,
                                    order,
                                    i as u16,
                                )
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            info!(
                                    "add collection with db_addr {}, collection_name: {}, from owner {}",
                                    db_addr.to_hex().as_str(),
                                    col_mutation.collection_name.as_str(),
                                    address.to_hex().as_str()
                                );
                        }
                    }
                }
                MutationAction::UpdateDocument => {
                    for (_i, body) in dm.bodies.iter().enumerate() {
                        let db_address_ref: &[u8] = body.db_address.as_ref();
                        let db_addr = DB3Address::try_from(db_address_ref)
                            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                        if let Some(Body::DocumentMutation(ref doc_mutation)) = &body.body {
                            if doc_mutation.documents.len() != doc_mutation.ids.len() {
                                let msg = format!(
                                    "doc ids size {} not equal to documents size {}",
                                    doc_mutation.ids.len(),
                                    doc_mutation.documents.len()
                                );
                                warn!("{}", msg.as_str());
                                return Err(DB3Error::InvalidMutationError(msg));
                            }
                            let mut docs = Vec::<String>::new();
                            for buf in doc_mutation.documents.iter() {
                                let document = bytes_to_bson_document(buf.clone())
                                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                                let doc_str = document.to_string();
                                debug!("add document: {}", doc_str);
                                docs.push(doc_str);
                            }
                            self.db_store
                                .update_docs(
                                    &db_addr,
                                    &address,
                                    doc_mutation.collection_name.as_str(),
                                    &docs,
                                    &doc_mutation.ids,
                                )
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            info!(
                                    "update documents with db_addr {}, collection_name: {}, from owner {}",
                                    db_addr.to_hex().as_str(),
                                    doc_mutation.collection_name.as_str(),
                                    address.to_hex().as_str()
                                );
                        }
                    }
                }
                MutationAction::DeleteDocument => {
                    for (_i, body) in dm.bodies.iter().enumerate() {
                        let db_address_ref: &[u8] = body.db_address.as_ref();
                        let db_addr = DB3Address::try_from(db_address_ref)
                            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                        if let Some(Body::DocumentMutation(ref doc_mutation)) = &body.body {
                            self.db_store
                                .delete_docs(
                                    &db_addr,
                                    &address,
                                    doc_mutation.collection_name.as_str(),
                                    &doc_mutation.ids,
                                )
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            info!(
                                    "delete documents with db_addr {}, collection_name: {}, from owner {}",
                                    db_addr.to_hex().as_str(),
                                    doc_mutation.collection_name.as_str(),
                                    address.to_hex().as_str()
                                );
                        }
                    }
                }

                MutationAction::AddDocument => {
                    for (_i, body) in dm.bodies.iter().enumerate() {
                        let db_address_ref: &[u8] = body.db_address.as_ref();
                        let db_addr = DB3Address::try_from(db_address_ref)
                            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                        if let Some(Body::DocumentMutation(ref doc_mutation)) = &body.body {
                            let mut docs = Vec::<String>::new();
                            for buf in doc_mutation.documents.iter() {
                                let document = bytes_to_bson_document(buf.clone())
                                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                                let doc_str = document.to_string();
                                debug!("add document: {}", doc_str);
                                docs.push(doc_str);
                            }
                            let ids = self
                                .db_store
                                .add_docs(
                                    &db_addr,
                                    &address,
                                    doc_mutation.collection_name.as_str(),
                                    &docs,
                                )
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            info!(
                                    "add documents with db_addr {}, collection_name: {}, from owner {}, ids: {:?}",
                                    db_addr.to_hex().as_str(),
                                    doc_mutation.collection_name.as_str(),
                                    address.to_hex().as_str(),
                                    ids
                                );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl IndexerNode for IndexerNodeImpl {
    async fn get_contract_sync_status(
        &self,
        _request: Request<GetContractSyncStatusRequest>,
    ) -> std::result::Result<Response<GetContractSyncStatusResponse>, Status> {
        let status_list = match self.processor_mapping.lock() {
            Ok(mapping) => mapping
                .iter()
                .map(|ref processor| ContractSyncStatus {
                    addr: processor.1.get_config().contract_addr.to_string(),
                    evm_node_url: processor.1.get_config().evm_node_url.to_string(),
                    block_number: processor.1.get_block_number(),
                    event_number: processor.1.get_event_number(),
                })
                .collect(),
            _ => todo!(),
        };
        Ok(Response::new(GetContractSyncStatusResponse { status_list }))
    }

    async fn setup(
        &self,
        request: Request<SetupRequest>,
    ) -> std::result::Result<Response<SetupResponse>, Status> {
        let r = request.into_inner();
        let (addr, data) = MutationUtil::verify_setup(&r.payload, r.signature.as_str())
            .map_err(|e| Status::internal(format!("{e}")))?;
        let _rollup_interval = MutationUtil::get_u64_field(&data, "rollupInterval", 0);
        let _min_rollup_size = MutationUtil::get_u64_field(&data, "minRollupSize", 0);
        let evm_node_rpc =
            MutationUtil::get_str_field(&data, "evmNodeRpc", self.evm_node_url.as_str());
        let network = MutationUtil::get_u64_field(&data, "network", 0_u64);
        let admin_addr =
            MetaStoreClient::get_admin(self.contract_addr.as_str(), evm_node_rpc, network)
                .await
                .map_err(|e| Status::internal(format!("{e}")))?;
        if admin_addr != addr {
            return Ok(Response::new(SetupResponse {
                code: -1,
                msg: "you are not the admin".to_string(),
            }));
        }
        self.network_id.store(network, Ordering::Relaxed);
        return Ok(Response::new(SetupResponse {
            code: 0,
            msg: "ok".to_string(),
        }));
    }

    async fn get_system_status(
        &self,
        _request: Request<GetSystemStatusRequest>,
    ) -> std::result::Result<Response<SystemStatus>, Status> {
        let wallet = Self::build_wallet(self.key_root_path.as_str())
            .map_err(|e| Status::internal(format!("{e}")))?;
        let addr = format!("0x{}", hex::encode(wallet.address().as_bytes()));
        Ok(Response::new(SystemStatus {
            evm_account: addr,
            evm_balance: "0".to_string(),
            node_url: self.node_url.to_string(),
            config: None,
        }))
    }

    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> std::result::Result<Response<RunQueryResponse>, Status> {
        let r = request.into_inner();
        let addr =
            DB3Address::from_hex(r.db.as_str()).map_err(|e| Status::internal(format!("{e}")))?;
        if let Some(q) = &r.query {
            info!("query str {} q {:?}", q.query_str, q);
            let documents = self
                .db_store
                .query_docs(&addr, r.col_name.as_str(), q)
                .map_err(|e| Status::internal(format!("{e}")))?;

            info!(
                "query str {} from collection {} in db {} with result len {}, parameters len {}",
                q.query_str,
                r.col_name.as_str(),
                r.db.as_str(),
                documents.len(),
                q.parameters.len()
            );
            Ok(Response::new(RunQueryResponse { documents }))
        } else {
            Err(Status::internal("no query provided".to_string()))
        }
    }
}
#[cfg(test)]
mod tests {}
