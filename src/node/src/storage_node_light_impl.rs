//
// stroage_node_light_impl.rs
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
use crate::rollup_executor::{RollupExecutor, RollupExecutorConfig};
use db3_crypto::db3_address::DB3Address;
use db3_crypto::id::TxId;
use db3_error::Result;
use db3_proto::db3_mutation_v2_proto::{
    mutation::body_wrapper::Body, MutationAction, MutationRollupStatus,
};
use db3_proto::db3_storage_proto::{
    storage_node_server::StorageNode, ExtraItem, GetDatabaseOfOwnerRequest,
    GetDatabaseOfOwnerResponse, GetMutationBodyRequest, GetMutationBodyResponse,
    GetMutationHeaderRequest, GetMutationHeaderResponse, GetNonceRequest, GetNonceResponse,
    ScanMutationHeaderRequest, ScanMutationHeaderResponse, ScanRollupRecordRequest,
    ScanRollupRecordResponse, SendMutationRequest, SendMutationResponse,
};
use db3_storage::db_store_v2::{DBStoreV2, DBStoreV2Config};
use db3_storage::mutation_store::{MutationStore, MutationStoreConfig};
use db3_storage::state_store::{StateStore, StateStoreConfig};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

pub struct StorageNodeV2Config {
    pub store_config: MutationStoreConfig,
    pub state_config: StateStoreConfig,
    pub rollup_config: RollupExecutorConfig,
    pub db_store_config: DBStoreV2Config,
    pub network_id: u64,
    pub block_interval: u64,
}

pub struct StorageNodeV2Impl {
    storage: MutationStore,
    state_store: StateStore,
    config: StorageNodeV2Config,
    running: Arc<AtomicBool>,
    db_store: DBStoreV2,
}

impl StorageNodeV2Impl {
    pub fn new(config: StorageNodeV2Config) -> Result<Self> {
        let storage = MutationStore::new(config.store_config.clone())?;
        let state_store = StateStore::new(config.state_config.clone())?;
        let db_store = DBStoreV2::new(config.db_store_config.clone())?;
        Ok(Self {
            storage,
            state_store,
            config,
            running: Arc::new(AtomicBool::new(true)),
            db_store,
        })
    }

    pub fn start_to_produce_block(&self) {
        let local_running = self.running.clone();
        let local_storage = self.storage.clone();
        let local_block_interval = self.config.block_interval;
        task::spawn_blocking(move || {
            info!("start the block producer thread");
            while local_running.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(local_block_interval));
                match local_storage.increase_block() {
                    Ok(()) => {}
                    Err(e) => {
                        warn!("fail to produce block for error {e}");
                    }
                }
            }
        });
    }

    pub async fn start_to_rollup(&self) {
        let local_running = self.running.clone();
        let local_storage = self.storage.clone();
        let rollup_config = self.config.rollup_config.clone();
        task::spawn(async move {
            info!("start the rollup thread");
            let rollup_interval = rollup_config.rollup_interval;
            //TODO handle err
            let executor = RollupExecutor::new(rollup_config, local_storage).unwrap();
            while local_running.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(rollup_interval));
                match executor.process().await {
                    Ok(()) => {}
                    Err(e) => {
                        warn!("fail to rollup for error {e}");
                    }
                }
            }
        });
    }
}

#[tonic::async_trait]
impl StorageNode for StorageNodeV2Impl {
    async fn get_database_of_owner(
        &self,
        request: Request<GetDatabaseOfOwnerRequest>,
    ) -> std::result::Result<Response<GetDatabaseOfOwnerResponse>, Status> {
        let r = request.into_inner();
        let addr =
            DB3Address::from_hex(r.owner.as_str()).map_err(|e| Status::internal(format!("{e}")))?;
        let databases = self
            .db_store
            .get_database_of_owner(&addr)
            .map_err(|e| Status::internal(format!("{e}")))?;
        Ok(Response::new(GetDatabaseOfOwnerResponse { databases }))
    }

    async fn get_mutation_body(
        &self,
        request: Request<GetMutationBodyRequest>,
    ) -> std::result::Result<Response<GetMutationBodyResponse>, Status> {
        let r = request.into_inner();
        let tx_id =
            TxId::try_from_hex(r.id.as_str()).map_err(|e| Status::internal(format!("{e}")))?;
        let body = self
            .storage
            .get_mutation(&tx_id)
            .map_err(|e| Status::internal(format!("{e}")))?;
        Ok(Response::new(GetMutationBodyResponse { body }))
    }

    async fn scan_rollup_record(
        &self,
        request: Request<ScanRollupRecordRequest>,
    ) -> std::result::Result<Response<ScanRollupRecordResponse>, Status> {
        let r = request.into_inner();
        let records = self
            .storage
            .scan_rollup_records(r.start, r.limit)
            .map_err(|e| Status::internal(format!("{e}")))?;
        Ok(Response::new(ScanRollupRecordResponse { records }))
    }

    async fn scan_mutation_header(
        &self,
        request: Request<ScanMutationHeaderRequest>,
    ) -> std::result::Result<Response<ScanMutationHeaderResponse>, Status> {
        let r = request.into_inner();
        let headers = self
            .storage
            .scan_mutation_headers(r.start, r.limit)
            .map_err(|e| Status::internal(format!("{e}")))?;
        Ok(Response::new(ScanMutationHeaderResponse { headers }))
    }

    async fn get_mutation_header(
        &self,
        request: Request<GetMutationHeaderRequest>,
    ) -> std::result::Result<Response<GetMutationHeaderResponse>, Status> {
        let r = request.into_inner();
        let header = self
            .storage
            .get_mutation_header(r.block_id, r.order_id)
            .map_err(|e| Status::internal(format!("{e}")))?;
        Ok(Response::new(GetMutationHeaderResponse {
            header,
            status: MutationRollupStatus::Pending.into(),
            rollup_tx: vec![],
        }))
    }

    async fn get_nonce(
        &self,
        request: Request<GetNonceRequest>,
    ) -> std::result::Result<Response<GetNonceResponse>, Status> {
        let r = request.into_inner();
        let address = DB3Address::try_from(r.address.as_str())
            .map_err(|e| Status::internal(format!("{e}")))?;
        let used_nonce = self
            .state_store
            .get_nonce(&address)
            .map_err(|e| Status::internal(format!("{e}")))?;
        debug!("address {} used nonce {}", address.to_hex(), used_nonce);
        Ok(Response::new(GetNonceResponse {
            nonce: used_nonce + 1,
        }))
    }

    async fn send_mutation(
        &self,
        request: Request<SendMutationRequest>,
    ) -> std::result::Result<Response<SendMutationResponse>, Status> {
        let r = request.into_inner();
        // validate the signature
        let (dm, address, nonce) =
            MutationUtil::unwrap_and_light_verify(&r.payload, r.signature.as_str())
                .map_err(|e| Status::internal(format!("{e}")))?;
        let action = MutationAction::from_i32(dm.action)
            .ok_or(Status::internal("fail to convert action type".to_string()))?;
        // TODO validate the database mutation
        match self.state_store.incr_nonce(&address, nonce) {
            Ok(_) => {
                // mutation id
                let (id, block, order) = self
                    .storage
                    .add_mutation(&r.payload, r.signature.as_str(), &address)
                    .map_err(|e| Status::internal(format!("{e}")))?;
                match action {
                    MutationAction::CreateDocumentDb => {
                        let mut items: Vec<ExtraItem> = Vec::new();
                        for body in dm.bodies {
                            if let Some(Body::DocDatabaseMutation(ref doc_db_mutation)) = &body.body
                            {
                                let db_id = self
                                    .db_store
                                    .create_doc_database(
                                        &address,
                                        doc_db_mutation,
                                        nonce,
                                        self.config.network_id,
                                        block,
                                        order,
                                    )
                                    .map_err(|e| Status::internal(format!("{e}")))?;
                                let item = ExtraItem {
                                    key: "db_addr".to_string(),
                                    value: db_id.to_hex(),
                                };
                                items.push(item);
                                break;
                            }
                        }
                        Ok(Response::new(SendMutationResponse {
                            id,
                            code: 0,
                            msg: "ok".to_string(),
                            items,
                            block,
                            order,
                        }))
                    }
                    MutationAction::AddCollection => {
                        let mut items: Vec<ExtraItem> = Vec::new();
                        for (i, body) in dm.bodies.iter().enumerate() {
                            let db_address_ref: &[u8] = body.db_address.as_ref();
                            let db_addr = DB3Address::try_from(db_address_ref)
                                .map_err(|e| Status::internal(format!("{e}")))?;
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
                                    .map_err(|e| Status::internal(format!("{e}")))?;
                                let item = ExtraItem {
                                    key: "collection".to_string(),
                                    value: col_mutation.collection_name.to_string(),
                                };
                                items.push(item);
                            }
                        }
                        Ok(Response::new(SendMutationResponse {
                            id,
                            code: 0,
                            msg: "ok".to_string(),
                            items,
                            block,
                            order,
                        }))
                    }
                    _ => Ok(Response::new(SendMutationResponse {
                        id,
                        code: 0,
                        msg: "ok".to_string(),
                        items: vec![],
                        block,
                        order,
                    })),
                }
            }
            Err(_e) => Ok(Response::new(SendMutationResponse {
                id: "".to_string(),
                code: 1,
                msg: "bad nonce".to_string(),
                items: vec![],
                block: 0,
                order: 0,
            })),
        }
    }
}
