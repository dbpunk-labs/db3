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
use db3_base::strings;
use db3_crypto::db3_address::DB3Address;
use db3_crypto::id::TxId;
use db3_error::{DB3Error, Result};
use db3_event::meta_store_event_processor::MetaStoreEventProcessor;
use db3_proto::db3_mutation_v2_proto::{MutationAction, MutationRollupStatus};
use db3_proto::db3_storage_proto::block_response;
use db3_proto::db3_storage_proto::event_message::Event as EventV2;
use db3_proto::db3_storage_proto::{
    storage_node_server::StorageNode, BlockRequest, BlockResponse, GetCollectionOfDatabaseRequest,
    GetCollectionOfDatabaseResponse, GetDatabaseOfOwnerRequest, GetDatabaseOfOwnerResponse,
    GetDatabaseRequest, GetDatabaseResponse, GetMutationBodyRequest, GetMutationBodyResponse,
    GetMutationHeaderRequest, GetMutationHeaderResponse, GetMutationStateRequest,
    GetMutationStateResponse, GetNonceRequest, GetNonceResponse, MutationStateView,
    ScanGcRecordRequest, ScanGcRecordResponse, ScanMutationHeaderRequest,
    ScanMutationHeaderResponse, ScanRollupRecordRequest, ScanRollupRecordResponse,
    SendMutationRequest, SendMutationResponse, SubscribeRequest,
};
use db3_proto::db3_storage_proto::{
    BlockEvent as BlockEventV2, EventMessage as EventMessageV2, EventType as EventTypeV2,
    Subscription as SubscriptionV2,
};
use db3_storage::db_store_v2::{DBStoreV2, DBStoreV2Config};
use db3_storage::mutation_store::{MutationStore, MutationStoreConfig};
use db3_storage::state_store::StateStore;
use db3_storage::system_store::{SystemRole, SystemStore};
use ethers::core::types::Bytes as EthersBytes;
use ethers::types::U256;
use prost::Message;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;
use tokio::time::{sleep, Duration as TokioDuration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct StorageNodeV2Config {
    pub store_config: MutationStoreConfig,
    pub rollup_config: RollupExecutorConfig,
    pub db_store_config: DBStoreV2Config,
    pub block_interval: u64,
}

pub struct StorageNodeV2Impl {
    storage: MutationStore,
    state_store: Arc<StateStore>,
    config: StorageNodeV2Config,
    running: Arc<AtomicBool>,
    db_store: DBStoreV2,
    sender: Sender<(
        DB3Address,
        SubscriptionV2,
        Sender<std::result::Result<EventMessageV2, Status>>,
    )>,
    broadcast_sender: BroadcastSender<EventMessageV2>,
    rollup_executor: Arc<RollupExecutor>,
    rollup_interval: Arc<AtomicU64>,
    network_id: Arc<AtomicU64>,
    system_store: Arc<SystemStore>,
    event_processor: Arc<MetaStoreEventProcessor>,
}

impl StorageNodeV2Impl {
    pub async fn new(
        config: StorageNodeV2Config,
        system_store: Arc<SystemStore>,
        state_store: Arc<StateStore>,
        sender: Sender<(
            DB3Address,
            SubscriptionV2,
            Sender<std::result::Result<EventMessageV2, Status>>,
        )>,
    ) -> Result<Self> {
        std::fs::create_dir_all(config.rollup_config.key_root_path.as_str())
            .map_err(|e| DB3Error::InvalidKeyPathError(format!("{e}")))?;
        let storage = MutationStore::new(config.store_config.clone())?;
        storage.recover()?;
        let db_store = DBStoreV2::new(config.db_store_config.clone())?;
        let (broadcast_sender, _) = broadcast::channel(1024);
        let event_processor = Arc::new(MetaStoreEventProcessor::new(
            state_store.clone(),
            db_store.clone(),
            storage.clone(),
            system_store.clone(),
        ));
        if let Some(c) = system_store.get_config(&SystemRole::DataRollupNode)? {
            info!("init storage node from persistence config {:?}", c);
            let network_id = Arc::new(AtomicU64::new(c.network_id));
            let rollup_executor = Arc::new(
                RollupExecutor::new(
                    config.rollup_config.clone(),
                    storage.clone(),
                    system_store.clone(),
                )
                .await?,
            );
            event_processor
                .start(
                    c.contract_addr.as_str(),
                    c.evm_node_url.as_str(),
                    0,
                    c.chain_id,
                    c.network_id,
                )
                .await?;
            let rollup_interval = c.rollup_interval;
            Ok(Self {
                storage,
                state_store,
                config,
                running: Arc::new(AtomicBool::new(true)),
                db_store,
                sender,
                broadcast_sender,
                rollup_executor,
                rollup_interval: Arc::new(AtomicU64::new(rollup_interval)),
                network_id,
                system_store,
                event_processor,
            })
        } else {
            info!("please setup the node first");
            let network_id = Arc::new(AtomicU64::new(0));
            let rollup_executor = Arc::new(
                RollupExecutor::new(
                    config.rollup_config.clone(),
                    storage.clone(),
                    system_store.clone(),
                )
                .await?,
            );
            Ok(Self {
                storage,
                state_store,
                config,
                running: Arc::new(AtomicBool::new(true)),
                db_store,
                sender,
                broadcast_sender,
                rollup_executor,
                rollup_interval: Arc::new(AtomicU64::new(1000 * 10 * 60)),
                network_id,
                system_store,
                event_processor,
            })
        }
    }

    pub fn recover(&self) -> Result<()> {
        self.db_store.recover_db_state()?;
        Ok(())
    }

    pub async fn start_bg_task(&self) {
        self.start_to_produce_block().await;
        self.start_to_rollup().await;
        self.start_flush_state().await;
    }

    async fn start_flush_state(&self) {
        let local_db_store = self.db_store.clone();
        let local_running = self.running.clone();
        task::spawn(async move {
            info!("start the database meta flush thread");
            while local_running.load(Ordering::Relaxed) {
                sleep(TokioDuration::from_millis(60000)).await;
                match local_db_store.flush_database_state() {
                    Ok(_) => {
                        info!("flush database meta done");
                    }
                    Err(e) => {
                        warn!("flush database meta error {e}");
                    }
                }
            }
            info!("exit the flush thread");
        });
    }

    async fn start_to_produce_block(&self) {
        let local_running = self.running.clone();
        let local_storage = self.storage.clone();
        let local_block_interval = self.config.block_interval;
        let local_event_sender = self.broadcast_sender.clone();
        task::spawn(async move {
            info!("start the block producer thread");
            while local_running.load(Ordering::Relaxed) {
                sleep(TokioDuration::from_millis(local_block_interval)).await;
                debug!(
                    "produce block {}",
                    local_storage.get_current_block().unwrap_or(0)
                );
                match local_storage.increase_block_return_last_state() {
                    Ok((block_id, mutation_count)) => {
                        // sender block event
                        let e = BlockEventV2 {
                            block_id,
                            mutation_count,
                        };
                        let msg = EventMessageV2 {
                            r#type: EventTypeV2::Block as i32,
                            event: Some(EventV2::BlockEvent(e)),
                        };
                        match local_event_sender.send(msg) {
                            Ok(_) => {
                                debug!("broadcast block event {}, {}", block_id, mutation_count);
                            }
                            Err(e) => {
                                warn!("the broadcast channel error for {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("fail to produce block for error {e}");
                    }
                }
            }
            info!("exit the block producer thread");
        });
    }

    async fn start_to_rollup(&self) {
        let local_running = self.running.clone();
        let executor = self.rollup_executor.clone();
        let rollup_interval = self.rollup_interval.clone();
        task::spawn(async move {
            info!("start the rollup thread");
            while local_running.load(Ordering::Relaxed) {
                sleep(TokioDuration::from_millis(
                    rollup_interval.load(Ordering::Relaxed),
                ))
                .await;
                match executor.process().await {
                    Ok(()) => {}
                    Err(e) => {
                        warn!("fail to rollup for error {e}");
                    }
                }
            }
            info!("exit the rollup thread");
        });
    }

    pub async fn keep_subscription(
        &self,
        mut receiver: Receiver<(
            DB3Address,
            SubscriptionV2,
            Sender<std::result::Result<EventMessageV2, Status>>,
        )>,
        mut update_receiver: Receiver<()>,
    ) -> std::result::Result<(), Status> {
        info!("start to keep subscription");
        let local_running = self.running.clone();
        let local_broadcast_sender = self.broadcast_sender.clone();
        let local_rollup_interval = self.rollup_interval.clone();
        let local_rollup_executor = self.rollup_executor.clone();
        let local_system_store = self.system_store.clone();
        let local_network_id = self.network_id.clone();
        let local_event_processor = self.event_processor.clone();
        tokio::spawn(async move {
            info!("listen to subscription update event and event message broadcaster");
            while local_running.load(Ordering::Relaxed) {
                info!("keep subscription loop");
                let mut subscribers: BTreeMap<
                    DB3Address,
                    (
                        Sender<std::result::Result<EventMessageV2, Status>>,
                        SubscriptionV2,
                    ),
                > = BTreeMap::new();
                let mut to_be_removed: HashSet<DB3Address> = HashSet::new();
                let mut event_sub = local_broadcast_sender.subscribe();
                while local_running.load(Ordering::Relaxed) {
                    tokio::select! {
                        Some(()) = update_receiver.recv() => {
                            if let Ok(Some(c)) = local_system_store.get_config(&SystemRole::DataRollupNode) {
                                local_network_id.store(c.network_id, Ordering::Relaxed);
                                local_rollup_interval.store(c.rollup_interval, Ordering::Relaxed);
                                info!("update the network {} and rollup interval {}", local_network_id.load(Ordering::Relaxed),
                                local_rollup_interval.load(Ordering::Relaxed)
                                );
                                if let Err(e) = local_event_processor.start(c.contract_addr.as_str(), c.evm_node_url.as_str(), 0, c.chain_id, c.network_id).await {
                                    warn!("fail update the event processor with error {e}");
                                }
                            }
                            if let Err(e) = local_rollup_executor.update_config().await {
                                warn!("fail update rollup executor config for {e}");
                            }
                        }
                        Some((addr, sub, sender)) = receiver.recv() => {
                            info!("add or update the subscriber with addr 0x{}", hex::encode(addr.as_ref()));
                            //TODO limit the max address count
                            subscribers.insert(addr, (sender, sub));
                            info!("subscribers len : {}", subscribers.len());
                        }

                        Ok(event) = event_sub.recv() => {
                            debug!("receive event {:?}", event);
                            for (key , (sender, sub)) in subscribers.iter() {
                                if sender.is_closed() {
                                    to_be_removed.insert(key.clone());
                                    warn!("the channel has been closed by client for addr 0x{}", hex::encode(key.as_ref()));
                                    continue;
                                }
                                for idx in 0..sub.topics.len() {
                                    if sub.topics[idx] != EventTypeV2::Block as i32 {
                                        continue;
                                    }
                                    match sender.try_send(Ok(event.clone())) {
                                        Ok(_) => {
                                            debug!("send event to addr 0x{}", hex::encode(key.as_ref()));
                                            break;
                                        }
                                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                            // retry?
                                            // TODO
                                            warn!("the channel is full for addr 0x{}", hex::encode(key.as_ref()));
                                        }
                                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                            // remove the address
                                            to_be_removed.insert(key.clone());
                                            warn!("the channel has been closed by client for addr 0x{}", hex::encode(key.as_ref()));
                                        }

                                    }
                                }
                            }
                        },
                        else => {
                            info!("unexpected channel update");
                            // reconnect in 5 seconds
                            sleep(TokioDuration::from_millis(1000 * 5)).await;
                            break;
                        }

                    }
                    for k in to_be_removed.iter() {
                        subscribers.remove(k);
                    }
                    to_be_removed.clear();
                }
            }
            info!("exit the keep subscription thread");
        });
        Ok(())
    }
}

#[tonic::async_trait]
impl StorageNode for StorageNodeV2Impl {
    async fn get_mutation_state(
        &self,
        _request: Request<GetMutationStateRequest>,
    ) -> std::result::Result<Response<GetMutationStateResponse>, Status> {
        let state = self.storage.get_latest_state();
        let total_storage_cost = strings::ar_to_readable_num_str(U256::from_big_endian(
            state.total_storage_cost.as_ref() as &[u8],
        ));
        let total_evm_cost = strings::evm_to_readable_num_str(U256::from_big_endian(
            state.total_evm_cost.as_ref() as &[u8],
        ));
        let view = MutationStateView {
            mutation_count: state.mutation_count,
            total_mutation_bytes: state.total_mutation_bytes,
            gc_count: state.gc_count,
            rollup_count: state.rollup_count,
            total_rollup_bytes: state.total_rollup_bytes,
            total_gc_bytes: state.total_gc_bytes,
            total_rollup_raw_bytes: state.total_rollup_raw_bytes,
            total_rollup_mutation_count: state.total_rollup_mutation_count,
            total_storage_cost,
            total_evm_cost,
        };
        Ok(Response::new(GetMutationStateResponse { view: Some(view) }))
    }

    async fn scan_gc_record(
        &self,
        request: Request<ScanGcRecordRequest>,
    ) -> std::result::Result<Response<ScanGcRecordResponse>, Status> {
        let r = request.into_inner();
        let records = self
            .storage
            .scan_gc_records(r.start, r.limit)
            .map_err(|e| Status::internal(format!("{e}")))?;
        Ok(Response::new(ScanGcRecordResponse { records }))
    }

    type SubscribeStream = ReceiverStream<std::result::Result<EventMessageV2, Status>>;
    /// add subscription to the light node
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> std::result::Result<Response<Self::SubscribeStream>, Status> {
        let r = request.into_inner();
        if r.payload.len() == 0 || r.signature.len() == 0 {
            return Err(Status::invalid_argument("the payload or signature is null"));
        }
        let sender = self.sender.clone();
        let (address, data) = MutationUtil::verify_setup(r.payload.as_ref(), r.signature.as_str())
            .map_err(|e| Status::invalid_argument(format!("invalid signature with error {e}")))?;
        if let Some(payload) = data.message.get("payload") {
            let db3_address = DB3Address::from(address.as_fixed_bytes());
            let data: EthersBytes = serde_json::from_value(payload.clone()).map_err(|e| {
                Status::invalid_argument(format!("decode the payload failed for error {e}"))
            })?;
            let subscription = SubscriptionV2::decode(data.as_ref()).map_err(|e| {
                Status::invalid_argument(format!("decode the data to object failed for error {e}"))
            })?;
            info!(
                "add subscriber for addr 0x{}",
                hex::encode(address.as_ref())
            );
            let (msg_sender, msg_receiver) =
                tokio::sync::mpsc::channel::<std::result::Result<EventMessageV2, Status>>(10);
            sender
                .try_send((db3_address, subscription, msg_sender))
                .map_err(|e| Status::internal(format!("fail to add subscriber for {e}")))?;
            Ok(Response::new(ReceiverStream::new(msg_receiver)))
        } else {
            Err(Status::invalid_argument(
                "payload was not found from the message".to_string(),
            ))
        }
    }

    async fn get_block(
        &self,
        request: Request<BlockRequest>,
    ) -> std::result::Result<Response<BlockResponse>, Status> {
        let r = request.into_inner();
        let mutation_header_bodys = self
            .storage
            .get_range_mutations(r.block_start, r.block_end)
            .map_err(|e| Status::internal(format!("{e}")))?;
        let mutations = mutation_header_bodys
            .iter()
            .map(|(h, b)| block_response::MutationWrapper {
                header: Some(h.to_owned()),
                body: Some(b.to_owned()),
            })
            .collect();
        Ok(Response::new(BlockResponse { mutations }))
    }

    async fn get_database(
        &self,
        request: Request<GetDatabaseRequest>,
    ) -> std::result::Result<Response<GetDatabaseResponse>, Status> {
        let r = request.into_inner();
        let addr = DB3Address::from_hex(r.addr.as_str())
            .map_err(|e| Status::invalid_argument(format!("invalid database address {e}")))?;
        let database = self
            .db_store
            .get_database(&addr)
            .map_err(|e| Status::internal(format!("fail to get database {e}")))?;
        let state = self.db_store.get_database_state(&addr);
        Ok(Response::new(GetDatabaseResponse { database, state }))
    }

    async fn get_collection_of_database(
        &self,
        request: Request<GetCollectionOfDatabaseRequest>,
    ) -> std::result::Result<Response<GetCollectionOfDatabaseResponse>, Status> {
        let r = request.into_inner();
        let addr = DB3Address::from_hex(r.db_addr.as_str())
            .map_err(|e| Status::invalid_argument(format!("invalid database address {e}")))?;
        let (collections, collection_states) = self
            .db_store
            .get_collection_of_database(&addr)
            .map_err(|e| Status::internal(format!("fail to get collect of database {e}")))?;
        info!(
            "query collection count {} with database {}",
            collections.len(),
            r.db_addr.as_str()
        );
        Ok(Response::new(GetCollectionOfDatabaseResponse {
            collections,
            states: collection_states,
        }))
    }

    async fn get_database_of_owner(
        &self,
        request: Request<GetDatabaseOfOwnerRequest>,
    ) -> std::result::Result<Response<GetDatabaseOfOwnerResponse>, Status> {
        let r = request.into_inner();
        let addr = DB3Address::from_hex(r.owner.as_str())
            .map_err(|e| Status::invalid_argument(format!("invalid database address {e}")))?;
        let (databases, states) = self
            .db_store
            .get_database_of_owner(&addr)
            .map_err(|e| Status::internal(format!("{e}")))?;
        info!(
            "query database list count {} with account {}",
            databases.len(),
            r.owner.as_str()
        );
        Ok(Response::new(GetDatabaseOfOwnerResponse {
            databases,
            states,
        }))
    }

    async fn get_mutation_body(
        &self,
        request: Request<GetMutationBodyRequest>,
    ) -> std::result::Result<Response<GetMutationBodyResponse>, Status> {
        let r = request.into_inner();
        let tx_id = TxId::try_from_hex(r.id.as_str())
            .map_err(|e| Status::invalid_argument(format!("invalid mutation id {e}")))?;
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
        let mut records_pending = vec![self.rollup_executor.get_pending_rollup()];
        let records_done = self
            .storage
            .scan_rollup_records(r.start, r.limit)
            .map_err(|e| Status::internal(format!("{e}")))?;
        records_pending.extend_from_slice(&records_done);
        Ok(Response::new(ScanRollupRecordResponse {
            records: records_pending,
        }))
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
        info!(
            "scan mutation headers {} with start {} and limit {}",
            headers.len(),
            r.start,
            r.limit
        );
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
            .map_err(|e| Status::invalid_argument(format!("invalid account address {e}")))?;
        let used_nonce = self
            .state_store
            .get_nonce(&address)
            .map_err(|e| Status::internal(format!("{e}")))?;
        info!("address {} used nonce {}", address.to_hex(), used_nonce);
        Ok(Response::new(GetNonceResponse {
            nonce: used_nonce + 1,
        }))
    }

    async fn send_mutation(
        &self,
        request: Request<SendMutationRequest>,
    ) -> std::result::Result<Response<SendMutationResponse>, Status> {
        let network = self.network_id.load(Ordering::Relaxed);
        if network == 0 {
            warn!("setup the node first");
            return Err(Status::internal(
                "the system has not been setup".to_string(),
            ));
        }
        let r = request.into_inner();
        let (dm, address, nonce) = MutationUtil::unwrap_and_light_verify(
            &r.payload,
            r.signature.as_str(),
        )
        .map_err(|e| {
            warn!("invalid signature for error {e}");
            Status::invalid_argument(format!("fail to verify the payload and signature {e}"))
        })?;
        let action = MutationAction::from_i32(dm.action)
            .ok_or(Status::invalid_argument("bad mutation action".to_string()))?;
        match self.state_store.incr_nonce(&address, nonce) {
            Ok(_) => {
                // mutation id
                let (id, block, order) = self
                    .storage
                    .generate_mutation_block_and_order(&r.payload, r.signature.as_str())
                    .map_err(|e| {
                        warn!("fail to generate the block and order for {e}");
                        Status::internal(format!("{e}"))
                    })?;
                let response = match self.db_store.apply_mutation(
                    action,
                    dm,
                    &address,
                    network,
                    nonce,
                    block,
                    order,
                    &HashMap::new(),
                ) {
                    Ok(items) => {
                        let doc_ids_map = MutationUtil::get_create_doc_ids_map(&items);
                        self.storage
                            .add_mutation(
                                &r.payload,
                                r.signature.as_str(),
                                doc_ids_map.as_str(),
                                &address,
                                nonce,
                                block,
                                order,
                                network,
                                action,
                            )
                            .map_err(|e| {
                                warn!("fail to add mutation for error {e}");
                                Status::internal(format!("{e}"))
                            })?;
                        Response::new(SendMutationResponse {
                            id,
                            code: 0,
                            msg: "ok".to_string(),
                            items,
                            block,
                            order,
                        })
                    }
                    Err(e) => {
                        warn!("fail to apply mutation for error {e}");
                        return Err(Status::internal(format!("{e}")));
                    }
                };
                Ok(response)
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::system_impl::SystemImpl;
    use db3_proto::db3_system_proto::system_server::System;
    use db3_proto::db3_system_proto::SetupRequest;
    use db3_storage::doc_store::DocStoreConfig;
    use db3_storage::state_store::StateStoreConfig;
    use db3_storage::system_store::SystemStoreConfig;

    use tempdir::TempDir;

    fn generate_rand_node_config(
        real_path: &str,
    ) -> (StateStoreConfig, SystemStoreConfig, StorageNodeV2Config) {
        if let Err(_e) = std::fs::create_dir_all(real_path) {}
        let rollup_config = RollupExecutorConfig {
            temp_data_path: format!("{real_path}/data_path"),
            key_root_path: format!("{real_path}/keys"),
            use_legacy_tx: false,
        };

        let system_store_config = SystemStoreConfig {
            key_root_path: rollup_config.key_root_path.to_string(),
            evm_wallet_key: "evm".to_string(),
            ar_wallet_key: "ar".to_string(),
        };

        let store_config = MutationStoreConfig {
            db_path: format!("{real_path}/mutation_path"),
            block_store_cf_name: "block_store_cf".to_string(),
            tx_store_cf_name: "tx_store_cf".to_string(),
            rollup_store_cf_name: "rollup_store_cf".to_string(),
            gc_cf_name: "gc_store_cf".to_string(),
            message_max_buffer: 4 * 1024,
            scan_max_limit: 50,
            block_state_cf_name: "block_state_cf".to_string(),
        };

        let state_config = StateStoreConfig {
            db_path: format!("{real_path}/state_store"),
        };

        if let Err(_e) = std::fs::create_dir_all(state_config.db_path.as_str()) {}

        let db_store_config = DBStoreV2Config {
            db_path: format!("{real_path}/db_path").to_string(),
            db_store_cf_name: "db_store_cf".to_string(),
            doc_store_cf_name: "doc_store_cf".to_string(),
            collection_store_cf_name: "col_store_cf".to_string(),
            index_store_cf_name: "idx_store_cf".to_string(),
            doc_owner_store_cf_name: "doc_owner_store_cf".to_string(),
            db_owner_store_cf_name: "db_owner_cf".to_string(),
            scan_max_limit: 1000,
            enable_doc_store: false,
            doc_store_conf: DocStoreConfig::default(),
            doc_start_id: 0,
        };

        (
            state_config,
            system_store_config,
            StorageNodeV2Config {
                store_config,
                rollup_config,
                db_store_config,
                block_interval: 10000,
            },
        )
    }
    #[tokio::test]
    async fn test_update_config() {
        let tmp_dir_path = TempDir::new("add_store_path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let (sender, receiver) = tokio::sync::mpsc::channel::<(
            DB3Address,
            SubscriptionV2,
            Sender<std::result::Result<EventMessageV2, Status>>,
        )>(1024);
        let (update_sender, update_receiver) = tokio::sync::mpsc::channel::<()>(1024);
        let (state_config, system_store_config, config) =
            generate_rand_node_config(real_path.as_str());
        let state_store = Arc::new(StateStore::new(state_config).unwrap());
        let system_store = Arc::new(SystemStore::new(system_store_config, state_store.clone()));
        let sig:&str = "0x279beccf8d7309fe6bdb2ca692cfd61278ab9166052d05297c74fcde5e2a345940b8b5b91fa646117e71c26c9a02d2b0f6f88904242919e64826a4e577aa6e0c1c";
        let payload: &str = r#"
        {"types":{"EIP712Domain":[],"Message":[{"name":"rollupInterval","type":"string"},{"name":"minRollupSize","type":"string"},{"name":"networkId","type":"string"},{"name":"chainId","type":"string"},{"name":"contractAddr","type":"address"},{"name":"rollupMaxInterval","type":"string"},{"name":"evmNodeUrl","type":"string"},{"name":"arNodeUrl","type":"string"},{"name":"minGcOffset","type":"string"}]},"domain":{},"primaryType":"Message","message":{"rollupInterval":"600000","minRollupSize":"1048576","networkId":"1","chainId":"80000","contractAddr":"0x5FbDB2315678afecb367f032d93F642f64180aa3","rollupMaxInterval":"6000000","evmNodeUrl":"ws://127.0.0.1:8545","arNodeUrl":"http://127.0.0.1:1984","minGcOffset":"864000"}}
            "#;
        let storage_node = StorageNodeV2Impl::new(
            config.clone(),
            system_store.clone(),
            state_store.clone(),
            sender,
        )
        .await
        .unwrap();
        storage_node
            .keep_subscription(receiver, update_receiver)
            .await
            .unwrap();
        let admin_addr = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";
        let public_url = "http://127.0.0.1:8080";
        let system_impl = SystemImpl::new(
            update_sender,
            system_store.clone(),
            SystemRole::DataRollupNode,
            public_url.to_string(),
            admin_addr,
        )
        .unwrap();
        let request = SetupRequest {
            signature: sig.to_string(),
            payload: payload.to_string(), //payload_binary,
        };
        let tonic_req = Request::new(request);
        if let Ok(response) = system_impl.setup(tonic_req).await {
            let r = response.into_inner();
            assert_eq!(0, r.code);
        } else {
            assert!(false);
        }
        sleep(TokioDuration::from_millis(10000)).await;
    }
}
