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
use db3_crypto::db3_verifier::DB3Verifier;
use db3_crypto::id::TxId;
use db3_error::Result;
use db3_proto::db3_mutation_v2_proto::{
    mutation::body_wrapper::Body, MutationAction, MutationRollupStatus,
};
use db3_proto::db3_storage_proto::block_response;
use db3_proto::db3_storage_proto::event_message::Event as EventV2;
use db3_proto::db3_storage_proto::{
    storage_node_server::StorageNode, BlockRequest, BlockResponse, ExtraItem,
    GetCollectionOfDatabaseRequest, GetCollectionOfDatabaseResponse, GetDatabaseOfOwnerRequest,
    GetDatabaseOfOwnerResponse, GetMutationBodyRequest, GetMutationBodyResponse,
    GetMutationHeaderRequest, GetMutationHeaderResponse, GetNonceRequest, GetNonceResponse,
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
use db3_storage::state_store::{StateStore, StateStoreConfig};
use prost::Message;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender as BroadcastSender;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;
use tokio::time::{sleep, Duration as TokioDuration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, warn};
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
    sender: Sender<(
        DB3Address,
        SubscriptionV2,
        Sender<std::result::Result<EventMessageV2, Status>>,
    )>,
    broadcast_sender: BroadcastSender<EventMessageV2>,
}

impl StorageNodeV2Impl {
    pub fn new(
        config: StorageNodeV2Config,
        sender: Sender<(
            DB3Address,
            SubscriptionV2,
            Sender<std::result::Result<EventMessageV2, Status>>,
        )>,
    ) -> Result<Self> {
        let storage = MutationStore::new(config.store_config.clone())?;
        storage.recover()?;
        let state_store = StateStore::new(config.state_config.clone())?;
        let db_store = DBStoreV2::new(config.db_store_config.clone())?;
        let (broadcast_sender, _) = broadcast::channel(1024);
        Ok(Self {
            storage,
            state_store,
            config,
            running: Arc::new(AtomicBool::new(true)),
            db_store,
            sender,
            broadcast_sender,
        })
    }

    pub async fn start_to_produce_block(&self) {
        let local_running = self.running.clone();
        let local_storage = self.storage.clone();
        let local_block_interval = self.config.block_interval;
        let local_event_sender = self.broadcast_sender.clone();
        task::spawn(async move {
            info!("start the block producer thread");
            while local_running.load(Ordering::Relaxed) {
                sleep(TokioDuration::from_millis(local_block_interval)).await;
                info!(
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
                                info!("broadcast block event {}, {}", block_id, mutation_count);
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

    pub async fn start_to_rollup(&self) {
        let local_running = self.running.clone();
        let local_storage = self.storage.clone();
        let rollup_config = self.config.rollup_config.clone();
        let network_id = self.config.network_id;
        task::spawn(async move {
            info!("start the rollup thread");
            let rollup_interval = rollup_config.rollup_interval;
            //TODO handle err
            let executor = RollupExecutor::new(rollup_config, local_storage, network_id).unwrap();
            while local_running.load(Ordering::Relaxed) {
                sleep(TokioDuration::from_millis(rollup_interval)).await;
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
    ) -> std::result::Result<(), Status> {
        info!("start to keep subscription");
        let local_running = self.running.clone();
        let local_broadcast_sender = self.broadcast_sender.clone();

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
                         Some((addr, sub, sender)) = receiver.recv() => {
                            info!("add or update the subscriber with addr 0x{}", hex::encode(addr.as_ref()));
                            //TODO limit the max address count
                            subscribers.insert(addr, (sender, sub));
                            info!("subscribers len : {}", subscribers.len());
                        }
                        Ok(event) = event_sub.recv() => {
                            info!("receive event {:?}", event);
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
                                            info!("send event to addr 0x{}", hex::encode(key.as_ref()));
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
        info!("receive subscribe request");
        let r = request.into_inner();
        let sender = self.sender.clone();
        info!("sender is close: {}", sender.is_closed());
        let account_id = DB3Verifier::verify(r.payload.as_ref(), r.signature.as_ref())
            .map_err(|e| Status::internal(format!("bad signature for {e}")))?;
        let payload = SubscriptionV2::decode(r.payload.as_ref()).map_err(|e| {
            Status::internal(format!("fail to decode open session request for {e} "))
        })?;
        info!(
            "add subscriber for addr 0x{}",
            hex::encode(account_id.addr.as_ref())
        );
        info!("payload {:?}", payload);
        info!("sender {:?}", sender);
        let (msg_sender, msg_receiver) =
            tokio::sync::mpsc::channel::<std::result::Result<EventMessageV2, Status>>(10);
        sender
            .try_send((account_id.addr, payload, msg_sender))
            .map_err(|e| Status::internal(format!("fail to add subscriber for {e}")))?;
        Ok(Response::new(ReceiverStream::new(msg_receiver)))
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

    async fn get_collection_of_database(
        &self,
        request: Request<GetCollectionOfDatabaseRequest>,
    ) -> std::result::Result<Response<GetCollectionOfDatabaseResponse>, Status> {
        let r = request.into_inner();
        let addr = DB3Address::from_hex(r.db_addr.as_str())
            .map_err(|e| Status::internal(format!("{e}")))?;
        let collections = self
            .db_store
            .get_collection_of_database(&addr)
            .map_err(|e| Status::internal(format!("{e}")))?;
        info!(
            "query collection count {} with database {}",
            collections.len(),
            r.db_addr.as_str()
        );
        Ok(Response::new(GetCollectionOfDatabaseResponse {
            collections,
        }))
    }
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
        info!(
            "query database list count {} with account {}",
            databases.len(),
            r.owner.as_str()
        );
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
            .map_err(|e| Status::internal(format!("{e}")))?;
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
                    .add_mutation(&r.payload, r.signature.as_str(), &address, nonce)
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
                                let db_id_hex = db_id.to_hex();
                                info!(
                                    "add database with addr {} from owner {}",
                                    db_id_hex.as_str(),
                                    address.to_hex().as_str()
                                );
                                let item = ExtraItem {
                                    key: "db_addr".to_string(),
                                    value: db_id_hex,
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
                                info!(
                                    "add collection with db_addr {}, collection_name: {}, from owner {}",
                                    db_addr.to_hex().as_str(),
                                    col_mutation.collection_name.as_str(),
                                    address.to_hex().as_str()
                                );
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
