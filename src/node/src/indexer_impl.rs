use crate::auth_storage::AuthStorage;
use crate::mutation_utils::MutationUtil;
use crate::node_storage::NodeStorage;
use chrono::Utc;
use db3_crypto::{db3_address::DB3Address as AccountAddress, id::TxId};
use db3_proto::db3_event_proto::event_message;
use db3_proto::db3_event_proto::EventMessage;
use db3_proto::db3_mutation_proto::{DatabaseAction, DatabaseMutation, PayloadType, WriteRequest};
use db3_sdk::store_sdk::StoreSDK;
use prost::Message;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tendermint::block;
use tonic::Status;
use tracing::{debug, info, warn};

pub struct IndexerImpl {
    store_sdk: StoreSDK,
    node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>,
}

impl IndexerImpl {
    pub fn new(store_sdk: StoreSDK, node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>) -> Self {
        Self {
            store_sdk,
            node_store,
        }
    }

    /// start standalone indexer
    /// 1. subscribe db3 event
    /// 2. handle event
    pub async fn start(&mut self) -> std::result::Result<(), Status> {
        info!("[Indexer] start indexer ...");
        info!("[Indexer] subscribe event from db3 network");
        let mut stream = self
            .store_sdk
            .subscribe_event_message(true)
            .await?
            .into_inner();
        info!("listen and handle event message");
        while let Some(event) = stream.message().await? {
            match self.handle_event(event).await {
                Err(e) => {
                    info!("[Indexer] handle event error: {:?}", e);
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
                    "[Indexer] Receive BlockEvent: Block\t{}\t0x{}\t0x{}\t{}",
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
                        let mut pending_databases: Vec<(AccountAddress, DatabaseMutation, TxId)> =
                            vec![];
                        Self::parse_and_pending_mutations(&block.data, &mut pending_databases)
                            .await?;
                        Self::apply_database_mutations(store.get_auth_store(), &pending_databases)
                            .await?;
                        store.get_auth_store().commit().map_err(|e| {
                            Status::internal(format!("fail to commit database for {e}"))
                        })?;
                        // TODO: add show indexer state api
                        info!(
                            "[Indexer] last block state: {:?}",
                            store.get_auth_store().get_last_block_state()
                        );
                        info!(
                            "[Indexer] indexer state: {:?}",
                            store.get_auth_store().get_state()
                        );
                    }
                    Err(e) => {
                        warn!("[Indexer] get node store error: {:?}", e);
                        return Err(Status::internal(format!("get node store error: {:?}", e)));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
    async fn parse_and_pending_mutations(
        txs: &Vec<Vec<u8>>,
        database_mutations: &mut Vec<(AccountAddress, DatabaseMutation, TxId)>,
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
                            debug!("[Indexer] Skip QuerySessionPayload");
                        }
                        PayloadType::MintCreditsPayload => {
                            debug!("[Indexer] Skip MintCreditsPayload");
                        }
                        _ => {
                            debug!("[Indexer] Skip other payload type");
                        }
                    },
                    Err(e) => {
                        warn!("[Indexer] invalid write request: {:?}", e);
                        return Err(Status::internal(format!("invalid write request: {:?}", e)));
                    }
                },
                Err(e) => {
                    warn!("[Indexer] invalid write request: {:?}", e);
                    return Err(Status::internal(format!("invalid write request: {:?}", e)));
                }
            }
        }
        Ok(())
    }
    async fn apply_database_mutations(
        auth_store: &mut AuthStorage,
        pending_databases: &Vec<(AccountAddress, DatabaseMutation, TxId)>,
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

#[cfg(test)]
mod tests {}
