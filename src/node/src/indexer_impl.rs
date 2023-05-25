use crate::auth_storage::AuthStorage;
use crate::node_storage::NodeStorage;
use bytes::Bytes;
use chrono::Utc;
use db3_crypto::id::AccountId;
use db3_crypto::{db3_address::DB3Address as AccountAddress, db3_verifier, id::TxId};
use db3_error::DB3Error;
use db3_proto::db3_event_proto::event_message;
use db3_proto::db3_event_proto::EventMessage;
use db3_proto::db3_mutation_proto::{DatabaseAction, DatabaseMutation, PayloadType, WriteRequest};
use db3_sdk::store_sdk::StoreSDK;
use ethers::core::types::Bytes as EthersBytes;
use ethers::types::transaction::eip712::{Eip712, TypedData};
use prost::Message;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tendermint::block;
use tonic::Status;
use tracing::{debug, info, warn};

/// parse mutation
/// TODO duplicate with abci impl. will refactor later
macro_rules! parse_mutation {
    ($func:ident, $type:ident) => {
        fn $func(payload: &[u8]) -> Result<$type, DB3Error> {
            match $type::decode(payload) {
                Ok(dm) => match &dm.meta {
                    Some(_) => Ok(dm),
                    None => {
                        warn!("no meta for mutation");
                        Err(DB3Error::ApplyMutationError("meta is none".to_string()))
                    }
                },
                Err(e) => {
                    //TODO add event ?
                    warn!("invalid mutation data {e}");
                    Err(DB3Error::ApplyMutationError(
                        "invalid mutation data".to_string(),
                    ))
                }
            }
        }
    };
}
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
    parse_mutation!(parse_database_mutation, DatabaseMutation);

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
                        Self::apply_database_mutations(
                            store.get_auth_store(),
                            &mut pending_databases,
                        )
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
                Ok(req) => match Self::unwrap_and_verify(req) {
                    Ok((data, data_type, account_id)) => match data_type {
                        PayloadType::DatabasePayload => {
                            match Self::parse_database_mutation(data.as_ref()) {
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
        pending_databases: &mut Vec<(AccountAddress, DatabaseMutation, TxId)>,
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
    /// unwrap and verify write request
    /// TODO: duplicate with abci unwrap_and_verify, refactor it to a common/util function
    fn unwrap_and_verify(
        req: WriteRequest,
    ) -> Result<(EthersBytes, PayloadType, AccountId), DB3Error> {
        if req.payload_type == 3 {
            // typed data
            match serde_json::from_slice::<TypedData>(req.payload.as_ref()) {
                Ok(data) => {
                    let hashed_message = data.encode_eip712().map_err(|e| {
                        DB3Error::ApplyMutationError(format!("invalid payload type for err {e}"))
                    })?;
                    let account_id = db3_verifier::DB3Verifier::verify_hashed(
                        &hashed_message,
                        req.signature.as_ref(),
                    )?;
                    if let (Some(payload), Some(payload_type)) =
                        (data.message.get("payload"), data.message.get("payloadType"))
                    {
                        //TODO advoid data copy
                        let data: EthersBytes =
                            serde_json::from_value(payload.clone()).map_err(|e| {
                                DB3Error::ApplyMutationError(format!(
                                    "invalid payload type for err {e}"
                                ))
                            })?;
                        let internal_data_type = i32::from_str(payload_type.as_str().ok_or(
                            DB3Error::QuerySessionVerifyError("invalid payload type".to_string()),
                        )?)
                        .map_err(|e| {
                            DB3Error::QuerySessionVerifyError(format!(
                                "fail to convert payload type to i32 {e}"
                            ))
                        })?;
                        let data_type: PayloadType = PayloadType::from_i32(internal_data_type)
                            .ok_or(DB3Error::ApplyMutationError(
                                "invalid payload type".to_string(),
                            ))?;
                        Ok((data, data_type, account_id))
                    } else {
                        Err(DB3Error::ApplyMutationError("bad typed data".to_string()))
                    }
                }
                Err(e) => Err(DB3Error::ApplyMutationError(format!(
                    "bad typed data for err {e}"
                ))),
            }
        } else {
            let account_id =
                db3_verifier::DB3Verifier::verify(req.payload.as_ref(), req.signature.as_ref())?;
            let data_type: PayloadType = PayloadType::from_i32(req.payload_type).ok_or(
                DB3Error::ApplyMutationError("invalid payload type".to_string()),
            )?;
            let data = Bytes::from(req.payload);
            Ok((EthersBytes(data), data_type, account_id))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
