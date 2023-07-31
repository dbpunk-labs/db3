//
// meta_store_event_processor.rs
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

use arc_swap::ArcSwapOption;
use bytes::BytesMut;
use db3_crypto::db3_address::DB3Address;
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_v2_proto::{
    mutation::body_wrapper::Body, mutation::BodyWrapper, MintCollectionMutation,
    MintDocumentDatabaseMutation, Mutation, MutationAction,
};
use db3_storage::db_store_v2::DBStoreV2;
use db3_storage::mutation_store::MutationStore;
use db3_storage::state_store::StateStore;
use db3_storage::system_store::SystemStore;
use ethers::prelude::{LocalWallet, Signer};
use ethers::{
    abi::{RawLog, Token::Bytes},
    contract::{abigen, EthEvent},
    core::types::{
        transaction::eip2718::TypedTransaction,
        transaction::eip712::{EIP712Domain, TypedData, Types},
        Address, Filter, Log, Signature, Transaction,
    },
    providers::{Middleware, Provider, StreamExt, Ws},
};

use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::task;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

abigen!(DB3MetaStoreEvents, "abi/Events.json");

pub struct MetaStoreEventProcessor {
    state_store: Arc<StateStore>,
    last_running: ArcSwapOption<AtomicBool>,
    mutation_type: Types,
    storage: MutationStore,
    db_store: DBStoreV2,
    system_store: Arc<SystemStore>,
}

unsafe impl Sync for MetaStoreEventProcessor {}
unsafe impl Send for MetaStoreEventProcessor {}

impl MetaStoreEventProcessor {
    pub fn new(
        state_store: Arc<StateStore>,
        db_store: DBStoreV2,
        storage: MutationStore,
        system_store: Arc<SystemStore>,
    ) -> Self {
        let mutation_type = serde_json::json!({
          "EIP712Domain": [
          ],
          "Message":[
            {"name":"payload", "type":"bytes"},
            {"name":"nonce", "type":"string"},
          ]
        });
        let mutation_type: Types = serde_json::from_value(mutation_type).unwrap();
        Self {
            state_store,
            last_running: ArcSwapOption::from(None),
            mutation_type,
            storage,
            db_store,
            system_store,
        }
    }

    async fn handle_create_collection(
        log: &Log,
        t: &Transaction,
        network: u64,
        wallet: &LocalWallet,
        state_store: &Arc<StateStore>,
        storage: &MutationStore,
        db_store: &DBStoreV2,
        mutation_type: &Types,
    ) -> Result<()> {
        let row_log = RawLog {
            topics: log.topics.clone(),
            data: log.data.to_vec(),
        };
        let event = CreateCollectionFilter::decode_log(&row_log).map_err(|e| {
            DB3Error::StoreEventError(format!("fail to decode the create database event {e}"))
        })?;
        // jude the network
        if event.network_id.as_u64() != network {
            warn!("ignore the mismatch network event");
            return Ok(());
        }

        let typed_tx: TypedTransaction = t.into();
        let tx_bytes = typed_tx.rlp();
        let tx_hex_str = hex::encode(tx_bytes);
        let db3_addr = DB3Address::from(&event.db.0);
        let sender_addr = DB3Address::from(&event.sender.0);
        let name = String::from_utf8(event.name.to_vec()).map_err(|e| {
            DB3Error::StoreEventError(format!("fail to decode collection name for error {e}"))
        })?;
        let name = name.trim_matches(char::from(0));
        let signature = Signature {
            r: t.r,
            s: t.s,
            v: t.v.as_u64(),
        };
        let mint = MintCollectionMutation {
            signature: format!("{signature}"),
            tx: tx_hex_str,
            db_addr: db3_addr.to_hex(),
            name: name.to_string(),
            sender: sender_addr.to_hex(),
        };
        let body = Body::MintCollectionMutation(mint);
        let (signature, mutation, nonce, payload) = Self::sign_mutation(
            wallet,
            state_store,
            body,
            MutationAction::MintCollection,
            mutation_type,
        )
        .await?;
        Self::apply_mutation(
            signature.as_str(),
            &payload,
            MutationAction::MintCollection,
            mutation,
            storage,
            db_store,
            nonce,
            network,
            &sender_addr,
        )
    }

    fn apply_mutation(
        signature: &str,
        payload: &[u8],
        action: MutationAction,
        mutation: Mutation,
        storage: &MutationStore,
        db_store: &DBStoreV2,
        nonce: u64,
        network: u64,
        sender_addr: &DB3Address,
    ) -> Result<()> {
        let (_, block, order) = storage
            .generate_mutation_block_and_order(payload, signature)
            .map_err(|e| DB3Error::StoreEventError(format!("fail to generate tx for {e}")))?;
        if let Ok(_) = db_store.apply_mutation(
            action,
            mutation,
            sender_addr,
            network,
            nonce,
            block,
            order,
            &HashMap::new(),
        ) {
            match storage.add_mutation(
                &payload,
                signature,
                "",
                sender_addr,
                nonce,
                block,
                order,
                network,
                action,
            ) {
                Ok(_) => {
                    info!("mint event with from sender {} done", sender_addr.to_hex());
                }
                Err(e) => {
                    warn!(
                        "fail to mint evemt with from address {} for {e}",
                        sender_addr.to_hex()
                    );
                }
            }
        }
        Ok(())
    }
    async fn sign_mutation(
        wallet: &LocalWallet,
        state_store: &Arc<StateStore>,
        body: Body,
        action: MutationAction,
        mutation_type: &Types,
    ) -> Result<(String, Mutation, u64, Vec<u8>)> {
        let my_addr = DB3Address::from(&wallet.address().0);
        let nonce = state_store.get_nonce(&my_addr)? + 1;
        let mutation = Mutation {
            action: action.into(),
            bodies: vec![BodyWrapper {
                db_address: vec![],
                body: Some(body),
            }],
        };
        let mut mbuf = BytesMut::with_capacity(1024 * 4);
        mutation.encode(&mut mbuf).map_err(|e| {
            DB3Error::StoreEventError(format!("fail to encode mint mutation for error {e}"))
        })?;
        let mbuf = Bytes(mbuf.freeze().to_vec());
        let mut message: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        message.insert(
            "payload".to_string(),
            serde_json::Value::from(format!("{mbuf}")),
        );
        message.insert(
            "nonce".to_string(),
            serde_json::Value::from(nonce.to_string()),
        );
        let typed_data = TypedData {
            domain: EIP712Domain {
                name: None,
                version: None,
                chain_id: None,
                verifying_contract: None,
                salt: None,
            },
            types: mutation_type.clone(),
            primary_type: "Message".to_string(),
            message,
        };

        // let the node to sign the message
        let signature = wallet.sign_typed_data(&typed_data).await.map_err(|e| {
            DB3Error::StoreEventError(format!("fail to sign mint event request {e}"))
        })?;
        let payload = serde_json::to_vec(&typed_data).map_err(|_| {
            DB3Error::StoreEventError("fail to convert typed data to json".to_string())
        })?;
        Ok((format!("{signature}"), mutation, nonce, payload))
    }

    async fn handle_create_doc_database(
        log: &Log,
        t: &Transaction,
        network: u64,
        wallet: &LocalWallet,
        state_store: &Arc<StateStore>,
        storage: &MutationStore,
        db_store: &DBStoreV2,
        mutation_type: &Types,
    ) -> Result<()> {
        let row_log = RawLog {
            topics: log.topics.clone(),
            data: log.data.to_vec(),
        };
        let event = CreateDatabaseFilter::decode_log(&row_log).map_err(|e| {
            DB3Error::StoreEventError(format!("fail to decode the create database event {e}"))
        })?;
        // jude the network
        if event.network_id.as_u64() != network {
            warn!(
                "ignore the mismatch network event received network {} but expect {}",
                event.network_id.as_u64(),
                network
            );
            return Ok(());
        }
        let typed_tx: TypedTransaction = t.into();
        let tx_bytes = typed_tx.rlp();
        let tx_hex_str = hex::encode(tx_bytes);
        let signature = Signature {
            r: t.r,
            s: t.s,
            v: t.v.as_u64(),
        };

        let desc = String::from_utf8(event.desc.to_vec()).map_err(|e| {
            DB3Error::StoreEventError(format!("fail to decode description for error {e}"))
        })?;
        let desc = desc.trim_matches(char::from(0));
        let db3_addr = DB3Address::from(&event.database_address.0);
        let sender_addr = DB3Address::from(&event.sender.0);
        let mint = MintDocumentDatabaseMutation {
            signature: format!("{signature}"),
            tx: tx_hex_str,
            db_addr: db3_addr.to_hex(),
            desc: desc.to_string(),
            sender: sender_addr.to_hex(),
        };
        let body = Body::MintDocDatabaseMutation(mint);
        let (signature, mutation, nonce, payload) = Self::sign_mutation(
            wallet,
            state_store,
            body,
            MutationAction::MintDocumentDb,
            mutation_type,
        )
        .await?;
        Self::apply_mutation(
            signature.as_str(),
            &payload,
            MutationAction::MintDocumentDb,
            mutation,
            storage,
            db_store,
            nonce,
            network,
            &sender_addr,
        )
    }

    pub async fn start(
        &self,
        contract_addr: &str,
        evm_node_url: &str,
        start_block: u64,
        chain_id: u32,
        network: u64,
    ) -> Result<()> {
        if let Some(ref last_running) = self.last_running.load_full() {
            // stop last job
            last_running.store(false, Ordering::Relaxed);
        }
        let wallet = self.system_store.get_evm_wallet(chain_id)?;
        let local_state_store = self.state_store.clone();
        let local_storage = self.storage.clone();
        let local_db_store = self.db_store.clone();
        let mutation_type = self.mutation_type.clone();
        let running = Arc::new(AtomicBool::new(true));
        info!(
            "start meta store event processor with evm node url {}",
            evm_node_url
        );
        self.last_running.store(Some(running.clone()));
        let address = contract_addr
            .parse::<Address>()
            .map_err(|_| DB3Error::InvalidAddress)?;
        let db3_address = DB3Address::from(&address.0);
        let progress = self.state_store.get_event_progress(&db3_address)?;
        let filter = match (progress, start_block == 0) {
            (Some(block), _) => {
                info!(
                    "start process contract from block {} with address {}",
                    block, contract_addr
                );
                Filter::new()
                    .from_block(block)
                    .events(vec!["CreateDatabase", "CreateCollection"])
                    .address(address)
            }
            (None, false) => {
                info!(
                    "start process contract from with config block {} with address {}",
                    start_block, contract_addr
                );
                Filter::new()
                    .from_block(start_block)
                    .events(vec!["CreateDatabase", "CreateCollection"])
                    .address(address)
            }
            (None, true) => {
                info!(
                    "start process contract from with current block with address {}",
                    contract_addr
                );
                Filter::new()
                    .events(vec![
                        CreateDatabaseFilter::abi_signature().as_bytes(),
                        CreateCollectionFilter::abi_signature().as_bytes(),
                    ])
                    .address(address)
            }
        };
        let local_contract_addr = contract_addr.to_string();
        let local_evm_node_url = evm_node_url.to_string();
        task::spawn(async move {
            loop {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
                if let Ok(provider) =
                    Provider::<Ws>::connect_with_reconnects(local_evm_node_url.as_str(), 100).await
                {
                    let provider = Arc::new(provider);
                    match provider.subscribe_logs(&filter).await {
                        Ok(mut stream) => loop {
                            if !running.load(Ordering::Relaxed) {
                                info!(
                                    "stop event processor for contract {}",
                                    local_contract_addr.as_str()
                                );
                                break;
                            }
                            match stream.next().await {
                                Some(log) => {
                                    info!(
                                        "block number {:?} transacion {:?} sender address {:?} ",
                                        log.block_number, log.transaction_hash, log.address
                                    );
                                    if let (Ok(Some(transaction)), Ok(event_signature)) = (
                                        provider
                                            .get_transaction(log.transaction_hash.unwrap())
                                            .await
                                            .map_err(|e| {
                                                DB3Error::StoreEventError(format!(
                                                    "fail to get transaction {e}"
                                                ))
                                            }),
                                        log.topics.get(0).ok_or(DB3Error::StoreEventError(
                                            format!("fail to get topics"),
                                        )),
                                    ) {
                                        if event_signature == &(CreateDatabaseFilter::signature()) {
                                            if let Ok(_) = Self::handle_create_doc_database(
                                                &log,
                                                &transaction,
                                                network,
                                                &wallet,
                                                &local_state_store,
                                                &local_storage,
                                                &local_db_store,
                                                &mutation_type,
                                            )
                                            .await
                                            {}
                                        } else if event_signature
                                            == &(CreateCollectionFilter::signature())
                                        {
                                            if let Ok(_) = Self::handle_create_collection(
                                                &log,
                                                &transaction,
                                                network,
                                                &wallet,
                                                &local_state_store,
                                                &local_storage,
                                                &local_db_store,
                                                &mutation_type,
                                            )
                                            .await
                                            {}
                                        }
                                    }
                                }
                                None => {
                                    warn!("empty log from stream, sleep 5 seconds and reconnect to it");
                                    sleep(Duration::from_millis(5 * 1000)).await;
                                    break;
                                }
                            }
                        },
                        Err(e) => {
                            warn!("fail get stream for error {e}");
                        }
                    }
                    sleep(Duration::from_millis(5 * 1000)).await;
                }
            }
            warn!("the meta contract event listener exits");
        });
        Ok(())
    }
}
