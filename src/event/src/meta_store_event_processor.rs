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
    mutation::body_wrapper::Body, mutation::BodyWrapper, MintDocumentDatabaseMutation, Mutation,
    MutationAction,
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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::task;
use tracing::{info, warn};

abigen!(DB3MetaStoreEvents, "abi/Events.json");

pub struct MetaStoreEventProcessor {
    block_number: Arc<AtomicU64>,
    event_number: Arc<AtomicU64>,
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
            block_number: Arc::new(AtomicU64::new(0)),
            event_number: Arc::new(AtomicU64::new(0)),
            state_store,
            last_running: ArcSwapOption::from(None),
            mutation_type,
            storage,
            db_store,
            system_store,
        }
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

        let typed_tx: TypedTransaction = t.into();
        let tx_bytes = typed_tx.rlp();
        let tx_hex_str = hex::encode(tx_bytes);
        let db3_addr = DB3Address::from(&event.database_address.0);
        let sender_addr = DB3Address::from(&event.sender.0);

        let signature = Signature {
            r: t.r,
            s: t.s,
            v: t.v.as_u64(),
        };

        let desc = String::from_utf8(event.desc.to_vec()).map_err(|e| {
            DB3Error::StoreEventError(format!("fail to decode description for error {e}"))
        })?;
        let desc = desc.trim_matches(char::from(0));
        let mint = MintDocumentDatabaseMutation {
            signature: format!("{signature}"),
            tx: tx_hex_str,
            db_addr: db3_addr.to_hex(),
            desc: desc.to_string(),
            sender: sender_addr.to_hex(),
        };
        let my_addr = DB3Address::from(&wallet.address().0);
        let nonce = state_store.get_nonce(&my_addr)? + 1;
        let mutation = Mutation {
            action: MutationAction::MintDocumentDb.into(),
            bodies: vec![BodyWrapper {
                db_address: vec![],
                body: Some(Body::MintDocDatabaseMutation(mint)),
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
        let signature = format!("{signature}");
        let payload = serde_json::to_vec(&typed_data).map_err(|_| {
            DB3Error::StoreEventError("fail to convert typed data to json".to_string())
        })?;

        let (_, block, order) = storage
            .generate_mutation_block_and_order(&payload, signature.as_str())
            .map_err(|e| DB3Error::StoreEventError(format!("fail to generate tx for {e}")))?;
        if let Ok(_) = db_store.apply_mutation(
            MutationAction::MintDocumentDb,
            mutation,
            &sender_addr,
            network,
            nonce,
            block,
            order,
            &HashMap::new(),
        ) {
            match storage.add_mutation(
                &payload,
                signature.as_str(),
                "",
                &sender_addr,
                nonce,
                block,
                order,
                network,
                MutationAction::MintDocumentDb,
            ) {
                Ok(_) => {
                    info!("mint database with address {} done", db3_addr.to_hex());
                }
                Err(e) => {
                    warn!(
                        "fail to mint database with address {} for {e}",
                        db3_addr.to_hex()
                    );
                }
            }
        }
        Ok(())
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
        let running = Arc::new(AtomicBool::new(true));
        info!(
            "start meta store event processor with evm node url {}",
            evm_node_url
        );
        self.last_running.store(Some(running.clone()));
        let provider = Provider::<Ws>::connect(evm_node_url)
            .await
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let provider = Arc::new(provider);
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
                    .events(vec!["CreateDatabase", "CreateCollection"])
                    .address(address)
            }
        };
        let local_contract_addr = contract_addr.to_string();
        task::spawn(async move {
            match provider.subscribe_logs(&filter).await {
                Ok(mut stream) => loop {
                    if !running.load(Ordering::Relaxed) {
                        info!(
                            "stop event processor for contract {}",
                            local_contract_addr.as_str()
                        );
                        break;
                    }
                    if let Some(log) = stream.next().await {
                        info!(
                            "block number {:?} transacion {:?} sender address {:?} ",
                            log.block_number, log.transaction_hash, log.address
                        );
                    }
                },
                Err(e) => {
                    warn!("fail get stream for error {e}");
                }
            }
        });
        Ok(())
    }
}
