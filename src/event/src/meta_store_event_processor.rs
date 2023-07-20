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
use db3_crypto::db3_address::DB3Address;
use db3_error::{DB3Error, Result};
use db3_storage::state_store::StateStore;
use ethers::types::Address;
use ethers::types::Filter;
use ethers::{
    contract::abigen,
    // core::types::{transaction::eip2718::TypedTransaction, Log, Signature, Transaction},
    providers::{Middleware, Provider, StreamExt, Ws},
};
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
}

unsafe impl Sync for MetaStoreEventProcessor {}
unsafe impl Send for MetaStoreEventProcessor {}

impl MetaStoreEventProcessor {
    pub fn new(state_store: Arc<StateStore>) -> Self {
        Self {
            block_number: Arc::new(AtomicU64::new(0)),
            event_number: Arc::new(AtomicU64::new(0)),
            state_store,
            last_running: ArcSwapOption::from(None),
        }
    }

    pub async fn start(
        &self,
        contract_addr: &str,
        evm_node_url: &str,
        start_block: u64,
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
