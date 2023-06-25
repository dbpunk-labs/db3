//
// event_processor.rs
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

use db3_crypto::db3_address::DB3Address;
use db3_error::{DB3Error, Result};
use db3_storage::db_store_v2::DBStoreV2;
use ethabi::{Log as EthLog, Token};
use ethers::abi::RawLog;
use ethers::types::Address;
use ethers::types::Filter;
use ethers::{
    core::abi::Abi,
    providers::{Middleware, Provider, StreamExt, Ws},
};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Debug)]
pub struct EventProcessorConfig {
    pub evm_node_url: String,
    pub db_addr: String,
    pub abi: String,
    pub target_events: HashSet<String>,
    pub contract_addr: String,
}

pub struct EventProcessor {
    config: EventProcessorConfig,
    provider: Arc<Provider<Ws>>,
    running: Arc<AtomicBool>,
    db_store: DBStoreV2,
    block_number: Arc<AtomicU64>,
    event_number: Arc<AtomicU64>,
}

unsafe impl Sync for EventProcessor {}
unsafe impl Send for EventProcessor {}
impl EventProcessor {
    pub async fn new(config: EventProcessorConfig, db_store: DBStoreV2) -> Result<Self> {
        info!("new event processor with config {:?}", config);
        let provider = Provider::<Ws>::connect(&config.evm_node_url)
            .await
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let provider_arc = Arc::new(provider);
        Ok(Self {
            config,
            provider: provider_arc,
            running: Arc::new(AtomicBool::new(false)),
            db_store,
            block_number: Arc::new(AtomicU64::new(0)),
            event_number: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn close(&self) {
        self.running.store(false, Ordering::Relaxed);
        info!(
            "stop the event processor for db {}",
            self.config.db_addr.as_str()
        );
    }

    pub fn get_config<'a>(&'a self) -> &'a EventProcessorConfig {
        &self.config
    }

    pub fn get_block_number(&self) -> u64 {
        self.block_number.load(Ordering::Relaxed)
    }

    pub fn get_event_number(&self) -> u64 {
        self.event_number.load(Ordering::Relaxed)
    }

    pub async fn start(&self) -> Result<()> {
        let abi: Abi = serde_json::from_str(self.config.abi.as_str())
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        self.running
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let address = self
            .config
            .contract_addr
            .parse::<Address>()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let db_addr = DB3Address::from_hex(self.config.db_addr.as_str())
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let filter = Filter::new().address(address);
        let mut stream = self
            .provider
            .subscribe_logs(&filter)
            .await
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        info!(
            "event processor for contract {}",
            self.config.contract_addr.as_str()
        );
        while let Some(log) = stream.next().await {
            if !self.running.load(Ordering::Relaxed) {
                info!(
                    "stop event processor for contract {}",
                    self.config.contract_addr.as_str()
                );
                break;
            }
            if let Some(number) = log.block_number {
                if number.as_u64() % 10 == 0 {
                    info!(
                        "contract {} sync status block {} event number {}",
                        self.config.contract_addr.as_str(),
                        self.block_number.load(Ordering::Relaxed),
                        self.event_number.load(Ordering::Relaxed)
                    );
                }
                self.block_number.store(number.as_u64(), Ordering::Relaxed)
            }
            for e in abi.events() {
                // verify
                let event_signature = log
                    .topics
                    .get(0)
                    .ok_or(DB3Error::StoreEventError(format!("")))?;

                if event_signature != &e.signature() {
                    continue;
                }
                if !self.config.target_events.contains(e.name.as_str()) {
                    continue;
                }
                let raw_log = RawLog {
                    topics: log.topics.clone(),
                    data: log.data.to_vec(),
                };
                if let Ok(log_entry) = e.parse_log(raw_log) {
                    let json_value = Self::log_to_doc(&log_entry);
                    match serde_json::to_string(&json_value) {
                        Ok(value) => {
                            let values = vec![value.to_string()];
                            if let Err(e) = self.db_store.add_docs(
                                &db_addr,
                                &DB3Address::ZERO,
                                e.name.as_str(),
                                &values,
                            ) {
                                warn!("fail to write json doc {} for {e}", value.as_str());
                            } else {
                                self.event_number.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            warn!("fail to convert to json for {e} ");
                        }
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    fn log_to_doc(log: &EthLog) -> serde_json::Value {
        let mut doc = serde_json::Map::new();
        for log_param in &log.params {
            doc.insert(
                log_param.name.to_string(),
                Self::param_to_value(&log_param.value),
            );
        }
        serde_json::Value::Object(doc)
    }

    fn param_to_value(param: &Token) -> serde_json::Value {
        match param {
            Token::Address(addr) => {
                serde_json::value::Value::String(format!("0x{}", hex::encode(addr.as_bytes())))
            }
            Token::String(value) => serde_json::value::Value::String(value.to_string()),
            Token::Uint(value) | Token::Int(value) => {
                serde_json::value::Value::String(value.to_string())
            }
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_event_processor() {
        // let contract_abi: &str = r#"[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"deposit","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"},{"name":"","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"guy","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Deposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Withdrawal","type":"event"}]"#;
        // let config = EventProcessorConfig {
        //     evm_node_url: "wss://polygon-mainnet.g.alchemy.com/v2/EH9ZSJ0gS7a1DEIohAWMbhP33lK6qHj9"
        //         .to_string(),
        //     db_addr: "0xaaaaa".to_string(),
        //     abi: contract_abi.to_string(),
        //     target_events: Has,
        //     contract_addr: "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270".to_string(),
        // };
    }
}
