//
// evm_chain_watcher.rs
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

use db3_error::{DB3Error, Result};
use db3_proto::db3_message_proto::DepositEvent;
use db3_storage::event_store::EventStore;
use ethers::abi::RawLog;
use ethers::{
    contract::{abigen, Contract},
    core::types::ValueOrArray,
    providers::{Provider, StreamExt, Ws},
};
use redb::Database;
use std::path::Path;
use std::sync::Arc;
abigen!(
    DB3RollupContract,
    "bridge/artifacts/contracts/DB3Rollup.sol/DB3Rollup.json"
);

pub struct EvmChainConfig {
    pub chain_id: u32,
    pub db_path: String,
    pub node_list: Vec<String>,
    // a hex string
    pub contract_address: String,
}

pub struct EvmChainWatcher {
    config: EvmChainConfig,
    db: Arc<Database>,
    provider: Arc<Provider>,
}

impl EvmChainWatcher {
    pub async fn new(config: EvmChainConfig) -> Result<EvmChainWatcher> {
        let provider = Provider::<Ws>::connect(&config.node_list[0]).await?;
        let provider_arc = Arc::new(provider);
        let path = Path::new(&config.db_path);
        let db = Arc::new(
            Database::create(&path).map_err(|e| DB3Error::StoreEventError(format!("{e}")))?,
        );
        Ok(EvmChainWatcher {
            config,
            db,
            provider: provider_arc,
        })
    }

    pub async fn start(&self) -> Result<()> {
        let address = self
            .config
            .contract_address
            .parse::<Address>()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let db3_deposit_filter = Filter::new()
            .address(address)
            .event(&DepositFilter::abi_signature());

        //TODO recover to last block number processed
        let mut stream = self
            .provider
            .clone()
            .subscribe_logs(&db3_deposit_filter)
            .await?
            .take(10);
        while let Some(log) = stream.next().await {
            let row_log = RawLog {
                topics: log.topics.clone(),
                data: log.data.to_vec(),
            };
            let event = DepositFilter::decode_log(&row_log)
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            let transacion = provider_arc
                .get_transaction(log.transaction_hash)
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            if let Some(t) = transacion {
                let deposit_event = DepositEvent {
                    chain_id: self.config.chain_id,
                    sender: t.from.as_ref().to_vec(),
                    amount: event.amount.as_u64(),
                    block_id: log.block_number,
                    transaction_id: log.transaction_hash.as_ref().to_vec(),
                    v: t.v.to_vec(),
                    r: t.r.to_vec(),
                    s: t.s.to_vec(),
                };
                let tx = self
                    .db
                    .clone()
                    .begin_write()
                    .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
                EventStore::store_deposit_event(tx, &deposit_event)?;
            } else {
                Err(DB3Error::StoreEventError(
                    "fail to get transaction".to_string(),
                ));
            }
        }
    }
}
