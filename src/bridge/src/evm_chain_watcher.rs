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
use ethers::types::Address;
use ethers::types::Filter;
use ethers::{
    contract::{abigen, EthEvent},
    core::types::{transaction::eip2718::TypedTransaction, Log, Signature, Transaction},
    providers::{Middleware, Provider, StreamExt, Ws},
};

use redb::Database;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use tracing::{info, warn};
abigen!(
    DB3RollupContract,
    "bridge/artifacts/contracts/DB3Rollup.sol/DB3Rollup.json"
);

#[derive(Debug)]
pub struct EvmChainConfig {
    pub chain_id: u32,
    pub node_list: Vec<String>,
    // a hex string
    pub contract_address: String,
}

pub struct EvmChainWatcher {
    config: EvmChainConfig,
    db: Arc<Database>,
    provider: Arc<Provider<Ws>>,
    running: AtomicBool,
}

impl EvmChainWatcher {
    pub async fn new(config: EvmChainConfig, db: Arc<Database>) -> Result<EvmChainWatcher> {
        info!("new evem chain watcher with config {:?}", config);
        let provider = Provider::<Ws>::connect(&config.node_list[0])
            .await
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let provider_arc = Arc::new(provider);
        Ok(EvmChainWatcher {
            config,
            db,
            provider: provider_arc,
            running: AtomicBool::new(true),
        })
    }

    fn process_event(
        &self,
        log: &Log,
        t: &Transaction,
        sender: &SyncSender<(u32, u64)>,
    ) -> Result<()> {
        let row_log = RawLog {
            topics: log.topics.clone(),
            data: log.data.to_vec(),
        };
        let event = DepositFilter::decode_log(&row_log)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        if let Some(id) = t.chain_id {
            if id.as_u32() != self.config.chain_id {
                warn!(
                    "chain_id mismatch expect {} but {}",
                    self.config.chain_id,
                    id.as_u32()
                );
                return Err(DB3Error::StoreEventError("chain id mismatch".to_string()));
            }
        } else {
            warn!("chain_id is required");
            return Err(DB3Error::StoreEventError(
                "chain id is required but none".to_string(),
            ));
        }
        let signature = Signature {
            r: t.r,
            s: t.s,
            v: t.v.as_u64(),
        };
        let typed_tx: TypedTransaction = t.into();
        let deposit_event = DepositEvent {
            chain_id: self.config.chain_id,
            sender: t.from.as_ref().to_vec(),
            amount: event.amount.as_u64(),
            block_id: log.block_number.unwrap().as_u64(),
            transaction_id: log.transaction_hash.as_ref().unwrap().0.to_vec(),
            signature: signature.to_vec(),
            tx_signed_hash: typed_tx.sighash().0.to_vec(),
        };
        let tx = self
            .db
            .begin_write()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        match EventStore::store_deposit_event(tx, &deposit_event) {
            Ok(_) => {
                info!("store event for block number {:?} transacion {:?} sender address {:?} amount {:?} done",
                  log.block_number, log.transaction_hash, log.address, event.amount.as_u64()
                  );
                sender.send((self.config.chain_id, log.block_number.unwrap().as_u64())).map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            }
            Err(e) => warn!("store event for block number {:?} transacion {:?} sender address {:?} amount {:?} with error {e}",
                  log.block_number, log.transaction_hash, log.address, event.amount.as_u64()
                  ),
        }
        Ok(())
    }

    pub async fn start(&self, sender: SyncSender<(u32, u64)>) -> Result<()> {
        info!("watcher is started");
        self.running
            .store(true, std::sync::atomic::Ordering::Relaxed);
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
            .subscribe_logs(&db3_deposit_filter)
            .await
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?
            .take(10);

        //
        // get the related transaction
        // validate the transaction by checking the signature and the chain id
        // check the transaction whether it has been processed by the bridge
        //
        while let Some(log) = stream.next().await {
            let transacion = self
                .provider
                .clone()
                .get_transaction(log.transaction_hash.unwrap())
                .await
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            if let Some(t) = transacion {
                self.process_event(&log, &t, &sender)?;
            } else {
                return Err(DB3Error::StoreEventError(
                    "fail to get transaction".to_string(),
                ));
            }
        }
        warn!("watcher is exited");
        Ok(())
    }
}
