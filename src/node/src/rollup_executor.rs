//
// rollup_executor.rs
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

use crate::ar_toolbox::ArToolBox;
use arc_swap::ArcSwap;
use db3_base::times;
use db3_error::{DB3Error, Result};
use db3_proto::db3_rollup_proto::{GcRecord, RollupRecord};
use db3_storage::key_store::{KeyStore, KeyStoreConfig};
use db3_storage::meta_store_client::MetaStoreClient;
use db3_storage::mutation_store::MutationStore;
use ethers::prelude::{LocalWallet, Signer};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

#[derive(Clone)]
pub struct RollupExecutorConfig {
    // the interval in ms
    pub rollup_interval: u64,
    pub temp_data_path: String,
    pub ar_node_url: String,
    pub min_rollup_size: u64,
    pub min_gc_round_offset: u64,
    pub key_root_path: String,
    pub evm_node_url: String,
    pub contract_addr: String,
}
pub struct RollupExecutor {
    config: RollupExecutorConfig,
    storage: MutationStore,
    ar_toolbox: ArToolBox,
    wallet: LocalWallet,
    min_rollup_size: Arc<AtomicU64>,
    meta_store: Arc<MetaStoreClient>,
    pending_mutations: Arc<AtomicU64>,
    pending_data_size: Arc<AtomicU64>,
    pending_start_block: Arc<AtomicU64>,
    pending_end_block: Arc<AtomicU64>,
    network_id: Arc<AtomicU64>,
}

unsafe impl Sync for RollupExecutor {}
unsafe impl Send for RollupExecutor {}

impl RollupExecutor {
    pub async fn new(
        config: RollupExecutorConfig,
        storage: MutationStore,
        network_id: Arc<AtomicU64>,
    ) -> Result<Self> {
        let wallet = Self::build_wallet(config.key_root_path.as_str())?;
        info!(
            "evm address {}",
            format!("0x{}", hex::encode(wallet.address().as_bytes()))
        );
        let wallet2 = Self::build_wallet(config.key_root_path.as_str())?;
        //TODO config the chain id
        let wallet2 = wallet2.with_chain_id(80001_u32);
        let min_rollup_size = config.min_rollup_size;
        let meta_store = Arc::new(
            MetaStoreClient::new(
                config.contract_addr.as_str(),
                config.evm_node_url.as_str(),
                wallet2,
            )
            .await?,
        );
        let ar_toolbox = ArToolBox::new(
            config.key_root_path.clone(),
            config.ar_node_url.clone(),
            config.temp_data_path.clone(),
        )?;
        Ok(Self {
            config,
            storage,
            ar_toolbox,
            wallet,
            min_rollup_size: Arc::new(AtomicU64::new(min_rollup_size)),
            meta_store,
            pending_mutations: Arc::new(AtomicU64::new(0)),
            pending_data_size: Arc::new(AtomicU64::new(0)),
            pending_start_block: Arc::new(AtomicU64::new(0)),
            pending_end_block: Arc::new(AtomicU64::new(0)),
            network_id,
        })
    }

    fn build_wallet(key_root_path: &str) -> Result<LocalWallet> {
        let config = KeyStoreConfig {
            key_root_path: key_root_path.to_string(),
        };
        let key_store = KeyStore::new(config);
        match key_store.has_key("evm") {
            true => {
                let data = key_store.get_key("evm")?;
                let data_ref: &[u8] = &data;
                let wallet = LocalWallet::from_bytes(data_ref)
                    .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
                Ok(wallet)
            }

            false => {
                let mut rng = rand::thread_rng();
                let wallet = LocalWallet::new(&mut rng);
                let data = wallet.signer().to_bytes();
                key_store.write_key("evm", data.deref())?;
                Ok(wallet)
            }
        }
    }

    fn gc_mutation(&self) -> Result<()> {
        let (last_start_block, last_end_block, first) = match self.storage.get_last_gc_record()? {
            Some(r) => (r.start_block, r.end_block, false),
            None => (0_u64, 0_u64, true),
        };

        info!(
            "last gc block range [{}, {})",
            last_start_block, last_end_block
        );

        let now = Instant::now();
        if self
            .storage
            .has_enough_round_left(last_start_block, self.config.min_gc_round_offset)?
        {
            if first {
                if let Some(r) = self.storage.get_rollup_record(last_start_block)? {
                    self.storage.gc_range_mutation(r.start_block, r.end_block)?;
                    let record = GcRecord {
                        start_block: r.start_block,
                        end_block: r.end_block,
                        data_size: r.raw_data_size,
                        time: times::get_current_time_in_secs(),
                        processed_time: now.elapsed().as_secs(),
                    };
                    self.storage.add_gc_record(&record)?;
                    info!(
                        "gc mutation from block range [{}, {}) done",
                        r.start_block, r.end_block
                    );
                    Ok(())
                } else {
                    // going here is not normal case
                    warn!(
                        "fail to get next rollup record with start block {}",
                        last_start_block
                    );
                    Ok(())
                }
            } else {
                if let Some(r) = self.storage.get_next_rollup_record(last_start_block)? {
                    self.storage.gc_range_mutation(r.start_block, r.end_block)?;
                    let record = GcRecord {
                        start_block: r.start_block,
                        end_block: r.end_block,
                        data_size: r.raw_data_size,
                        time: times::get_current_time_in_secs(),
                        processed_time: now.elapsed().as_secs(),
                    };
                    self.storage.add_gc_record(&record)?;
                    info!(
                        "gc mutation from block range [{}, {}) done",
                        r.start_block, r.end_block
                    );
                    Ok(())
                } else {
                    // going here is not normal case
                    warn!(
                        "fail to get next rollup record with start block {}",
                        last_start_block
                    );
                    Ok(())
                }
            }
        } else {
            info!("not enough round to run gc");
            Ok(())
        }
    }

    pub async fn get_ar_account(&self) -> Result<(String, String)> {
        self.ar_toolbox.get_ar_account().await
    }

    pub fn get_pending_rollup(&self) -> RollupRecord {
        RollupRecord {
            end_block: self.pending_end_block.load(Ordering::Relaxed),
            start_block: self.pending_start_block.load(Ordering::Relaxed),
            raw_data_size: self.pending_data_size.load(Ordering::Relaxed),
            compress_data_size: 0,
            processed_time: 0,
            arweave_tx: "".to_string(),
            time: times::get_current_time_in_secs(),
            mutation_count: self.pending_mutations.load(Ordering::Relaxed),
            cost: 0,
            evm_tx: "".to_string(),
            evm_cost: 0,
        }
    }
    pub async fn get_evm_account(&self) -> Result<String> {
        Ok(format!(
            "0x{}",
            hex::encode(self.wallet.address().as_bytes())
        ))
    }

    pub fn update_min_rollup_size(&self, min_rollup_data_size: u64) {
        self.min_rollup_size
            .store(min_rollup_data_size, Ordering::Relaxed)
    }

    pub fn get_min_rollup_size(&self) -> u64 {
        self.min_rollup_size.load(Ordering::Relaxed)
    }

    pub async fn process(&self) -> Result<()> {
        self.storage.flush_state()?;
        let (last_start_block, last_end_block, tx) = match self.storage.get_last_rollup_record()? {
            Some(r) => (r.start_block, r.end_block, r.arweave_tx.to_string()),
            _ => (0_u64, 0_u64, "".to_string()),
        };
        let current_block = self.storage.get_current_block()?;
        if current_block <= last_end_block {
            info!("no block to rollup");
            return Ok(());
        }
        let now = Instant::now();
        info!(
            "the next rollup start block {} and the newest block {current_block}",
            last_end_block
        );
        let network_id = self.network_id.load(Ordering::Relaxed);
        self.pending_start_block
            .store(last_start_block, Ordering::Relaxed);
        self.pending_end_block
            .store(current_block, Ordering::Relaxed);
        let mutations = self
            .storage
            .get_range_mutations(last_end_block, current_block)?;
        if mutations.len() <= 0 {
            info!("no block to rollup");
            return Ok(());
        }
        self.pending_mutations
            .store(mutations.len() as u64, Ordering::Relaxed);
        let recordbatch = self
            .ar_toolbox
            .convert_mutations_to_recordbatch(&mutations)?;
        let memory_size = recordbatch.get_array_memory_size();
        self.pending_data_size
            .store(memory_size as u64, Ordering::Relaxed);
        if memory_size < self.min_rollup_size.load(Ordering::Relaxed) as usize {
            info!(
                "there not enough data to trigger rollup, the min_rollup_size {}, current size {}",
                self.config.min_rollup_size, memory_size
            );
            return Ok(());
        } else {
            self.pending_start_block
                .store(current_block, Ordering::Relaxed);
            self.pending_end_block
                .store(current_block, Ordering::Relaxed);
            self.pending_data_size.store(0, Ordering::Relaxed);
            self.pending_mutations.store(0, Ordering::Relaxed);
        }
        let (id, reward, num_rows, size) = self
            .ar_toolbox
            .compress_and_upload_record_batch(
                tx,
                last_end_block,
                current_block,
                &recordbatch,
                network_id,
            )
            .await?;
        let (evm_cost, tx_hash) = self
            .meta_store
            .update_rollup_step(id.as_str(), network_id)
            .await?;
        let tx_str = format!("0x{}", hex::encode(tx_hash.as_bytes()));
        info!("the process rollup done with num mutations {num_rows}, raw data size {memory_size}, compress data size {size} and processed time {} id {} ar cost {} and evm tx {} and cost {}", now.elapsed().as_secs(),
        id.as_str(), reward,
        tx_str.as_str(),
        evm_cost.as_u64()
        );
        let record = RollupRecord {
            end_block: current_block,
            raw_data_size: memory_size as u64,
            compress_data_size: size,
            processed_time: now.elapsed().as_secs(),
            arweave_tx: id,
            time: times::get_current_time_in_secs(),
            mutation_count: num_rows,
            cost: reward,
            start_block: last_end_block,
            evm_tx: tx_str,
            evm_cost: evm_cost.as_u64(),
        };
        self.storage
            .add_rollup_record(&record)
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        self.gc_mutation()?;
        Ok(())
    }
}
