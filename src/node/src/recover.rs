//
// recover.rs
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
use crate::mutation_utils::MutationUtil;
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_v2_proto::MutationAction;
use db3_storage::ar_fs::{ArFileSystem, ArFileSystemConfig};
use db3_storage::db_store_v2::DBStoreV2;
use db3_storage::meta_store_client::MetaStoreClient;
use db3_storage::mutation_store::MutationStore;
use db3_storage::system_store::{SystemRole, SystemStore};
use ethers::prelude::Signer;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info};

#[derive(Clone)]
pub enum RecoverType {
    Index,
    Rollup,
}
#[derive(Clone)]
pub struct RecoverConfig {
    pub key_root_path: String,
    pub temp_data_path: String,
    pub recover_type: RecoverType,
}
pub struct Recover {
    pub config: RecoverConfig,
    pub ar_toolbox: Arc<ArToolBox>,
    pub meta_store: Arc<MetaStoreClient>,
    pub db_store: Arc<DBStoreV2>,
    pub storage: Option<Arc<MutationStore>>,
    network_id: Arc<AtomicU64>,
}

impl Recover {
    pub async fn new(
        config: RecoverConfig,
        db_store: DBStoreV2,
        system_store: Arc<SystemStore>,
        storage: Option<Arc<MutationStore>>,
    ) -> Result<Self> {
        let role = match config.recover_type {
            RecoverType::Index => SystemRole::DataIndexNode,
            RecoverType::Rollup => SystemRole::DataRollupNode,
        };
        let system_config = match system_store.get_config(&role) {
            Ok(Some(system_config)) => system_config,
            Ok(None) => {
                return Err(DB3Error::StoreEventError(
                    "system config not found".to_string(),
                ))
            }
            Err(e) => return Err(e),
        };
        let chain_id = system_config.chain_id;
        let wallet = system_store.get_evm_wallet(chain_id)?;
        let contract_addr = system_config.contract_addr;
        let evm_node_url = system_config.evm_node_url;
        let ar_node_url = system_config.ar_node_url;
        let network_id = Arc::new(AtomicU64::new(system_config.network_id));
        info!(
            "evm address {}",
            format!("0x{}", hex::encode(wallet.address().as_bytes()))
        );
        let meta_store = Arc::new(
            MetaStoreClient::new(contract_addr.as_str(), evm_node_url.as_str(), wallet, false)
                .await?,
        );
        let ar_fs_config = ArFileSystemConfig {
            arweave_url: ar_node_url,
            key_root_path: config.key_root_path.clone(),
        };
        let ar_filesystem = ArFileSystem::new(ar_fs_config)?;

        let ar_toolbox = Arc::new(ArToolBox::new(
            ar_filesystem,
            config.temp_data_path.clone(),
        )?);
        Ok(Self {
            config,
            ar_toolbox,
            meta_store,
            db_store: Arc::new(db_store),
            storage,
            network_id,
        })
    }

    pub async fn start() -> Result<()> {
        Ok(())
    }

    pub fn recover_stat(&self) -> Result<()> {
        self.db_store.recover_db_state()?;
        if let Some(s) = &self.storage {
            s.recover()?;
        }
        Ok(())
    }

    pub async fn recover_from_ar(&self) -> Result<()> {
        info!("start recover from arweave");
        let last_block = self.db_store.recover_block_state()?;
        let (block, _order) = match last_block {
            Some(block_state) => {
                info!(
                    "recover the block state done, last block is {:?}",
                    block_state
                );
                (block_state.block, block_state.order)
            }
            None => {
                info!("recover the block state done, last block is 0");
                (0, 0)
            }
        };
        self.recover_from_arweave(block).await?;
        info!("recover from arweave done!");
        Ok(())
    }

    /// recover from start_block to latest arweave tx
    pub async fn recover_from_arweave(&self, start_block: u64) -> Result<u64> {
        let mut from_block = start_block;
        loop {
            let txs = self.fetch_arweave_tx_from_block(from_block).await?;
            if txs.is_empty() {
                break;
            }
            for (tx, _end_block, version) in txs.iter().rev() {
                self.recover_from_arweave_tx(tx.as_str(), version.clone())
                    .await?;
            }
            from_block = txs[0].1 + 1;
        }

        Ok(from_block)
    }

    pub fn is_recover_rollup(&self) -> bool {
        match self.config.recover_type {
            RecoverType::Rollup => true,
            _ => false,
        }
    }

    /// recover from arweave tx
    async fn recover_from_arweave_tx(&self, tx: &str, version: Option<String>) -> Result<()> {
        debug!("recover_from_arweave_tx: {}, version {:?}", tx, version);
        let record_batch_vec = self.ar_toolbox.download_and_parse_record_batch(tx).await?;
        for record_batch in record_batch_vec.iter() {
            let mutations =
                ArToolBox::convert_recordbatch_to_mutation(record_batch, version.clone())?;
            for (body, block, order, doc_ids) in mutations.iter() {
                let (dm, address, nonce) =
                    MutationUtil::unwrap_and_light_verify(&body.payload, body.signature.as_str())
                        .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                let action = MutationAction::from_i32(dm.action).ok_or(
                    DB3Error::WriteStoreError("fail to convert action type".to_string()),
                )?;
                let doc_ids_map = MutationUtil::convert_doc_ids_map_to_vec(doc_ids.as_str())?;
                // apply mutation to db store
                self.db_store.apply_mutation(
                    action,
                    dm,
                    &address,
                    self.network_id.load(Ordering::Relaxed),
                    nonce,
                    block.clone(),
                    order.clone(),
                    &doc_ids_map,
                )?;

                if self.is_recover_rollup() {
                    if let Some(s) = &self.storage {
                        s.update_mutation_stat(
                            &body.payload,
                            body.signature.as_str(),
                            doc_ids.as_str(),
                            &address,
                            nonce,
                            *block,
                            *order,
                            self.network_id.load(Ordering::Relaxed),
                            action,
                        )?;
                    }
                }
            }
        }

        Ok(())
    }
    /// fetch arweave tx range from block to latest tx
    async fn fetch_arweave_tx_from_block(
        &self,
        block: u64,
    ) -> Result<Vec<(String, u64, Option<String>)>> {
        let mut txs = vec![];
        // 1. get latest arweave tx id from meta store
        let mut tx = self.get_latest_arweave_tx().await?;
        loop {
            println!("tx {}", tx.as_str());
            let (_start_block, end_block, last_rollup_tx, version) =
                self.ar_toolbox.get_tx_tags(tx.as_str()).await?;
            // 2. if end_block < block, return txs
            if end_block < block {
                return Ok(txs);
            }
            txs.push((tx.clone(), end_block, version));
            // stop if last_rollup_tx is None
            if let Some(t) = last_rollup_tx {
                tx = t;
            } else {
                break;
            }
        }
        Ok(txs)
    }

    /// retrieve the latest arweave tx id from meta store
    pub async fn get_latest_arweave_tx(&self) -> Result<String> {
        self.meta_store
            .get_latest_arweave_tx(self.network_id.load(Ordering::Relaxed))
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::node_test_base::tests::NodeTestBase;
    use std::thread::sleep;
    use tempdir::TempDir;

    #[tokio::test]
    async fn test_fetch_arware_tx_from_block() {
        sleep(std::time::Duration::from_secs(3));
        let tmp_dir_path =
            TempDir::new("test_fetch_arware_tx_from_block").expect("create temp dir");
        match NodeTestBase::setup_for_smoke_test(&tmp_dir_path).await {
            Ok((rollup_executor, recover, _storage)) => {
                let result = rollup_executor.process().await;
                assert_eq!(true, result.is_ok());
                let result = recover.fetch_arweave_tx_from_block(0).await;
                assert_eq!(true, result.is_ok());
                let txs = result.unwrap();
                assert!(txs.len() > 0);
                println!("txs: {:?}", txs);
            }
            Err(e) => {
                assert!(false, "{e}");
            }
        }
    }
}
