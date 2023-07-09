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
use db3_storage::db_store_v2::{DBStoreV2, DBStoreV2Config};
use db3_storage::key_store::{KeyStore, KeyStoreConfig};
use db3_storage::meta_store_client::MetaStoreClient;
use ethers::prelude::{LocalWallet, Signer};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::info;

pub struct RecoverConfig {
    pub db_store_config: DBStoreV2Config,
    pub key_root_path: String,
    pub ar_node_url: String,
    pub temp_data_path: String,
    pub contract_addr: String,
    pub evm_node_url: String,
    pub enable_mutation_recover: bool,
}
pub struct Recover {
    pub config: RecoverConfig,
    pub ar_toolbox: Arc<ArToolBox>,
    pub meta_store: Arc<MetaStoreClient>,
    pub db_store: Arc<DBStoreV2>,
    network_id: Arc<AtomicU64>,
}

impl Recover {
    pub async fn new(config: RecoverConfig, network_id: Arc<AtomicU64>) -> Result<Self> {
        let wallet = Self::build_wallet(config.key_root_path.as_str())?;
        info!(
            "evm address {}",
            format!("0x{}", hex::encode(wallet.address().as_bytes()))
        );
        //TODO config the chain id
        let wallet = wallet.with_chain_id(80001_u32);
        let meta_store = Arc::new(
            MetaStoreClient::new(
                config.contract_addr.as_str(),
                config.evm_node_url.as_str(),
                wallet,
            )
            .await?,
        );
        let ar_toolbox = Arc::new(ArToolBox::new(
            config.key_root_path.clone(),
            config.ar_node_url.clone(),
            config.temp_data_path.clone(),
        )?);
        let db_store = Arc::new(DBStoreV2::new(config.db_store_config.clone())?);
        Ok(Self {
            config,
            ar_toolbox,
            meta_store,
            db_store,
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

    pub async fn start() -> Result<()> {
        Ok(())
    }

    /// recover from start_block to latest arweave tx
    pub async fn recover_from_block(&self, start_block: u64) -> Result<u64> {
        let txs = self.fetch_arweave_tx_from_block(start_block).await?;
        for tx in txs.iter().rev() {
            self.recover_from_arweave_tx(tx.as_str()).await?;
        }
        Ok(start_block)
    }

    /// recover from arweave tx
    async fn recover_from_arweave_tx(&self, tx: &str) -> Result<()> {
        let record_batch_vec = self.ar_toolbox.download_and_parse_record_batch(tx).await?;
        for record_batch in record_batch_vec.iter() {
            let mutations = ArToolBox::convert_recordbatch_to_mutation(record_batch)?;
            for (body, block, order) in mutations.iter() {
                let (dm, address, nonce) =
                    MutationUtil::unwrap_and_light_verify(&body.payload, body.signature.as_str())
                        .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                let action = MutationAction::from_i32(dm.action).ok_or(
                    DB3Error::WriteStoreError("fail to convert action type".to_string()),
                )?;
                // apply mutation to db store
                self.db_store.apply_mutation(
                    action,
                    dm,
                    &address,
                    self.network_id.load(Ordering::Relaxed),
                    nonce,
                    block.clone(),
                    order.clone(),
                )?;
            }
        }

        Ok(())
    }
    /// fetch arweave tx range from block to latest tx
    async fn fetch_arweave_tx_from_block(&self, block: u64) -> Result<Vec<String>> {
        let mut txs = vec![];
        // 1. get latest arweave tx id from meta store
        let mut tx = self.get_latest_arweave_tx().await?;
        loop {
            let (_start_block, end_block, last_rollup_tx) =
                self.ar_toolbox.get_tx_tags(tx.as_str()).await?;
            // 2. if end_block < block, return txs
            if end_block < block {
                return Ok(txs);
            }
            txs.push(tx.clone());
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
    use super::*;
    use arweave_rs::crypto::base64;
    use db3_storage::doc_store::DocStoreConfig;
    use std::path::PathBuf;
    use tempdir::TempDir;

    async fn build_recover_instance(temp_dir: &TempDir) -> Recover {
        let contract_addr = "0x5FbDB2315678afecb367f032d93F642f64180aa3";
        let rpc_url = "http://127.0.0.1:8545";
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let key_root_path = path
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tools/keys")
            .to_str()
            .unwrap()
            .to_string();
        let network_id: u64 = 1;
        let real_path = temp_dir.path().to_str().unwrap().to_string();
        let db_store_config = DBStoreV2Config {
            db_path: real_path,
            db_store_cf_name: "db".to_string(),
            doc_store_cf_name: "doc".to_string(),
            collection_store_cf_name: "cf2".to_string(),
            index_store_cf_name: "index".to_string(),
            doc_owner_store_cf_name: "doc_owner".to_string(),
            db_owner_store_cf_name: "db_owner".to_string(),
            scan_max_limit: 50,
            enable_doc_store: false,
            doc_store_conf: DocStoreConfig::default(),
            doc_start_id: 1000,
        };

        let recover = Recover::new(
            RecoverConfig {
                db_store_config,
                key_root_path,
                ar_node_url: "https://arweave.net".to_string(),
                temp_data_path: temp_dir.path().to_str().unwrap().to_string(),
                contract_addr: contract_addr.to_string(),
                evm_node_url: rpc_url.to_string(),
                enable_mutation_recover: true,
            },
            Arc::new(AtomicU64::new(network_id)),
        )
        .await
        .unwrap();
        recover
    }
    #[tokio::test]
    async fn test_get_latest_arweave_tx() {
        let temp_dir = TempDir::new("test_get_latest_arweave_tx").unwrap();
        let recover = build_recover_instance(&temp_dir).await;
        let res = recover.get_latest_arweave_tx().await;
        assert!(res.is_ok());
        println!("res {:?}", res);
    }

    #[tokio::test]
    async fn test_fetch_arware_tx_from_block() {
        let temp_dir = TempDir::new("test_fetch_arware_tx_from_block").unwrap();
        let recover = build_recover_instance(&temp_dir).await;
        let res = recover.fetch_arweave_tx_from_block(0).await;
        assert!(res.is_ok());
        let txs = res.unwrap();
        assert!(txs.len() > 0);
        println!("txs {:?}", txs);
    }
}
