//
// system_store.rs
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

use crate::key_store::{KeyStore, KeyStoreConfig};
use crate::state_store::StateStore;
use arweave_rs::{crypto::sign::Signer as ArSigner, Arweave};
use db3_error::{DB3Error, Result};
use db3_proto::db3_base_proto::SystemConfig;
use ethers::core::types::Address;
use ethers::prelude::{LocalWallet, Signer};
use rsa::{pkcs8::DecodePrivateKey, pkcs8::EncodePrivateKey, RsaPrivateKey};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;
#[derive(Clone)]
/// the node role of db3 network
/// 1. DataRollupNode , rollup the data to arweave
/// 2. DataIndexNode, index the data and serve the data
pub enum SystemRole {
    DataRollupNode,
    DataIndexNode,
}

impl SystemRole {
    pub fn get_name(&self) -> &str {
        match self {
            // call 'storage' and 'index' for some compatible reasons
            SystemRole::DataRollupNode => "storage",
            SystemRole::DataIndexNode => "index",
        }
    }
}

#[derive(Clone)]
pub struct SystemStoreConfig {
    pub key_root_path: String,
    pub evm_wallet_key: String,
    pub ar_wallet_key: String,
}

#[derive(Clone)]
pub struct SystemStore {
    config: SystemStoreConfig,
    state_store: Arc<StateStore>,
}

unsafe impl Send for SystemStore {}
unsafe impl Sync for SystemStore {}

impl SystemStore {
    pub fn new(config: SystemStoreConfig, state_store: Arc<StateStore>) -> Self {
        Self {
            config,
            state_store,
        }
    }

    pub fn get_config(&self, role: &SystemRole) -> Result<Option<SystemConfig>> {
        self.state_store.get_node_config(role.get_name())
    }

    pub fn update_config(&self, role: &SystemRole, config: &SystemConfig) -> Result<()> {
        self.state_store.store_node_config(role.get_name(), config)
    }

    pub fn get_evm_address(&self) -> Result<Address> {
        let wallet = self.get_evm_wallet(0)?;
        Ok(wallet.address())
    }

    pub fn get_ar_address(&self) -> Result<String> {
        let key_store_config = KeyStoreConfig {
            key_root_path: self.config.key_root_path.to_string(),
        };
        let key_store = KeyStore::new(key_store_config);
        match key_store.has_key(self.config.ar_wallet_key.as_str()) {
            true => {
                let data = key_store.get_key(self.config.ar_wallet_key.as_str())?;
                let data_ref: &[u8] = &data;
                let priv_key: RsaPrivateKey = RsaPrivateKey::from_pkcs8_der(data_ref)
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                let signer = ArSigner::new(priv_key);
                let address = signer
                    .wallet_address()
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                Ok(address.to_string())
            }
            false => {
                let mut rng = rand::thread_rng();
                let bits = 2048;
                let priv_key = RsaPrivateKey::new(&mut rng, bits)
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                let doc = priv_key
                    .to_pkcs8_der()
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                key_store
                    .write_key(self.config.ar_wallet_key.as_str(), doc.as_ref())
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                let signer = ArSigner::new(priv_key);
                let address = signer
                    .wallet_address()
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                let address_str = address.to_string();
                info!("generate a new arweave wallet with address {}", address_str);
                Ok(address_str)
            }
        }
    }

    pub fn get_ar_wallet(&self, url: &str) -> Result<Arweave> {
        let arweave_url =
            url::Url::from_str(url).map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
        let key_store_config = KeyStoreConfig {
            key_root_path: self.config.key_root_path.to_string(),
        };
        let key_store = KeyStore::new(key_store_config);
        match key_store.has_key(self.config.ar_wallet_key.as_str()) {
            true => {
                let data = key_store.get_key(self.config.ar_wallet_key.as_str())?;
                let data_ref: &[u8] = &data;
                let priv_key: RsaPrivateKey = RsaPrivateKey::from_pkcs8_der(data_ref)
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                Arweave::from_private_key(priv_key, arweave_url)
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))
            }
            false => {
                let mut rng = rand::thread_rng();
                let bits = 2048;
                let priv_key = RsaPrivateKey::new(&mut rng, bits)
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                let doc = priv_key
                    .to_pkcs8_der()
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                key_store
                    .write_key(self.config.ar_wallet_key.as_str(), doc.as_ref())
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                let arweave = Arweave::from_private_key(priv_key, arweave_url)
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                info!(
                    "generate a new arweave wallet with address {}",
                    arweave.get_wallet_address().as_str()
                );
                Ok(arweave)
            }
        }
    }

    pub fn get_evm_wallet(&self, chain_id: u32) -> Result<LocalWallet> {
        let config = KeyStoreConfig {
            key_root_path: self.config.key_root_path.to_string(),
        };
        let key_store = KeyStore::new(config);
        match key_store.has_key(self.config.evm_wallet_key.as_str()) {
            true => {
                let data = key_store.get_key(self.config.evm_wallet_key.as_str())?;
                let data_ref: &[u8] = &data;
                let wallet = LocalWallet::from_bytes(data_ref)
                    .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
                let wallet = wallet.with_chain_id(chain_id);
                Ok(wallet)
            }
            false => {
                let mut rng = rand::thread_rng();
                let wallet = LocalWallet::new(&mut rng);
                let data = wallet.signer().to_bytes();
                key_store.write_key(self.config.evm_wallet_key.as_str(), data.deref())?;
                let wallet = wallet.with_chain_id(chain_id);
                let address_str = format!("0x{}", hex::encode(wallet.address().as_bytes()));
                info!(
                    "generate a new evm wallet with address {} with chain_id {}",
                    address_str.as_str(),
                    chain_id
                );
                Ok(wallet)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_store::StateStoreConfig;
    use tempdir::TempDir;

    #[test]
    fn system_config_smoke_test() {
        let tmp_dir_path = TempDir::new("system_store_path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let tmp_dir_path2 = TempDir::new("mutation_store_path").expect("create temp dir");
        let real_path2 = tmp_dir_path2.path().to_str().unwrap().to_string();

        {
            let state_config = StateStoreConfig {
                db_path: real_path2.to_string(),
            };
            let store = Arc::new(StateStore::new(state_config).unwrap());
            let config = SystemStoreConfig {
                key_root_path: real_path.to_string(),
                evm_wallet_key: "evm".to_string(),
                ar_wallet_key: "ar".to_string(),
            };
            let system_store = SystemStore::new(config, store);
            if let Ok(Some(_)) = system_store.get_config(&SystemRole::DataIndexNode) {
                assert!(false);
            }

            let system_config = SystemConfig {
                min_rollup_size: 1,
                rollup_interval: 1,
                network_id: 1,
                evm_node_url: "evm_node_url".to_string(),
                ar_node_url: "ar_node_url".to_string(),
                chain_id: 1,
                rollup_max_interval: 5,
                contract_addr: "0x1213".to_string(),
                min_gc_offset: 1,
            };
            let result = system_store.update_config(&SystemRole::DataIndexNode, &system_config);
            assert!(result.is_ok());
        }

        {
            let state_config = StateStoreConfig {
                db_path: real_path2.to_string(),
            };
            let store = Arc::new(StateStore::new(state_config).unwrap());
            let config = SystemStoreConfig {
                key_root_path: real_path.to_string(),
                evm_wallet_key: "evm".to_string(),
                ar_wallet_key: "ar".to_string(),
            };
            let system_store = SystemStore::new(config, store);
            if let Ok(Some(c)) = system_store.get_config(&SystemRole::DataIndexNode) {
                assert_eq!(c.min_rollup_size, 1);
                assert_eq!(c.rollup_interval, 1);
                assert_eq!(c.network_id, 1);
                assert_eq!(c.evm_node_url.as_str(), "evm_node_url");
                assert_eq!(c.ar_node_url.as_str(), "ar_node_url");
                assert_eq!(c.chain_id, 1);
                assert_eq!(c.rollup_max_interval, 5);
                assert_eq!(c.contract_addr.as_str(), "0x1213");
            } else {
                assert!(false);
            }
        }
    }

    #[test]
    fn system_store_ar_smoke_test() {
        let tmp_dir_path = TempDir::new("system_store_path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let tmp_dir_path2 = TempDir::new("mutation_store_path").expect("create temp dir");
        let real_path2 = tmp_dir_path2.path().to_str().unwrap().to_string();
        let mut address: Vec<String> = Vec::new();
        {
            let state_config = StateStoreConfig {
                db_path: real_path2.to_string(),
            };
            let store = Arc::new(StateStore::new(state_config).unwrap());
            let config = SystemStoreConfig {
                key_root_path: real_path.to_string(),
                evm_wallet_key: "evm".to_string(),
                ar_wallet_key: "ar".to_string(),
            };
            let system_store = SystemStore::new(config, store);
            let result = system_store.get_ar_address();
            assert!(result.is_ok());
            address.push(result.unwrap());
        }

        {
            let state_config = StateStoreConfig {
                db_path: real_path2.to_string(),
            };
            let store = Arc::new(StateStore::new(state_config).unwrap());
            let config = SystemStoreConfig {
                key_root_path: real_path.to_string(),
                evm_wallet_key: "evm".to_string(),
                ar_wallet_key: "ar".to_string(),
            };
            let system_store = SystemStore::new(config, store);
            let result = system_store.get_ar_address();
            assert!(result.is_ok());
            let addr = result.unwrap();
            assert_eq!(addr, address[0]);
        }
    }
    #[test]
    fn system_store_evm_smoke_test() {
        let tmp_dir_path = TempDir::new("system_store_path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let tmp_dir_path2 = TempDir::new("mutation_store_path").expect("create temp dir");
        let real_path2 = tmp_dir_path2.path().to_str().unwrap().to_string();
        let mut address: Vec<Address> = Vec::new();
        {
            let state_config = StateStoreConfig {
                db_path: real_path2.to_string(),
            };
            let store = Arc::new(StateStore::new(state_config).unwrap());
            let config = SystemStoreConfig {
                key_root_path: real_path.to_string(),
                evm_wallet_key: "evm".to_string(),
                ar_wallet_key: "ar".to_string(),
            };
            let system_store = SystemStore::new(config, store);
            let result = system_store.get_evm_address();
            assert!(result.is_ok());
            address.push(result.unwrap());
        }

        {
            let state_config = StateStoreConfig {
                db_path: real_path2.to_string(),
            };
            let store = Arc::new(StateStore::new(state_config).unwrap());
            let config = SystemStoreConfig {
                key_root_path: real_path.to_string(),
                evm_wallet_key: "evm".to_string(),
                ar_wallet_key: "ar".to_string(),
            };
            let system_store = SystemStore::new(config, store);
            let result = system_store.get_evm_address();
            assert!(result.is_ok());
            let addr = result.unwrap();
            assert_eq!(addr, address[0]);
        }
    }
}
