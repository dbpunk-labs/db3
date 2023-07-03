use crate::ar_toolbox::ArToolBox;
use arweave_rs::crypto::base64::Base64;
use bytes::Bytes;
use db3_error::{DB3Error, Result};
use db3_storage::key_store::{KeyStore, KeyStoreConfig};
use db3_storage::meta_store_client::MetaStoreClient;
use ethers::prelude::{LocalWallet, Signer};
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::info;
pub struct RecoverConfig {
    pub network_id: u64,
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
}

impl Recover {
    pub async fn new(config: RecoverConfig) -> Result<Self> {
        let network_id = Arc::new(AtomicU64::new(config.network_id));
        let wallet = Self::build_wallet(config.key_root_path.as_str())?;
        info!(
            "evm address {}",
            format!("0x{}", hex::encode(wallet.address().as_bytes()))
        );
        let wallet2 = Self::build_wallet(config.key_root_path.as_str())?;
        let wallet2 = wallet2.with_chain_id(80001_u32);
        let meta_store = Arc::new(
            MetaStoreClient::new(
                config.contract_addr.as_str(),
                config.evm_node_url.as_str(),
                network_id.clone(),
                wallet2,
            )
            .await?,
        );
        let ar_toolbox = Arc::new(ArToolBox::new(
            config.key_root_path.clone(),
            config.ar_node_url.clone(),
            config.temp_data_path.clone(),
            network_id.clone(),
        )?);

        Ok(Self {
            config,
            ar_toolbox,
            meta_store,
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

    pub async fn recover_from_block(&self, start_block: u64) -> Result<u64> {
        Ok(start_block)
    }

    /// retrieve the latest arweave tx id from meta store
    pub async fn get_latest_arweave_tx(&self) -> Result<String> {
        let tx = self.meta_store.get_latest_arweave_tx().await.unwrap();
        let data = hex::decode(&tx[2..])
            .map_err(|e| DB3Error::KeyCodecError(format!("fail to decode tx id for {e}")))
            .unwrap();
        let base64_tx_str = Base64(data)
            .to_utf8_string()
            .map_err(|e| DB3Error::KeyCodecError(format!("fail to decode tx id for {e}")))?;
        Ok(base64_tx_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arweave_rs::crypto::base64;
    use std::path::PathBuf;
    use tempdir::TempDir;

    #[tokio::test]
    async fn test_get_latest_arweave_tx() {
        let contract_addr = "0xb9709cE5E749b80978182db1bEdfb8c7340039A9";
        let rpc_url = "https://polygon-mumbai.g.alchemy.com/v2/KIUID-hlFzpnLetzQdVwO38IQn0giefR";
        let temp_dir = TempDir::new("test_get_admin").expect("create temp dir");
        let arweave_url = "https://arweave.net";
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

        let network_id: u64 = 1687961160;
        let recover = Recover::new(RecoverConfig {
            network_id,
            key_root_path,
            ar_node_url: "https://arweave.net".to_string(),
            temp_data_path: temp_dir.path().to_str().unwrap().to_string(),
            contract_addr: contract_addr.to_string(),
            evm_node_url: rpc_url.to_string(),
            enable_mutation_recover: true,
        })
        .await
        .unwrap();
        let res = recover.get_latest_arweave_tx().await;
        assert!(res.is_ok());
        println!("res {:?}", res);
    }
}
