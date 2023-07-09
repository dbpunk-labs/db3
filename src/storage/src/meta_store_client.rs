//
// meta_store_client.rs
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

use arweave_rs::crypto::base64::Base64;
use db3_error::{DB3Error, Result};
use ethers::prelude::{LocalWallet, Signer};
use ethers::{
    contract::abigen,
    core::types::{Address, TxHash, U256},
    middleware::SignerMiddleware,
    providers::{Http, Middleware, Provider, ProviderExt},
};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
abigen!(DB3MetaStore, "abi/DB3MetaStore.json");

pub struct MetaStoreClient {
    address: Address,
    client: Arc<SignerMiddleware<Arc<Provider<Http>>, LocalWallet>>,
}
unsafe impl Sync for MetaStoreClient {}
unsafe impl Send for MetaStoreClient {}

impl MetaStoreClient {
    pub async fn new(contract_addr: &str, rpc_url: &str, wallet: LocalWallet) -> Result<Self> {
        let address = contract_addr
            .parse::<Address>()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let provider = Provider::<Http>::connect(rpc_url).await;
        let provider_arc = Arc::new(provider);
        let signable_client = SignerMiddleware::new(provider_arc, wallet);
        let client = Arc::new(signable_client);
        Ok(Self { address, client })
    }

    pub async fn register_data_network(
        &self,
        rollup_node_address: &Address,
        rollup_node_url: &str,
    ) -> Result<()> {
        let store = DB3MetaStore::new(self.address, self.client.clone());
        let empty_index_urls: Vec<String> = vec![];
        let empty_index_addresses: Vec<Address> = vec![];
        let desc: [u8; 32] = [0; 32];
        let tx = store.register_data_network(
            rollup_node_url.to_string(),
            rollup_node_address.clone(),
            empty_index_urls,
            empty_index_addresses,
            desc,
        );
        tx.send()
            .await
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        Ok(())
    }

    pub async fn get_latest_arweave_tx(&self, network: u64) -> Result<String> {
        let store = DB3MetaStore::new(self.address, self.client.clone());
        let network_id = U256::from(network);
        let data_network = store
            .get_data_network(network_id)
            .call()
            .await
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let tx_ref: &[u8] = data_network.latest_arweave_tx.as_ref();
        let b64 = Base64::from(tx_ref);
        Ok(format!("{}", b64))
    }

    pub async fn get_admin(&self, network: u64) -> Result<Address> {
        let store = DB3MetaStore::new(self.address, self.client.clone());
        let network_id = U256::from(network);
        let data_network = store
            .get_data_network(network_id)
            .call()
            .await
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        Ok(data_network.admin)
    }

    pub async fn update_rollup_step(&self, ar_tx: &str, network: u64) -> Result<(U256, TxHash)> {
        let b64: Base64 = Base64::from_str(ar_tx).map_err(|e| {
            DB3Error::StoreEventError(format!(
                "fail to decode arweave tx from base64 for error {e}"
            ))
        })?;
        let ar_tx_binary: [u8; 32] = b64.0.try_into().map_err(|_| {
            DB3Error::StoreEventError("fail to convert tx bytes to bytes32".to_string())
        })?;
        let store = DB3MetaStore::new(self.address, self.client.clone());
        let network_id = U256::from(network);
        let tx = store.update_rollup_steps(network_id, ar_tx_binary);
        //TODO set gas limit
        let pending_tx = tx
            .send()
            .await
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let tx_hash = pending_tx.tx_hash();
        let mut count_down: i32 = 5;
        loop {
            if count_down <= 0 {
                break;
            }
            sleep(Duration::from_millis(1000 * 5)).await;
            if let Some(tx) = self
                .client
                .get_transaction(tx_hash)
                .await
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?
            {
                if let Some(price) = tx.gas_price {
                    if let Some(fee) = price.checked_mul(tx.gas) {
                        return Ok((fee, tx_hash));
                    }
                }
                break;
            }
            count_down = count_down - 1;
        }
        Ok((U256::zero(), tx_hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    #[tokio::test]
    async fn register_a_data_network_test() {
        let data = hex::decode("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
            .unwrap();
        let data_ref: &[u8] = data.as_ref();
        let wallet = LocalWallet::from_bytes(data_ref).unwrap();
        let wallet = wallet.with_chain_id(31337_u32);
        let rollup_node_address = wallet.address();
        let contract_addr = "0x5FbDB2315678afecb367f032d93F642f64180aa3";
        let address = contract_addr.parse::<Address>().unwrap();
        let rpc_url = "http://127.0.0.1:8545";
        let client = MetaStoreClient::new(contract_addr, rpc_url, wallet)
            .await
            .unwrap();
        let result = client
            .register_data_network(&rollup_node_address, rpc_url)
            .await;
        assert!(result.is_ok());
        let tx = "TY5SMaPPRk_TMvSDROaQWyc_WHyJrEL760-UhiNnHG4";
        let result = client.update_rollup_step(tx, 1).await;
        assert!(result.is_ok());
        let tx_ret = client.get_latest_arweave_tx(1).await;
        assert!(tx_ret.is_ok());
        let tx_remote = tx_ret.unwrap();
        assert_eq!(tx, tx_remote);
    }
}
