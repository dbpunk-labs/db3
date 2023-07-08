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
use ethers::prelude::LocalWallet;
use ethers::{
    contract::abigen,
    core::types::{Address, TxHash, U256},
    middleware::SignerMiddleware,
    providers::{Http, Middleware, Provider, ProviderExt},
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
abigen!(DB3MetaStore, "abi/DB3MetaStore.json");

pub struct MetaStoreClient {
    address: Address,
    client: Arc<SignerMiddleware<Arc<Provider<Http>>, LocalWallet>>,
    network: Arc<AtomicU64>,
}
unsafe impl Sync for MetaStoreClient {}
unsafe impl Send for MetaStoreClient {}

impl MetaStoreClient {
    pub async fn new(
        contract_addr: &str,
        rpc_url: &str,
        network: Arc<AtomicU64>,
        wallet: LocalWallet,
    ) -> Result<Self> {
        let address = contract_addr
            .parse::<Address>()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let provider = Provider::<Http>::connect(rpc_url).await;
        let provider_arc = Arc::new(provider);
        let signable_client = SignerMiddleware::new(provider_arc, wallet);
        let client = Arc::new(signable_client);
        Ok(Self {
            address,
            client,
            network,
        })
    }

    pub async fn get_latest_arweave_tx(&self) -> Result<String> {
        let store = DB3MetaStore::new(self.address, self.client.clone());
        let network_id = U256::from(self.network.load(Ordering::Relaxed));
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

    pub async fn update_rollup_step(&self, ar_tx: &str) -> Result<(U256, TxHash)> {
        let b64: Base64 = serde_json::from_str(ar_tx)
            .map_err(|_| DB3Error::StoreEventError("fail to decode arweave tx".to_string()))?;
        let ar_tx_binary: [u8; 32] = b64
            .0
            .try_into()
            .map_err(|_| DB3Error::StoreEventError("fail to decode arweave tx".to_string()))?;
        let store = DB3MetaStore::new(self.address, self.client.clone());
        let network_id = U256::from(self.network.load(Ordering::Relaxed));
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
    async fn test_get_admin() {
        let contract_addr = "0xb9709cE5E749b80978182db1bEdfb8c7340039A9";
        let rpc_url = "https://polygon-mumbai.g.alchemy.com/v2/KIUID-hlFzpnLetzQdVwO38IQn0giefR";
        let network: u64 = 1687245246;
        //let addr = MetaStoreClient::get_admin(contract_addr, rpc_url, network)
        //    .await
        //    .unwrap();
        //let expect_addr = "0xF78c7469939f1f21338E4E58b901EC7D9Aa29679";
        //let expect_address = expect_addr.parse::<Address>().unwrap();
        //assert_eq!(addr, expect_address);
    }
}
