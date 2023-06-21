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

use db3_error::{DB3Error, Result};
use ethers::{
    contract::abigen,
    core::types::Address,
    providers::{Http, Provider, ProviderExt},
};
use std::sync::Arc;
use tracing::debug;
abigen!(
    DB3MetaStore,
    "metadata/artifacts/contracts/DB3MetaStore.sol/DB3MetaStore.json"
);

pub struct MetaStoreClient {}

impl MetaStoreClient {
    pub async fn get_admin(contract_addr: &str, rpc_url: &str, network: u64) -> Result<Address> {
        debug!(
            "get admin with contract_addr {contract_addr}, rpc_url {rpc_url} and network {network}"
        );
        let address = contract_addr
            .parse::<Address>()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let client = Provider::<Http>::connect(rpc_url).await;
        let store = DB3MetaStore::new(address, Arc::new(client));
        let registration = store
            .get_network_registration(network)
            .call()
            .await
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        Ok(registration.admin)
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
        let addr = MetaStoreClient::get_admin(contract_addr, rpc_url, network)
            .await
            .unwrap();
        let expect_addr = "0xF78c7469939f1f21338E4E58b901EC7D9Aa29679";
        let expect_address = expect_addr.parse::<Address>().unwrap();
        assert_eq!(addr, expect_address);
    }
}
