//
// system_sdk.rs
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

use db3_proto::db3_system_proto::SetupResponse;
use db3_proto::db3_system_proto::{system_client::SystemClient, SetupRequest};
use ethers::core::types::transaction::eip712::{EIP712Domain, TypedData, Types};
use ethers::prelude::{LocalWallet, Signer};
use std::collections::BTreeMap;
use std::sync::Arc;
use tonic::Status;

pub struct SystemConfig {
    pub rollup_interval: u64,
    pub min_rollup_size: u64,
    pub network: u64,
    pub chain_id: u32,
    pub contract_address: String,
    pub rollup_max_interval: u64,
    pub evm_node_rpc: String,
    pub ar_node_url: String,
    pub min_gc_offset: u64,
}

pub struct SystemSDK {
    client: Arc<SystemClient<tonic::transport::Channel>>,
    wallet: LocalWallet,
    types: Types,
}

impl SystemSDK {
    pub fn new(client: Arc<SystemClient<tonic::transport::Channel>>, wallet: LocalWallet) -> Self {
        let json = serde_json::json!({
          "EIP712Domain": [
          ],
          "Message":[
          {"name":"rollupInterval", "type":"string"},
          {"name":"minRollupSize", "type":"string"},
          {"name":"networkId", "type":"string"},
          {"name":"chainId", "type":"string"},
          {"name":"contractAddr", "type":"address"},
          {"name":"rollupMaxInterval", "type":"string"},
          {"name":"evmNodeUrl", "type":"string"},
          {"name":"arNodeUrl", "type":"string"},
          {"name":"minGcOffset", "type":"string"}
          ]
        });
        let types: Types = serde_json::from_value(json).unwrap();
        Self {
            client,
            wallet,
            types,
        }
    }

    pub async fn setup(
        &self,
        config: &SystemConfig,
    ) -> Result<tonic::Response<SetupResponse>, Status> {
        let mut message: BTreeMap<String, serde_json::Value> = BTreeMap::new();

        message.insert(
            "rollupInterval".to_string(),
            serde_json::Value::String(config.rollup_interval.to_string()),
        );

        message.insert(
            "minRollupSize".to_string(),
            serde_json::Value::String(config.min_rollup_size.to_string()),
        );

        message.insert(
            "networkId".to_string(),
            serde_json::Value::String(config.network.to_string()),
        );

        message.insert(
            "chainId".to_string(),
            serde_json::Value::String(config.chain_id.to_string()),
        );

        message.insert(
            "contractAddr".to_string(),
            serde_json::Value::String(config.contract_address.to_string()),
        );

        message.insert(
            "rollupMaxInterval".to_string(),
            serde_json::Value::String(config.rollup_max_interval.to_string()),
        );

        message.insert(
            "evmNodeUrl".to_string(),
            serde_json::Value::String(config.evm_node_rpc.to_string()),
        );

        message.insert(
            "arNodeUrl".to_string(),
            serde_json::Value::String(config.ar_node_url.to_string()),
        );
        message.insert(
            "minGcOffset".to_string(),
            serde_json::Value::String(config.min_gc_offset.to_string()),
        );

        let typed_data = TypedData {
            domain: EIP712Domain {
                name: None,
                version: None,
                chain_id: None,
                verifying_contract: None,
                salt: None,
            },
            types: self.types.clone(),
            primary_type: "Message".to_string(),
            message,
        };

        let signature = self
            .wallet
            .sign_typed_data(&typed_data)
            .await
            .map_err(|e| Status::internal(format!("Fail to sign subscription {e}")))?;

        let message_str = serde_json::to_string(&typed_data).map_err(|_| {
            Status::invalid_argument("fail to convert typed data to json".to_string())
        })?;

        let sig = format!("0x{}", signature);

        let req = SetupRequest {
            signature: sig,
            payload: message_str,
        };

        let mut client = self.client.as_ref().clone();
        client.setup(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::transport::Endpoint;

    #[tokio::test]
    async fn test_test_data_rollup_node_setup() {
        let data = hex::decode("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
            .unwrap();
        let data_ref: &[u8] = data.as_ref();
        let wallet = LocalWallet::from_bytes(data_ref).unwrap();
        let ep = "http://127.0.0.1:26619";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(SystemClient::new(channel));
        let system_sdk = SystemSDK::new(client, wallet);
        let config = SystemConfig {
            rollup_interval: 10 * 60 * 1000,
            min_rollup_size: 1024 * 1024,
            network: 1,
            chain_id: 31337_u32,
            contract_address: "0x5fbdb2315678afecb367f032d93f642f64180aa3".to_string(),
            rollup_max_interval: 24 * 60 * 60 * 1000,
            evm_node_rpc: "ws://127.0.0.1:8545".to_string(),
            ar_node_url: "http://127.0.0.1:1984".to_string(),
            min_gc_offset: 10 * 24 * 60 * 60,
        };
        let response = system_sdk.setup(&config).await.unwrap().into_inner();
        assert_eq!(0, response.code);
    }

    #[tokio::test]
    async fn test_test_data_index_node_setup() {
        let data = hex::decode("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
            .unwrap();
        let data_ref: &[u8] = data.as_ref();
        let wallet = LocalWallet::from_bytes(data_ref).unwrap();
        let ep = "http://127.0.0.1:26639";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(SystemClient::new(channel));
        let system_sdk = SystemSDK::new(client, wallet);
        let config = SystemConfig {
            rollup_interval: 10 * 60 * 1000,
            min_rollup_size: 1024 * 1024,
            network: 1,
            chain_id: 31337_u32,
            contract_address: "0x5fbdb2315678afecb367f032d93f642f64180aa3".to_string(),
            rollup_max_interval: 24 * 60 * 60 * 1000,
            evm_node_rpc: "ws://127.0.0.1:8545".to_string(),
            ar_node_url: "http://127.0.0.1:1984".to_string(),
            min_gc_offset: 10 * 24 * 60 * 60,
        };
        let response = system_sdk.setup(&config).await.unwrap().into_inner();
        assert_eq!(0, response.code);
    }
}
