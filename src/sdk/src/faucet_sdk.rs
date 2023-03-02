//
// mutation_sdk.rs
// Copyright (C) 2022 db3.network Author imotai <codego.me@gmail.com>
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
use db3_proto::db3_faucet_proto::{faucet_node_client::FaucetNodeClient, FaucetRequest};
use ethers::{core::utils::hash_message, signers::LocalWallet};
use std::sync::Arc;

pub struct FaucetSDK {
    wallet: LocalWallet,
    client: Arc<FaucetNodeClient<tonic::transport::Channel>>,
}

impl FaucetSDK {
    pub fn new(
        client: Arc<FaucetNodeClient<tonic::transport::Channel>>,
        wallet: LocalWallet,
    ) -> Self {
        Self { wallet, client }
    }

    pub async fn faucet(&self) -> Result<()> {
        let message: [u8; 10] = [1; 10];
        let hash = hash_message(&message);
        let signature = self.wallet.sign_hash(hash);
        let faucet_request = FaucetRequest {
            hash: hash.0.to_vec(),
            signature: signature.to_vec(),
        };
        let request = tonic::Request::new(faucet_request);
        let mut client = self.client.as_ref().clone();
        let response = client
            .faucet(request)
            .await
            .map_err(|e| DB3Error::RequestFaucetError(format!("{e}")))?
            .into_inner();
        if response.code == 0 {
            Ok(())
        } else {
            Err(DB3Error::RequestFaucetError(response.msg.to_string()))
        }
    }
}
