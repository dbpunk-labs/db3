//
// deposit.rs
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

use ethers::{
    contract::abigen,
    core::types::{Address, U256},
    middleware::SignerMiddleware,
    providers::{Provider, Ws},
    signers::{LocalWallet, Signer},
};

use eyre::Result;
use std::sync::Arc;

abigen!(
    DB3TokenContract,
    "bridge/artifacts/contracts/DB3Token.sol/Db3Token.json"
);

abigen!(
    DB3RollupContract,
    "bridge/artifacts/contracts/DB3Rollup.sol/DB3Rollup.json"
);

///
///TODO handle error
///
pub async fn lock_balance(
    erc20_token_addr: &str,
    rollup_addr: &str,
    ws: &str,
    amount: f32,
    wallet: LocalWallet,
) -> Result<()> {
    let provider = Provider::<Ws>::connect(ws).await?;
    let provider_arc = Arc::new(provider);
    let token_address = erc20_token_addr.parse::<Address>().unwrap();
    let rollup_address = rollup_addr.parse::<Address>().unwrap();
    let signable_client = SignerMiddleware::new(provider_arc.clone(), wallet);
    let client = Arc::new(signable_client);
    let token_contract = DB3TokenContract::new(token_address, client.clone());
    let approve_amount = U256::from(100_000_000_000 as u64); // 10 db3
    let approve_request = token_contract.approve(rollup_address, approve_amount);
    let _result = approve_request.send().await;
    let rollup_contract = DB3RollupContract::new(rollup_address, client);
    let deposit_amount = U256::from((amount * 1000_000_000.0) as u64);
    let deposit_request = rollup_contract.deposit(deposit_amount);
    let _result = deposit_request.send().await;
    Ok(())
}
