//
// show_evm_account.rs
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
    core::types::{Address, TransactionRequest, U256},
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

pub async fn get_account_balance(
    account_address: &Address,
    erc20_token_addr: &str,
    rollup_addr: &str,
    ws: &str,
) -> Result<(u64, u64)> {
    let token_address = erc20_token_addr.parse::<Address>().unwrap();
    let rollup_address = rollup_addr.parse::<Address>().unwrap();
    let provider = Provider::<Ws>::connect(ws).await?;
    let provider = Arc::new(provider);
    let token_contract = DB3TokenContract::new(token_address, provider.clone());
    let balance: U256 = token_contract
        .balance_of(*account_address)
        .call()
        .await
        .unwrap();
    let rollup_contract = DB3RollupContract::new(rollup_address, provider);
    let locked_balance: U256 = rollup_contract
        .get_locked_balance(*account_address)
        .call()
        .await
        .unwrap();
    Ok((balance.as_u64(), locked_balance.as_u64()))
}
