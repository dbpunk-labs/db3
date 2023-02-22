//
// fund_faucet.rs
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

use ethers::providers::Middleware;
use ethers::{
    contract::abigen,
    core::types::{Address, TransactionRequest, U256},
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

///
///
/// this method is just for development
///
pub async fn send_fund_to_faucet(
    ws: &str,
    pk: &str,
    erc20_token_addr: &str,
    faucet_addr: &str,
    amount: u64,
) -> Result<()> {
    let provider = Provider::<Ws>::connect(ws).await?;
    let provider_arc = Arc::new(provider);
    let wallet = pk.parse::<LocalWallet>()?;
    let token_address = erc20_token_addr.parse::<Address>().unwrap();
    let faucet_address = faucet_addr.parse::<Address>().unwrap();
    let my_address = wallet.address();
    let signable_client = SignerMiddleware::new(provider_arc.clone(), wallet);
    let client = Arc::new(signable_client);
    let ten_eth: u64 = 10_000_000_000_000_000_000;
    // send x_eth to faucet account
    let tx = TransactionRequest::new()
        .to(faucet_address)
        .value(ten_eth)
        .from(my_address);
    client.send_transaction(tx, None).await?.await?;
    let token_contract = DB3TokenContract::new(token_address, client);
    let transfer_amount = U256::from(amount);
    let transfer_request = token_contract.transfer(faucet_address, transfer_amount);
    let result = transfer_request.send().await;
    println!("approve result {:?}", result);
    Ok(())
}
