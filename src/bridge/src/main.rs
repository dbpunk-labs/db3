use ethers::contract::EthEvent;
use ethers::{
    contract::abigen,
    core::{
        abi::AbiDecode,
        types::{Address, BlockNumber, Filter, U256},
    },
    middleware::SignerMiddleware,
    providers::{Http, Middleware, Provider, StreamExt, Ws},
    signers::{LocalWallet, Signer},
};

use ethers::abi::RawLog;

use eyre::Result;
use std::sync::Arc;

abigen!(
    DB3RollupContract,
    "/home/jackwang/opensource/db3/bridge/artifacts/contracts/DB3Rollup.sol/DB3Rollup.json"
);

#[tokio::main]
async fn main() -> Result<()> {
    let provider = Provider::<Ws>::connect("ws://127.0.0.1:8545/").await?;
    let provider_arc = Arc::new(provider);
    let rollup_address = "0x6621e8996c47Bcf2fe3a6caCD9108457E9B8CcB4"
        .parse::<Address>()
        .unwrap();

    let last_block = provider_arc
        .get_block(BlockNumber::Latest)
        .await?
        .unwrap()
        .number
        .unwrap();
    let db3_deposit_filter = Filter::new()
        .address(rollup_address)
        .event(&DepositFilter::abi_signature());
    let mut stream = provider_arc
        .subscribe_logs(&db3_deposit_filter)
        .await?
        .take(10);
    while let Some(log) = stream.next().await {
        let row_log = RawLog {
            topics: log.topics.clone(),
            data: log.data.to_vec(),
        };
        let event = DepositFilter::decode_log(&row_log);
        println!(
            "block: {:?}, tx: {:?}, token: {:?}, event:{:?}",
            log.block_number, log.transaction_hash, log.address, event
        );
    }
    Ok(())
}
