//
// faucet_node_impl.rs
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
    core::types::{Address, Signature, TransactionRequest, H256},
    middleware::SignerMiddleware,
    providers::{Middleware, Provider, Ws},
    signers::{LocalWallet, Signer},
};

use db3_error::{DB3Error, Result as DB3Result};
use db3_proto::db3_faucet_proto::{
    faucet_node_server::FaucetNode, FaucetRequest, FaucetResponse, FaucetState,
    GetFaucetStateRequest,
};
use db3_storage::faucet_store::FaucetStore;
use hex;
use redb::Database;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

abigen!(
    DB3TokenContract,
    "bridge/artifacts/contracts/DB3Token.sol/Db3Token.json"
);

#[derive(Debug)]
pub struct FaucetNodeConfig {
    pub erc20_address: String,
    pub node_list: Vec<String>,
    // a amount for every faucet request
    pub amount: u64,
    pub enable_eth_fund: bool,
}

pub struct FaucetNodeImpl {
    config: FaucetNodeConfig,
    db: Arc<Database>,
    client: Arc<SignerMiddleware<Arc<Provider<Ws>>, LocalWallet>>,
    address: Address,
    erc20_address: Address,
}

impl FaucetNodeImpl {
    pub async fn new(
        db: Arc<Database>,
        config: FaucetNodeConfig,
        wallet: LocalWallet,
    ) -> DB3Result<Self> {
        let provider = Provider::<Ws>::connect(&config.node_list[0])
            .await
            .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
        let address = wallet.address();
        info!(
            "new faucet node  with config {:?} and faucet evm address 0x{}",
            config,
            hex::encode(address.0.as_ref())
        );
        let erc20_address = config
            .erc20_address
            .parse::<Address>()
            .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
        let provider_arc = Arc::new(provider);
        let signable_client = SignerMiddleware::new(provider_arc, wallet);
        let client = Arc::new(signable_client);
        Ok(Self {
            config,
            db,
            client,
            address,
            erc20_address,
        })
    }

    fn current_seconds() -> u32 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs() as u32,
            Err(_) => 0,
        }
    }
}

#[tonic::async_trait]
impl FaucetNode for FaucetNodeImpl {
    async fn get_faucet_state(
        &self,
        _request: Request<GetFaucetStateRequest>,
    ) -> std::result::Result<Response<FaucetState>, Status> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| Status::internal(format!("fail to open transacion {e}")))?;
        let (count, total_fund) = FaucetStore::get_state(read_txn)
            .map_err(|e| Status::internal(format!("fail to open transacion {e}")))?;
        return Ok(Response::new(FaucetState {
            total_amount: total_fund,
            total_address: count,
        }));
    }

    async fn faucet(
        &self,
        request: Request<FaucetRequest>,
    ) -> std::result::Result<Response<FaucetResponse>, Status> {
        let faucet_req = request.into_inner();
        let message_hash = H256::from_slice(faucet_req.hash.as_ref());
        let signature = Signature::try_from(faucet_req.signature.as_ref())
            .map_err(|e| Status::internal(format!("invalid signature {e}")))?;
        let address = signature
            .recover(message_hash)
            .map_err(|e| Status::internal(format!("invalid signature {e}")))?;
        {
            let write_txn = self
                .db
                .begin_write()
                .map_err(|e| Status::internal(format!("fail to open transacion {e}")))?;
            {
                match FaucetStore::store_record(
                    write_txn,
                    address.0.as_ref(),
                    Self::current_seconds(),
                    self.config.amount,
                ) {
                    Ok(_) => {
                        info!("address {} has chance to request faucet", address);
                    }
                    Err(e) => {
                        warn!(
                            "address {} has no chance to request faucet for {e}",
                            address
                        );
                        return Err(Status::internal("request faucet too much".to_string()));
                    }
                }
            }
        }
        // send x_eth to faucet account
        if self.config.enable_eth_fund {
            // 0.05 eth
            let one_eth: u64 = 500_000_000_000_000_000;
            let tx = TransactionRequest::new()
                .to(address)
                .value(one_eth)
                .from(self.address);
            self.client
                .send_transaction(tx, None)
                .await
                .unwrap()
                .await
                .unwrap();
        }
        let token_contract = DB3TokenContract::new(self.erc20_address, self.client.clone());
        let balance = token_contract.balance_of(self.address).call().await;
        info!("the main account balance {:?}", balance);
        // transfer token
        let transfer_tx = token_contract.transfer(address, self.config.amount.into());
        let result = transfer_tx.send().await;
        match result {
            Ok(_) => {
                info!("send db3 token to address {} ok", address);
                return Ok(Response::new(FaucetResponse {
                    code: 0,
                    msg: "ok".to_string(),
                }));
            }
            Err(e) => {
                warn!("fail call erc20 transfer function for {e}");
                return Err(Status::internal(format!("fail to transfer token for {e}")));
            }
        }
    }
}
