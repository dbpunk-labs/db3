//
// bill_sdk.rs
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

use bytes::BytesMut;
use db3_error::{DB3Error, Result};
use db3_proto::db3_bill_proto::{Bill, BillQueryRequest};
use merk::proofs::{Decoder, Node, Op as ProofOp};
use prost::Message;
use std::str::FromStr;
use tendermint::{abci::Path, block::Height};
use tendermint_rpc::{Client, HttpClient};

pub struct BillSDK {
    client: HttpClient,
}

impl BillSDK {
    pub fn new(client: HttpClient) -> Self {
        Self { client }
    }

    pub async fn get_bills_by_block(&self, height: u64, start: u64, end: u64) -> Result<Vec<Bill>> {
        let request = BillQueryRequest {
            block_height: height,
            start_id: start,
            end_id: end,
        };
        let mut buf = BytesMut::with_capacity(1024 * 8);
        request
            .encode(&mut buf)
            .map_err(|e| DB3Error::BillSDKError(format!("{}", e)))?;
        let buf = buf.freeze();
        let path = Path::from_str("bill").map_err(|e| DB3Error::BillSDKError(format!("{}", e)))?;
        let result = self
            .client
            .abci_query(
                Some(path),
                buf.as_ref(),
                Some(Height::from(height as u32)),
                false,
            )
            .await
            .map_err(|e| DB3Error::BillSDKError(format!("{}", e)))?;
        let mut decoder = Decoder::new(result.value.as_ref());
        let mut bills: Vec<Bill> = Vec::new();
        loop {
            let item = decoder.next();
            if let Some(Ok(op)) = item {
                match op {
                    ProofOp::Push(Node::KV(_, v)) => {
                        if let Ok(b) = Bill::decode(v.as_ref()) {
                            bills.push(b);
                        }
                    }
                    _ => {}
                }
                continue;
            }
            break;
        }
        Ok(bills)
    }
}

#[cfg(test)]
mod tests {
    use super::BillSDK;
    use super::HttpClient;
    #[tokio::test]
    async fn it_get_bills() {
        let client = HttpClient::new("http://127.0.0.1:26657").unwrap();
        let sdk = BillSDK::new(client);
        let result = sdk.get_bills_by_block(1, 0, 10).await;
        assert!(result.is_ok());
        if let Ok(bills) = result {
            assert_eq!(0, bills.len());
        }
    }
}
