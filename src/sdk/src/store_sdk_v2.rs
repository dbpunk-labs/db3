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
use db3_proto::db3_storage_proto::{
    storage_node_client::StorageNodeClient as StorageNodeV2Client, BlockRequest as BlockRequestV2,
    BlockResponse as BlockResponseV2, EventMessage as EventMessageV2, EventType as EventTypeV2,
    SubscribeRequest, Subscription as SubscriptionV2,
};

use ethers::core::types::{
    transaction::eip712::{EIP712Domain, TypedData, Types},
    Bytes,
};
use ethers::prelude::{LocalWallet, Signer};
use prost::Message;
use std::collections::BTreeMap;
use std::sync::Arc;
use tonic::{Status, Streaming};

pub struct StoreSDKV2 {
    client: Arc<StorageNodeV2Client<tonic::transport::Channel>>,
    wallet: LocalWallet,
    types: Types,
}

impl StoreSDKV2 {
    pub fn new(
        client: Arc<StorageNodeV2Client<tonic::transport::Channel>>,
        wallet: LocalWallet,
    ) -> Self {
        let json = serde_json::json!({
          "EIP712Domain": [
          ],
          "Message":[
          {"name":"payload", "type":"bytes"}
          ]
        });
        let types: Types = serde_json::from_value(json).unwrap();
        Self {
            client,
            wallet,
            types,
        }
    }

    pub async fn subscribe_event_message(
        &self,
    ) -> Result<tonic::Response<Streaming<EventMessageV2>>, Status> {
        let sub = SubscriptionV2 {
            topics: vec![EventTypeV2::Block.into()],
        };

        let mut buf = BytesMut::with_capacity(1024 * 4);
        sub.encode(&mut buf)
            .map_err(|e| Status::internal(format!("Fail to encode subscription {e}")))?;
        let buf = buf.freeze();
        let mbuf = Bytes(buf.clone());
        let mut message: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        message.insert(
            "payload".to_string(),
            serde_json::Value::from(format!("{mbuf}")),
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
        let req = SubscribeRequest {
            signature: sig,
            payload: message_str,
        };
        let mut client = self.client.as_ref().clone();
        client.subscribe(req).await
    }

    pub async fn get_blocks(
        &self,
        block_start: u64,
        block_end: u64,
    ) -> Result<tonic::Response<BlockResponseV2>, Status> {
        let req = BlockRequestV2 {
            block_start,
            block_end,
        };
        let mut client = self.client.as_ref().clone();
        client.get_block(req).await
    }
    pub async fn get_block_by_height(
        &self,
        height: u64,
    ) -> Result<tonic::Response<BlockResponseV2>, Status> {
        self.get_blocks(height, height + 1).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tonic::transport::Endpoint;

    async fn subscribe_event_message_flow(
        client: Arc<StorageNodeV2Client<tonic::transport::Channel>>,
    ) {
        let mut rng = rand::thread_rng();
        let wallet = LocalWallet::new(&mut rng);
        let sdk = StoreSDKV2::new(client, wallet);
        let res: Result<tonic::Response<Streaming<EventMessageV2>>, Status> =
            sdk.subscribe_event_message().await;
        println!("res {:?}", res);
        assert!(res.is_ok(), "{:?}", res);
        let two_second = Duration::from_millis(2000);
        std::thread::sleep(two_second);
        let mut stream = res.unwrap().into_inner();
        if let Some(next_message) = stream.message().await.unwrap() {
            let event_message: EventMessageV2 = next_message;
            assert_eq!(EventTypeV2::Block as i32, event_message.r#type);
            assert!(format!("{:?}", event_message)
                .contains("r#type: Block, event: Some(BlockEvent(BlockEvent { block_id:"));
        } else {
            assert!(false);
        }
    }

    async fn get_block_by_height_flow(
        client: Arc<StorageNodeV2Client<tonic::transport::Channel>>,
        height: u64,
    ) {
        let mut rng = rand::thread_rng();
        let wallet = LocalWallet::new(&mut rng);
        let sdk = StoreSDKV2::new(client, wallet);
        let res = sdk.get_block_by_height(height).await;
        println!("res {:?}", res);
        assert!(res.is_ok(), "{:?}", res);
    }

    #[tokio::test]
    async fn subscribe_event_message_ut() {
        let ep = "http://127.0.0.1:26619";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeV2Client::new(channel));

        subscribe_event_message_flow(client.clone()).await;
    }
    #[tokio::test]
    async fn get_block_by_height_ut() {
        let ep = "http://127.0.0.1:26619";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeV2Client::new(channel));
        get_block_by_height_flow(client.clone(), 1).await;
    }
}
