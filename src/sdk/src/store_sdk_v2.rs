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
use db3_crypto::db3_signer::Db3MultiSchemeSigner;
use db3_proto::db3_storage_proto::{
    storage_node_client::StorageNodeClient as StorageNodeV2Client, SubscribeRequest,
};
use db3_proto::db3_storage_proto::{
    BlockRequest as BlockRequestV2, BlockResponse as BlockResponseV2,
    EventMessage as EventMessageV2, EventType as EventTypeV2, Subscription as SubscriptionV2,
};

use prost::Message;
use std::sync::Arc;
use tonic::{Status, Streaming};

pub struct StoreSDKV2 {
    client: Arc<StorageNodeV2Client<tonic::transport::Channel>>,
    signer: Db3MultiSchemeSigner,
}

impl StoreSDKV2 {
    pub fn new(
        client: Arc<StorageNodeV2Client<tonic::transport::Channel>>,
        signer: Db3MultiSchemeSigner,
    ) -> Self {
        Self { client, signer }
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
        let signature = self
            .signer
            .sign(buf.as_ref())
            .map_err(|e| Status::internal(format!("Fail to sign subscription {e}")))?;
        let req = SubscribeRequest {
            signature: signature.as_ref().to_vec().to_owned(),
            payload: buf.as_ref().to_vec().to_owned(),
        };
        let mut client = self.client.as_ref().clone();
        client.subscribe(req).await
    }

    pub async fn get_block_by_height(
        &self,
        height: u64,
    ) -> Result<tonic::Response<BlockResponseV2>, Status> {
        let req = BlockRequestV2 {
            block_start: height,
            block_end: height + 1,
        };
        let mut client = self.client.as_ref().clone();
        client.get_block(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sdk_test;
    use std::time::Duration;
    use tonic::transport::Endpoint;

    async fn subscribe_event_message_flow(
        client: Arc<StorageNodeV2Client<tonic::transport::Channel>>,
        counter: i64,
    ) {
        let (_, signer) = sdk_test::gen_secp256k1_signer(counter);
        let sdk = StoreSDKV2::new(client, signer);
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
        counter: i64,
        height: u64,
    ) {
        let (_, signer) = sdk_test::gen_secp256k1_signer(counter);
        let sdk = StoreSDKV2::new(client, signer);
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

        subscribe_event_message_flow(client.clone(), 300).await;
    }
    #[tokio::test]
    async fn get_block_by_height_ut() {
        let ep = "http://127.0.0.1:26619";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeV2Client::new(channel));
        get_block_by_height_flow(client.clone(), 301, 1).await;
    }
}
