//
//
// mod.rs
// Copyright (C) 2022 rtstore.io Author imotai <codego.me@gmail.com>
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
//

pub mod compute_node_sdk;
pub mod memory_node_sdk;
pub mod meta_node_sdk;

use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::{RtStoreNodeType};
use crate::store::meta_store::MetaStore;
uselog!(info);

pub async fn build_compute_node_sdk(
    meta_store: &MetaStore,
) -> Result<compute_node_sdk::ComputeNodeSDK> {
    let nodes = meta_store.get_nodes(RtStoreNodeType::KComputeNode).await?;
    if nodes.is_empty() {
        return Err(RTStoreError::MetaStoreNotFoundErr);
    }
    let addr = format!("http://{}:{}", nodes[0].ns, nodes[0].port);
    info!("connect compute node {}", &addr);
    match compute_node_sdk::ComputeNodeSDK::connect(&addr).await {
        Ok(sdk) => Ok(sdk),
        Err(e) => Err(RTStoreError::NodeRPCError(format!(
            "fail to connect compute node for err {}",
            e
        ))),
    }
}

pub async fn build_memory_node_sdk(
    meta_store: &MetaStore,
) -> Result<memory_node_sdk::MemoryNodeSDK> {
    let nodes = meta_store.get_nodes(RtStoreNodeType::KMemoryNode).await?;
    if nodes.is_empty() {
        return Err(RTStoreError::MetaStoreNotFoundErr);
    }
    let addr = format!("http://{}:{}", nodes[0].ns, nodes[0].port);
    info!("connect memory node {}", &addr);
    match memory_node_sdk::MemoryNodeSDK::connect(&addr).await {
        Ok(sdk) => Ok(sdk),
        Err(e) => Err(RTStoreError::NodeRPCError(format!(
            "fail to connect memory node for err {}",
            e
        ))),
    }
}

pub async fn build_meta_node_sdk(meta_store: &MetaStore) -> Result<meta_node_sdk::MetaNodeSDK> {
    let nodes = meta_store.get_nodes(RtStoreNodeType::KMetaNode).await?;
    if nodes.is_empty() {
        return Err(RTStoreError::MetaStoreNotFoundErr);
    }
    let meta_addr = format!("http://{}:{}", nodes[0].ns, nodes[0].port);
    info!("connect meta node {}", &meta_addr);
    match meta_node_sdk::MetaNodeSDK::connect(&meta_addr).await {
        Ok(sdk) => Ok(sdk),
        Err(e) => Err(RTStoreError::NodeRPCError(format!(
            "fail to connect meta node for err {}",
            e
        ))),
    }
}
