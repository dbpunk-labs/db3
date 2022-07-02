//
//
// mod.rs
// Copyright (C) 2022 db3.network Author imrtstore <rtstore_dev@outlook.com>
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

use crate::error::{DB3Error, Result};
use etcd_client::Client;
uselog!(info);
pub mod cell_store;
pub mod meta_store;
pub mod object_store;

pub async fn build_meta_store(
    etcd_cluster: &str,
    etcd_root_path: &str,
    store_type: meta_store::MetaStoreType,
) -> Result<meta_store::MetaStore> {
    let meta_store_config = meta_store::MetaStoreConfig {
        store_type,
        root_path: etcd_root_path.to_string(),
    };
    let etcd_cluster_endpoints: Vec<&str> = etcd_cluster.split(',').collect();
    let client = match Client::connect(etcd_cluster_endpoints, None).await {
        Ok(client) => Ok(client),
        Err(_) => Err(DB3Error::NodeRPCInvalidEndpointError {
            name: "etcd".to_string(),
        }),
    }?;
    info!("connect to etcd {} done", etcd_cluster);
    let meta_store = meta_store::MetaStore::new(client, meta_store_config);
    Ok(meta_store)
}

pub async fn build_readonly_meta_store(
    etcd_cluster: &str,
    etcd_root_path: &str,
) -> Result<meta_store::MetaStore> {
    build_meta_store(
        etcd_cluster,
        etcd_root_path,
        meta_store::MetaStoreType::ImmutableMetaStore,
    )
    .await
}
