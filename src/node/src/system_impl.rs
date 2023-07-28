//
// system_impl.rs
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

use crate::mutation_utils::MutationUtil;
use crate::version_util;
use db3_crypto::db3_address::DB3Address;
use db3_error::{DB3Error, Result};
use db3_proto::db3_base_proto::SystemConfig;
use db3_proto::db3_base_proto::SystemStatus;
use db3_proto::db3_system_proto::{
    system_server::System, GetSystemStatusRequest, SetupRequest, SetupResponse,
};
use db3_storage::system_store::{SystemRole, SystemStore};
use ethers::types::Address;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

///
/// the setup grpc service for data rollup node and data index node
///
pub struct SystemImpl {
    system_store: Arc<SystemStore>,
    role: SystemRole,
    // include the protocol, host and port
    public_node_url: String,
    admin_addr: Address,
    sender: Sender<()>,
}

unsafe impl Send for SystemImpl {}
unsafe impl Sync for SystemImpl {}

impl SystemImpl {
    pub fn new(
        sender: Sender<()>,
        system_store: Arc<SystemStore>,
        role: SystemRole,
        public_node_url: String,
        admin_addr: &str,
    ) -> Result<Self> {
        let address = admin_addr
            .parse::<Address>()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        Ok(Self {
            sender,
            system_store,
            role,
            public_node_url,
            admin_addr: address,
        })
    }
}

#[tonic::async_trait]
impl System for SystemImpl {
    async fn setup(
        &self,
        request: Request<SetupRequest>,
    ) -> std::result::Result<Response<SetupResponse>, Status> {
        let r = request.into_inner();
        let (address, data) = MutationUtil::verify_setup(r.payload.as_str(), r.signature.as_str())
            .map_err(|e| Status::invalid_argument(format!("invalid signature {e}")))?;
        info!("setup with config {:?} from address {}", data, address);
        // only admin can request the setup function
        if self.admin_addr != address {
            return Err(Status::permission_denied(
                "You are not the admin".to_string(),
            ));
        }
        // the chain id must be provided
        let chain_id = MutationUtil::get_u32_field(&data, "chainId", 0);
        if chain_id == 0 {
            return Err(Status::invalid_argument(format!(
                "invalid chain id {chain_id}"
            )));
        }

        let contract_addr = MutationUtil::get_str_field(&data, "contractAddr", "");
        if contract_addr.is_empty() {
            return Err(Status::invalid_argument(format!(
                "contract address is empty"
            )));
        }
        let rollup_interval = MutationUtil::get_u64_field(&data, "rollupInterval", 10 * 60 * 1000);
        let rollup_max_interval =
            MutationUtil::get_u64_field(&data, "rollupMaxInterval", 24 * 60 * 60 * 1000);
        let min_gc_offset = MutationUtil::get_u64_field(&data, "minGcOffset", 10 * 24 * 60 * 1000);
        let min_rollup_size = MutationUtil::get_u64_field(&data, "minRollupSize", 1024 * 1024);
        let evm_node_rpc = MutationUtil::get_str_field(&data, "evmNodeUrl", "");
        if evm_node_rpc.is_empty() {
            return Err(Status::invalid_argument(format!("evm node rpc is empty")));
        }
        if !evm_node_rpc.starts_with("wss") && !evm_node_rpc.starts_with("ws") {
            return Err(Status::invalid_argument(format!(
                "only the websocket url is valid"
            )));
        }
        let ar_node_url = MutationUtil::get_str_field(&data, "arNodeUrl", "");
        if ar_node_url.is_empty() {
            return Err(Status::invalid_argument(format!("ar node rpc is empty")));
        }
        let network = MutationUtil::get_str_field(&data, "networkId", "0")
            .parse::<u64>()
            .map_err(|e| Status::invalid_argument(format!("fail to parse network id {e}")))?;
        if network == 0 {
            return Err(Status::invalid_argument(format!("invalid network id")));
        }
        if let Some(old_config) = self
            .system_store
            .get_config(&self.role)
            .map_err(|e| Status::internal(format!("fail to get old config {e}")))?
        {
            let system_config = SystemConfig {
                min_rollup_size,
                rollup_interval,
                network_id: old_config.network_id,
                evm_node_url: evm_node_rpc.to_string(),
                ar_node_url: ar_node_url.to_string(),
                chain_id: old_config.chain_id,
                rollup_max_interval,
                contract_addr: old_config.contract_addr,
                min_gc_offset,
            };
            // if the node has been setuped the network id and chain id can not been changed
            self.system_store
                .update_config(&self.role, &system_config)
                .map_err(|e| Status::internal(format!("{e}")))?;
            if let Err(e) = self.sender.send(()).await {
                warn!("fail to send update config notification with error {e}");
            }
        } else {
            let system_config = SystemConfig {
                min_rollup_size,
                rollup_interval,
                network_id: network,
                evm_node_url: evm_node_rpc.to_string(),
                ar_node_url: ar_node_url.to_string(),
                chain_id,
                rollup_max_interval,
                contract_addr: contract_addr.to_string(),
                min_gc_offset,
            };
            self.system_store
                .update_config(&self.role, &system_config)
                .map_err(|e| Status::internal(format!("{e}")))?;
            if let Err(e) = self.sender.send(()).await {
                warn!("fail to send update config notification with error {e}");
            }
        }
        return Ok(Response::new(SetupResponse {
            code: 0,
            msg: "ok".to_string(),
        }));
    }

    async fn get_system_status(
        &self,
        _request: Request<GetSystemStatusRequest>,
    ) -> std::result::Result<Response<SystemStatus>, Status> {
        let system_config = self
            .system_store
            .get_config(&self.role)
            .map_err(|e| Status::internal(format!("fail to get old config {e}")))?;
        let has_inited = !system_config.is_none();
        let evm_address = self
            .system_store
            .get_evm_address()
            .map_err(|e| Status::internal(format!("fail to get evm address {e}")))?;
        let ar_address = self
            .system_store
            .get_ar_address()
            .map_err(|e| Status::internal(format!("fail to get ar address {e}")))?;
        let readable_addr = hex::encode(evm_address);
        let db3_addr = DB3Address::try_from(self.admin_addr.0.as_ref())
            .map_err(|e| Status::internal(format!("fail to convert the admin address {e}")))?;
        Ok(Response::new(SystemStatus {
            evm_account: format!("0x{}", readable_addr),
            evm_balance: "".to_string(),
            ar_account: ar_address,
            ar_balance: "".to_string(),
            node_url: self.public_node_url.to_string(),
            config: system_config,
            has_inited,
            admin_addr: db3_addr.to_hex(),
            version: Some(version_util::build_version()),
        }))
    }
}
