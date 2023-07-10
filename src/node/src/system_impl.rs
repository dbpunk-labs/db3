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

use std::sync::Arc;
use ethers::abi::Address;
use crate::mutation_utils::MutationUtil;
use db3_storage::system_store::{SystemStore};
use db3_error::{Result, DB3Error};
use db3_proto::db3_system_proto::{
    system_server::System,
    SetupRequest, SetupResponse
};

type UpdateHook = fn();


///
/// the setup grpc service for data rollup node and data index node
///
pub struct SystemImpl {
    update_hook: UpdateHook,
    system_store: Arc<SystemStore>,
}

///
///
///
impl SystemImpl {
    pub fn new(
               update_hook: UpdateHook,
               state_store:Arc<StateStore>
               ) -> Self {
        Self {
            update_hook,
            state_store
        }
    }
}

#[tonic::async_trait]
impl System for SystemImpl {
    async fn setup(
        &self,
        request: Request<SetupRequest>,
    ) -> std::result::Result<Response<SetupResponse>, Status> {
        let r = request.into_inner();
        // TODO avoid replay attack
        // verify the typed data signature
        let (address, data) = MutationUtil::verify_setup(&r.payload, r.signature.as_str())
            .map_err(|e| Status::invalid_argument(format!("invalid signature {e}")))?;

        // only admin can request the setup function
        if self.admin_addr != addr {
            return Err(Status::permission_denied(
                "You are not the admin".to_string(),
            ));
        }

        // the chain id must be provided
        let chain_id = MutationUtil::get_u32_field(&data, "chainId", 0);
        if chain_id == 0 {
            return Err(Status::invalid_argument(
                    format!("invalid chain id {chain_id}")
            ));
        }

        let rollup_interval = MutationUtil::get_u64_field(
            &data,
            "rollupInterval",
            10 * 60 * 1000
        );

        let min_rollup_size = MutationUtil::get_u64_field(
            &data,
            "minRollupSize",
            1024 * 1024
        );

        let evm_node_rpc =
            MutationUtil::get_str_field(&data, "evmNodeRpc", "");
        if evm_node_rpc.is_empty() {
            return Err(Status::invalid_argument(
                    format!("evm node rpc is empty")
            ));
        }

        let ar_node_url = MutationUtil::get_str_field(
            &data,
            "arNodeUrl",
            ""
        );

        if ar_node_rpc.is_empty() {
            return Err(Status::invalid_argument(
                    format!("ar node rpc is empty")
            ));
        }

        let network = MutationUtil::get_str_field(&data, "network", "0")
            .parse::<u64>()
            .map_err(|e| Status::invalid_argument(format!("fail to parse network id {e}")))?;

        info!("setup with config {:?}", data);
        let system_config = SystemConfig {
            min_rollup_size,
            rollup_interval,
            network_id: network,
            evm_node_url: evm_node_rpc.to_string(),
            ar_node_url: ar_node_url.to_string(),
        };

        self.state_store
            .store_node_config("storage", &system_config)
            .map_err(|e| Status::internal(format!("{e}")))?;

        return Ok(Response::new(SetupResponse {
            code: 0,
            msg: "ok".to_string(),
        }));
    }
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn it_works() {
	}
}
