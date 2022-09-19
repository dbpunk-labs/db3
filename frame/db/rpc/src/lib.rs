//
// lib.rs
// Copyright (C) 2022 parallels <parallels@parallels-Parallels-Virtual-Platform>
// Distributed under terms of the MIT license.
//

#![warn(unused_crate_dependencies)]

use codec::Codec;
use jsonrpsee::{
    core::{async_trait, Error as JsonRpseeError, RpcResult},
    proc_macros::rpc,
    types::error::{CallError, ErrorCode, ErrorObject},
};
use serde::{Deserialize, Serialize};
use sp_api::ProvideRuntimeApi;
use sp_blockchain::HeaderBackend;
use sp_core::Bytes;
use sp_rpc::number::NumberOrHex;
use sp_runtime::{
    generic::BlockId,
    traits::{Block as BlockT, Header as HeaderT},
};
pub use sqldb_rpc_runtime_api::DBAccountApi;
use std::string::String;
use std::{marker::PhantomData, sync::Arc};

const RUNTIME_ERROR: i32 = 1;

#[rpc(client, server)]
pub trait SQLDBApi<AccountId> {
    #[method(name = "is_ns_owner")]
    fn is_ns_owner(&self, origin: AccountId, ns: String) -> RpcResult<bool>;

    #[method(name = "list_delegates")]
    fn list_delegates(&self, origin: AccountId) -> RpcResult<Vec<(AccountId, String, u8)>>;
}

/// Contracts RPC methods.
pub struct SQLDBIns<Client, Block> {
    client: Arc<Client>,
    _marker: PhantomData<Block>,
}

impl<Client, Block> SQLDBIns<Client, Block> {
    /// Create new `Contracts` with the given reference to the client.
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            _marker: Default::default(),
        }
    }
}

#[async_trait]
impl<Client, Block, AccountId> SQLDBApiServer<AccountId> for SQLDBIns<Client, Block>
where
    Block: BlockT,
    Client: Send + Sync + 'static + ProvideRuntimeApi<Block> + HeaderBackend<Block>,
    Client::Api: DBAccountApi<Block, AccountId>,
    AccountId: Codec,
{
    fn is_ns_owner(&self, accountid: AccountId, ns: String) -> RpcResult<bool> {
        let api = self.client.runtime_api();
        let at = BlockId::hash(self.client.info().best_hash);
        api.is_ns_owner(&at, accountid, ns.as_bytes().to_vec())
            .map_err(runtime_error_into_rpc_err)
    }

    fn list_delegates(&self, accountid: AccountId) -> RpcResult<Vec<(AccountId, String, u8)>> {
        let api = self.client.runtime_api();
        let at = BlockId::hash(self.client.info().best_hash);
        let result = api
            .list_delegates(&at, accountid)
            .map_err(runtime_error_into_rpc_err)?;
        let mut new_result: Vec<(AccountId, String, u8)> = Vec::new();
        for (delegate, ns, delegate_type) in result {
            new_result.push((
                delegate,
                std::str::from_utf8(&ns).unwrap().to_string(),
                delegate_type,
            ));
        }
        Ok(new_result)
    }
}
/// Converts a runtime trap into an RPC error.
fn runtime_error_into_rpc_err(err: impl std::fmt::Debug) -> JsonRpseeError {
    CallError::Custom(ErrorObject::owned(
        RUNTIME_ERROR,
        "Runtime error",
        Some(format!("{:?}", err)),
    ))
    .into()
}
