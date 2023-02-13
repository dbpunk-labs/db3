//
// json_rpc_impl.rs
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
use super::context::Context;
use super::hash_util;
use super::json_rpc;
use actix_web::{web, Error, HttpResponse};
use bytes::Bytes;
use db3_crypto::db3_address::DB3Address;
use db3_proto::db3_base_proto::Units;
use db3_proto::db3_bill_proto::Bill;
use db3_proto::db3_mutation_proto::{Mutation, WriteRequest};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Map;
use serde_json::Value;
use std::str::FromStr;
use subtle_encoding::base64;
use tendermint::Hash as TMHash;
use tendermint_rpc::{Client, Id, Paging};
use tracing::debug;
fn bills_to_value(bills: &Vec<Bill>) -> Value {
    let mut new_bills: Vec<Value> = Vec::new();
    for bill in bills {
        let mut new_bill: Map<String, Value> = Map::new();
        let base64_bytes = base64::encode(&bill.tx_id);
        let base64_string = String::from_utf8(base64_bytes).unwrap();
        new_bill.insert("tx_id".to_string(), Value::from(base64_string));
        //TODO add owner address
        new_bill.insert("time".to_string(), Value::from(bill.time));
        new_bill.insert("block_height".to_string(), Value::from(bill.block_height));
        new_bill.insert("bill_type".to_string(), Value::from(bill.bill_type));
        if let Some(ref gas) = bill.gas_fee {
            new_bill.insert("gas_fee_amount".to_string(), Value::from(gas.amount));
            new_bill.insert("gas_fee_utype".to_string(), Value::from(gas.utype));
        }
        new_bills.push(Value::Object(new_bill));
    }
    Value::Array(new_bills)
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Wrapper<R> {
    /// JSON-RPC version
    jsonrpc: String,

    /// Identifier included in request
    id: Id,

    /// Results of request (if successful)
    result: Option<R>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ReadableKvPair {
    #[serde(with = "tendermint::serializers::bytes::string")]
    key: Vec<u8>,
    #[serde(with = "tendermint::serializers::bytes::hexstring")]
    value: Vec<u8>,
    action: i32,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ReadableMutation {
    #[serde(with = "tendermint::serializers::bytes::string")]
    ns: Vec<u8>,
    kv_pairs: Option<Vec<ReadableKvPair>>,
    nonce: u64,
    chain_id: i32,
    chain_role: i32,
    gas_price: Option<Units>,
    gas: u64,
    #[serde(with = "tendermint::serializers::bytes::hexstring")]
    signature: Vec<u8>,
}

enum ResponseWrapper {
    Internal(json_rpc::Response),
    External(String),
}

fn convert_mutation_to_readable(request: &WriteRequest) -> ReadableMutation {
    let mut kv_pairs: Vec<ReadableKvPair> = Vec::new();
    let mutation = Mutation::decode(request.payload.as_ref()).unwrap();
    for kv in &mutation.kv_pairs {
        kv_pairs.push(ReadableKvPair {
            key: kv.key.to_owned(),
            value: kv.value.to_owned(),
            action: kv.action,
        });
    }
    ReadableMutation {
        ns: mutation.ns,
        kv_pairs: Some(kv_pairs),
        nonce: mutation.nonce,
        chain_id: mutation.chain_id,
        chain_role: mutation.chain_role,
        gas_price: mutation.gas_price,
        gas: mutation.gas,
        signature: request.signature.to_owned(),
    }
}

pub async fn rpc_router(body: Bytes, context: web::Data<Context>) -> Result<HttpResponse, Error> {
    let request: json_rpc::Request = match serde_json::from_slice(body.as_ref()) {
        Ok(ok) => ok,
        Err(e) => {
            let err_str = format!("{}", e);
            let r = json_rpc::Response {
                jsonrpc: String::from(json_rpc::JSONRPC_VERSION),
                result: Value::Null,
                error: Some(json_rpc::ErrorData::new(-32700, err_str.as_str())),
                id: Value::Null,
            };
            return Ok(HttpResponse::Ok()
                .content_type("application/json")
                .body(r.dump()));
        }
    };
    debug!("request method {}", request.method.as_str());
    let response = match request.method.as_str() {
        "bills" => handle_bills(&context, request.id, request.params).await,
        "latest_blocks" => handle_latestblocks(&context, request.id, request.params).await,
        "block" => handle_block(&context, request.id, request.params).await,
        "mutation" => handle_mutation(&context, request.id, request.params).await,
        "account" => handle_account(&context, request.id, request.params).await,
        "net_info" => handle_netinfo(&context, request.id, request.params).await,
        "validators" => handle_validators(&context, request.id, request.params).await,
        "broadcast" => handle_broadcast(&context, request.id, request.params).await,
        _ => todo!(),
    };
    let r = match response {
        Ok(r) => r,
        Err(e) => {
            let err_str = format!("{}", e);
            let r = ResponseWrapper::Internal(json_rpc::Response {
                jsonrpc: String::from(json_rpc::JSONRPC_VERSION),
                result: Value::Null,
                error: Some(json_rpc::ErrorData::new(-32700, err_str.as_str())),
                id: Value::Null,
            });
            r
        }
    };
    match r {
        ResponseWrapper::Internal(i) => Ok(HttpResponse::Ok()
            .content_type("application/json")
            .body(i.dump())),
        ResponseWrapper::External(e) => {
            Ok(HttpResponse::Ok().content_type("application/json").body(e))
        }
    }
}

///
/// send mutation or query session to tendermint
///
async fn handle_broadcast(
    context: &Context,
    id: Value,
    params: Vec<Value>,
) -> Result<ResponseWrapper, json_rpc::ErrorData> {
    if params.len() == 0 {
        let err = "invalid parameters";
        Err(json_rpc::ErrorData::new(-32602, err))
    } else {
        // the param must be encoded as base64 string
        if let Value::String(s) = &params[0] {
            let tx = base64::decode(s.as_str())
                .map_err(|e| json_rpc::ErrorData::new(-32602, format!("{}", e).as_str()))?;
            let response = context
                .client
                .broadcast_tx_async(tx)
                .await
                .map_err(|e| json_rpc::ErrorData::new(-32603, format!("{}", e).as_str()))?;
            let external_id = match id {
                Value::Number(n) => Id::Num(n.as_i64().unwrap()),
                Value::String(s) => Id::Str(s),
                _ => todo!(),
            };
            let base64_byte = base64::encode(response.hash.as_ref());
            let hash = String::from_utf8_lossy(base64_byte.as_ref()).to_string();
            let wrapper = Wrapper {
                jsonrpc: String::from(json_rpc::JSONRPC_VERSION),
                result: Some(hash),
                id: external_id,
            };
            return Ok(ResponseWrapper::External(
                serde_json::to_string(&wrapper).unwrap(),
            ));
        } else {
            let err = "invalid parameters";
            Err(json_rpc::ErrorData::new(-32602, err))
        }
    }
}

async fn handle_validators(
    context: &Context,
    id: Value,
    params: Vec<Value>,
) -> Result<ResponseWrapper, json_rpc::ErrorData> {
    if params.len() == 0 {
        let err = "invalid parameters";
        Err(json_rpc::ErrorData::new(-32602, err))
    } else {
        if let Value::Number(n) = &params[0] {
            let height = n.as_u64().unwrap() as u32;
            let response = context
                .client
                .validators(height, Paging::All)
                .await
                .map_err(|e| json_rpc::ErrorData::new(-32603, format!("{}", e).as_str()))?;
            let external_id = match id {
                Value::Number(n) => Id::Num(n.as_i64().unwrap()),
                Value::String(s) => Id::Str(s),
                _ => todo!(),
            };
            let wrapper = Wrapper {
                jsonrpc: String::from(json_rpc::JSONRPC_VERSION),
                result: Some(response),
                id: external_id,
            };
            return Ok(ResponseWrapper::External(
                serde_json::to_string(&wrapper).unwrap(),
            ));
        }
        Err(json_rpc::ErrorData::std(-32602))
    }
}

async fn handle_netinfo(
    context: &Context,
    id: Value,
    _params: Vec<Value>,
) -> Result<ResponseWrapper, json_rpc::ErrorData> {
    let response = context
        .client
        .net_info()
        .await
        .map_err(|e| json_rpc::ErrorData::new(-32603, format!("{}", e).as_str()))?;
    let external_id = match id {
        Value::Number(n) => Id::Num(n.as_i64().unwrap()),
        Value::String(s) => Id::Str(s),
        _ => todo!(),
    };
    let wrapper = Wrapper {
        jsonrpc: String::from(json_rpc::JSONRPC_VERSION),
        result: Some(response),
        id: external_id,
    };
    return Ok(ResponseWrapper::External(
        serde_json::to_string(&wrapper).unwrap(),
    ));
}

async fn handle_account(
    context: &Context,
    id: Value,
    params: Vec<Value>,
) -> Result<ResponseWrapper, json_rpc::ErrorData> {
    if params.len() == 0 {
        let err = "invalid parameters";
        Err(json_rpc::ErrorData::new(-32601, err))
    } else {
        if let Value::String(s) = &params[0] {
            if let Ok(addr) = DB3Address::try_from(s.as_str()) {
                let account = match context.node_store.lock() {
                    Ok(mut store) => store.get_auth_store().get_account(&addr),
                    _ => todo!(),
                }
                .map_err(|_| json_rpc::ErrorData::new(-32601, "fail to get account"))?;
                let external_id = match id {
                    Value::Number(n) => Id::Num(n.as_i64().unwrap()),
                    Value::String(s) => Id::Str(s),
                    _ => todo!(),
                };
                let wrapper = Wrapper {
                    jsonrpc: String::from(json_rpc::JSONRPC_VERSION),
                    result: Some(account),
                    id: external_id,
                };
                return Ok(ResponseWrapper::External(
                    serde_json::to_string(&wrapper).unwrap(),
                ));
            }
        }
        let err = "invalid parameters";
        Err(json_rpc::ErrorData::new(-32601, err))
    }
}

async fn handle_mutation(
    context: &Context,
    id: Value,
    params: Vec<Value>,
) -> Result<ResponseWrapper, json_rpc::ErrorData> {
    if params.len() == 0 {
        let err = "invalid parameters";
        Err(json_rpc::ErrorData::new(-32601, err))
    } else {
        if let Value::String(s) = &params[0] {
            let tx_hash_ret = hash_util::base64_to_hash(s.as_str());
            if let Ok(tx_hash) = tx_hash_ret {
                let response = context.client.tx(tx_hash, false).await.unwrap();
                let wrequest = WriteRequest::decode(response.tx.as_ref()).unwrap();
                let readable_mutation = convert_mutation_to_readable(&wrequest);
                let external_id = match id {
                    Value::Number(n) => Id::Num(n.as_i64().unwrap()),
                    Value::String(s) => Id::Str(s),
                    _ => todo!(),
                };
                let wrapper = Wrapper {
                    jsonrpc: String::from(json_rpc::JSONRPC_VERSION),
                    result: Some(readable_mutation),
                    id: external_id,
                };
                return Ok(ResponseWrapper::External(
                    serde_json::to_string(&wrapper).unwrap(),
                ));
            }
        }
        let err = "respnse errr";
        Err(json_rpc::ErrorData::new(-32601, err))
    }
}

async fn handle_block(
    context: &Context,
    id: Value,
    params: Vec<Value>,
) -> Result<ResponseWrapper, json_rpc::ErrorData> {
    if params.len() == 0 {
        let err = "invalid parameters";
        Err(json_rpc::ErrorData::new(-32601, err))
    } else {
        if let Value::String(s) = &params[0] {
            if let Ok(h) = TMHash::from_str(s.as_str()) {
                let response = context.client.block_by_hash(h).await;
                let external_id = match id {
                    Value::Number(n) => Id::Num(n.as_i64().unwrap()),
                    Value::String(s) => Id::Str(s),
                    _ => todo!(),
                };
                if let Ok(r) = response {
                    let wrapper = Wrapper {
                        jsonrpc: String::from(json_rpc::JSONRPC_VERSION),
                        result: Some(r),
                        id: external_id,
                    };
                    return Ok(ResponseWrapper::External(
                        serde_json::to_string(&wrapper).unwrap(),
                    ));
                }
            }
        }
        let err = "respnse errr";
        Err(json_rpc::ErrorData::new(-32601, err))
    }
}

async fn handle_bills(
    context: &Context,
    id: Value,
    params: Vec<Value>,
) -> Result<ResponseWrapper, json_rpc::ErrorData> {
    if params.len() == 0 {
        let err = "invalid parameters";
        Err(json_rpc::ErrorData::new(-32601, err))
    } else {
        if let Value::Number(n) = &params[0] {
            match context.node_store.lock() {
                Ok(mut store) => {
                    if let Ok(bills) = store.get_auth_store().get_bills(n.as_u64().unwrap()) {
                        let value = bills_to_value(&bills);
                        return Ok(ResponseWrapper::Internal(json_rpc::Response {
                            jsonrpc: String::from(json_rpc::JSONRPC_VERSION),
                            result: value,
                            error: None,
                            id,
                        }));
                    }
                }
                _ => todo!(),
            }
        }
        Err(json_rpc::ErrorData::std(-32601))
    }
}

async fn handle_latestblocks(
    context: &Context,
    id: Value,
    _params: Vec<Value>,
) -> Result<ResponseWrapper, json_rpc::ErrorData> {
    let response = context.client.status().await;
    match response {
        Ok(status) => {
            let max_height = status.sync_info.latest_block_height.value();
            let min_height = max_height - 10;
            let block_chain_response = context
                .client
                .blockchain(min_height as u32, max_height as u32)
                .await;
            let external_id = match id {
                Value::Number(n) => Id::Num(n.as_i64().unwrap()),
                Value::String(s) => Id::Str(s),
                _ => todo!(),
            };
            if let Ok(r) = block_chain_response {
                let wrapper = Wrapper {
                    jsonrpc: String::from(json_rpc::JSONRPC_VERSION),
                    result: Some(r),
                    id: external_id,
                };
                return Ok(ResponseWrapper::External(
                    serde_json::to_string(&wrapper).unwrap(),
                ));
            } else {
                Err(json_rpc::ErrorData::std(-32601))
            }
        }
        Err(e) => {
            let err = format!("{}", e);
            Err(json_rpc::ErrorData::new(-32601, err.as_str()))
        }
    }
}
