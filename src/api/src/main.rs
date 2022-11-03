//
// main.rs
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

use std::{
    error,
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};

#[macro_use(uselog)]
extern crate uselog_rs;
uselog!(info);
use actix_web::{middleware, web, App, Error, HttpResponse, HttpServer};
use actix_cors::Cors;
use bytes::Bytes;
use db3_proto::db3_bill_proto::Bill;
use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
use db3_sdk::bill_sdk::BillSDK;
use futures_util::FutureExt as _;
use hex;
use serde_json::Map;
use serde_json::Value;
use tonic::transport::Endpoint;

#[allow(dead_code)]
mod convention;

fn bills_to_value(bills: &Vec<Bill>) -> Value {
    let mut new_bills: Vec<Value> = Vec::new();
    for bill in bills {
        let mut new_bill: Map<String, Value> = Map::new();
        new_bill.insert("bill_id".to_string(), Value::from(bill.bill_id));
        new_bill.insert(
            "bill_target_id".to_string(),
            Value::from(hex::encode(&bill.bill_target_id)),
        );
        new_bill.insert("owner".to_string(), Value::from(hex::encode(&bill.owner)));
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

/// The main handler for JSONRPC server.
async fn rpc_handler(body: Bytes, bill_sdk: web::Data<BillSDK>) -> Result<HttpResponse, Error> {
    let reqjson: convention::Request = match serde_json::from_slice(body.as_ref()) {
        Ok(ok) => ok,
        Err(e) => {
            let r = convention::Response {
                jsonrpc: String::from(convention::JSONRPC_VERSION),
                result: Value::Null,
                error: Some(convention::ErrorData::std(-32700)),
                id: Value::Null,
            };
            return Ok(HttpResponse::Ok()
                .content_type("application/json")
                .body(r.dump()));
        }
    };
    let mut result = convention::Response {
        id: reqjson.id.clone(),
        ..convention::Response::default()
    };
    match rpc_select(&bill_sdk, reqjson.method.as_str(), reqjson.params).await {
        Ok(ok) => result.result = ok,
        Err(e) => result.error = Some(e),
    }
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(result.dump()))
}

async fn rpc_select(
    bill_sdk: &BillSDK,
    method: &str,
    params: Vec<Value>,
) -> Result<Value, convention::ErrorData> {
    match method {
        "bills" => {
            if params.len() == 0 {
                Err(convention::ErrorData::std(-32601))
            } else {
                if let Value::Number(n) = &params[0] {
                    if let Ok(bills) = bill_sdk
                        .get_bills_by_block(n.as_u64().unwrap(), 1, 100)
                        .await
                    {
                        return Ok(bills_to_value(&bills));
                    }
                }
                Err(convention::ErrorData::std(-32601))
            }
        }
        _ => Err(convention::ErrorData::std(-32601)),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let ep = "http://127.0.0.1:26659";
    let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
    let channel = rpc_endpoint.connect_lazy();
    let client = Arc::new(StorageNodeClient::new(channel));
    let sdk = BillSDK::new(client);
    HttpServer::new(move || {
		let cors = Cors::default()
 			  .send_wildcard()
              .allowed_methods(vec!["GET", "POST"])
              .max_age(3600);
        App::new()
            .app_data(web::Data::new(sdk.clone()))
            .wrap(cors)
            .service(web::resource("/").route(web::post().to(rpc_handler)))
    })
    .bind(("127.0.0.1", 26660))
    .unwrap()
    .run()
    .await
}
