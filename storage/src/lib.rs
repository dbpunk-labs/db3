//
// copyright (c) 2022 db3.network author imotai <codego.me@gmail.com>
//
// licensed under the apache license, version 2.0 (the "license");
// you may not use this file except in compliance with the license.
// you may obtain a copy of the license at
//
//    http://www.apache.org/licenses/license-2.0
//
// unless required by applicable law or agreed to in writing, software
// distributed under the license is distributed on an "as is" basis,
// without warranties or conditions of any kind, either express or implied.
// see the license for the specific language governing permissions and
// limitations under the license.
//

use futures_util::StreamExt as _;
use std::io;

use actix_web::{error, Error};
use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use arrow::json;
use duckdb::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::vec::Vec;
extern crate pretty_env_logger;
use log::info;
pub type Pool = r2d2::Pool<duckdb::DuckdbConnectionManager>;
pub type Pools = Arc<Mutex<HashMap<String, Pool>>>;

pub async fn execute(pool: &Pool, query: String, req_id: String) -> Result<String, Error> {
    let pool = pool.clone();
    let conn = web::block(move || pool.get()).await.unwrap().unwrap();
    if query.starts_with("select") {
        match conn.prepare(&query) {
            Ok(mut stmt) => {
                if let Ok(res) = stmt.query_arrow([]) {
                    let rbs: Vec<arrow::record_batch::RecordBatch> = Vec::from_iter(res);
                    let buf = Vec::new();
                    let mut writer = json::ArrayWriter::new(buf);
                    writer.write_batches(&rbs).unwrap();
                    writer.finish().unwrap();
                    let buf = writer.into_inner();
                    if rbs.len() > 0 {
                        let schema_value = rbs[0].schema().to_json();
                        let output = format!(
                            "{{\"status\":0, \"msg\":\"ok\", \"schema\":{}, \"data\":{}, \"req_id\":\"{}\"}}",
                            schema_value,
                            String::from_utf8(buf).unwrap(),
                            req_id
                        );
                        Ok(output)
                    } else {
                        let output = format!(
                            "{{\"status\":0, \"msg\":\"ok\", \"schema\": {{}}, \"data\":[],\"req_id\":\"{}\"}}",
                            req_id
                        );
                        Ok(output)
                    }
                } else {
                    let e = format!(
                        "{{\"status\":1, \"msg\":\"fail to execute stmt\",\"req_id\":\"{}\"}}",
                        req_id
                    );
                    Ok(e)
                }
            }
            Err(_) => {
                let e = format!(
                    "{{\"status\":1, \"msg\":\"fail to execute stmt\",\"req_id\":\"{}\"}}",
                    req_id
                );
                Ok(e)
            }
        }
    } else {
        if let Err(e) = conn.execute_batch(&query) {
            let e = format!(
                "{{\"status\":1, \"msg\":\"fail to execute stmt\",\"req_id\":\"{}\"}}",
                req_id
            );
            Ok(e)
        } else {
            let ok = format!("{{\"status\":0, \"msg\":\"ok\",\"req_id\":\"{}\"}}", req_id);
            Ok(ok.to_string())
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct SQLInputs {
    query: String,
    account: String,
    req_id: String,
}

/// State and POST Params
async fn handle_sql(
    pools: web::Data<Pools>,
    mut payload: web::Payload,
) -> actix_web::Result<HttpResponse> {
    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk?;
        body.extend_from_slice(&chunk);
    }
    let obj = serde_json::from_slice::<SQLInputs>(&body)?;
    info!("input sql {}, account {}", obj.query, obj.account);
    let some_pool = match pools.lock() {
        Ok(mut p) => {
            if !p.contains_key(&obj.account) {
                let manager = duckdb::DuckdbConnectionManager::file(":memory:").unwrap();
                let new_pool = r2d2::Pool::builder().max_size(1).build(manager).unwrap();
                p.insert(obj.account.to_string(), new_pool.clone());
                Some(new_pool)
            } else {
                Some(p.get(&obj.account).unwrap().clone())
            }
        }
        Err(_) => None,
    };
    if let Some(pool) = some_pool {
        let content = execute(&pool, obj.query.to_string(), obj.req_id.to_string()).await;
        match content {
            Ok(c) => Ok(HttpResponse::Ok().content_type("text/plain").body(c)),
            Err(_) => {
                let e = format!(
                    "{{\"status\":1, \"msg\":\"fail to execute stmt\",\"req_id\":\"{}\"}}",
                    obj.req_id
                );
                Ok(HttpResponse::Ok().content_type("text/plain").body(e))
            }
        }
    } else {
        let e = format!(
            "{{\"status\":1, \"msg\":\"fail to execute stmt\",\"req_id\":\"{}\"}}",
            obj.req_id
        );
        Ok(HttpResponse::Ok().content_type("text/plain").body(e))
    }
}

pub async fn start_server() {
    let pools: Pools = Arc::new(Mutex::new(HashMap::new()));
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pools.clone()))
            .service(web::resource("/query").route(web::post().to(handle_sql)))
    })
    .bind(("127.0.0.1", 8080))
    .unwrap()
    .workers(2)
    .run()
    .await;
}
