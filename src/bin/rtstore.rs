//
//
// rtstore.rs
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
//
#[macro_use(uselog)]
extern crate uselog_rs;
use rtstore::meta::meta_server::MetaServiceImpl;
use rtstore::proto::rtstore_meta_proto::meta_server::MetaServer;
use tonic::transport::Server;
extern crate pretty_env_logger;
uselog!(debug, info, warn);

fn setup_log() {
    pretty_env_logger::init();
}
async fn start_metaserver() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:9527".parse().unwrap();
    let meta_service = MetaServiceImpl::new();
    info!("start metaserver on port 9527");
    Server::builder()
        .add_service(MetaServer::new(meta_service))
        .serve(addr)
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_log();
    start_metaserver().await
}
