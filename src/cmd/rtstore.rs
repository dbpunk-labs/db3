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
use rtstore::memory_node::memory_node_impl::{MemoryNodeConfig, MemoryNodeImpl};
use rtstore::meta_node::meta_server::MetaServiceImpl;
use rtstore::proto::rtstore_memory_proto::memory_node_server::MemoryNodeServer;
use rtstore::proto::rtstore_meta_proto::meta_client::MetaClient;
use rtstore::proto::rtstore_meta_proto::meta_server::MetaServer;
use rtstore::proto::rtstore_meta_proto::PingRequest;
use tonic::transport::Server;
extern crate pretty_env_logger;
uselog!(debug, info, warn);
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[clap(name = "rtstore")]
#[clap(about = "a table store engine for realtime ingesting and analytics", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Start Meta Node Server
    #[clap(arg_required_else_help = true)]
    Meta {
        #[clap(required = true)]
        port: i32,
    },

    /// Start Client Cli
    #[clap(arg_required_else_help = true)]
    Client {
        #[clap(required = true)]
        port: i32,
    },

    /// Start Memory Node Server
    #[clap(arg_required_else_help = true)]
    MemoryNode {
        #[clap(required = true)]
        port: i32,
        #[clap(required = true)]
        binlog_root_dir: String,
        #[clap(required = true)]
        tmp_root_dir: String,
        meta_node: String,
    },
}

fn setup_log() {
    pretty_env_logger::init_timed();
}

async fn start_memory_node(
    port: i32,
    binlog_root_dir: &str,
    tmp_root_dir: &str,
    meta_node: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("127.0.0.1:{}", port);
    let config = MemoryNodeConfig {
        binlog_root_dir: binlog_root_dir.to_string(),
        tmp_store_root_dir: tmp_root_dir.to_string(),
        meta_node_endpoint: meta_node.to_string(),
        my_endpoint: format!("http://{}", addr).to_string(),
    };
    let memory_node = MemoryNodeImpl::new(config);
    if !meta_node.is_empty() {
        if let Err(e) = memory_node.connect_meta_node().await {
            warn!("fail to connect to meta node {} with err {}", meta_node, e);
            return Ok(());
        }
    }
    info!("start memory node server on port {}", port);
    Server::builder()
        .add_service(MemoryNodeServer::new(memory_node))
        .serve(addr.parse().unwrap())
        .await?;
    Ok(())
}

async fn start_metaserver(port: i32) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("127.0.0.1:{}", port).parse().unwrap();
    let meta_service = MetaServiceImpl::new();
    info!("start metaserver on port {}", port);
    Server::builder()
        .add_service(MetaServer::new(meta_service))
        .serve(addr)
        .await?;
    Ok(())
}

async fn start_client(port: i32) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("http://127.0.0.1:{}", port);
    let mut client = MetaClient::connect(addr).await?;
    let request = tonic::Request::new(PingRequest {});
    let response = client.ping(request).await?;
    println!("{:?}", response);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_log();
    let args = Cli::parse();
    match args.command {
        Commands::Meta { port } => start_metaserver(port).await,
        Commands::Client { port } => start_client(port).await,
        Commands::MemoryNode {
            port,
            binlog_root_dir,
            tmp_root_dir,
            meta_node,
        } => start_memory_node(port, &binlog_root_dir, &tmp_root_dir, &meta_node).await,
    }
}
