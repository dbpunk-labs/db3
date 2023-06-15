//
// command.rs
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

use crate::abci_impl::AbciImpl;
use crate::auth_storage::AuthStorage;
use crate::context::Context;
use crate::indexer_impl::IndexerNodeImpl;
use crate::node_storage::NodeStorage;
use crate::rollup_executor::RollupExecutorConfig;
use crate::storage_node_impl::StorageNodeImpl;
use crate::storage_node_light_impl::{StorageNodeV2Config, StorageNodeV2Impl};
use clap::Parser;
use db3_cmd::command::{DB3ClientCommand, DB3ClientContext, DB3ClientContextV2};
use db3_crypto::db3_address::DB3Address;
use db3_crypto::db3_signer::Db3MultiSchemeSigner;
use db3_proto::db3_event_proto::{EventMessage, Subscription};
use db3_proto::db3_indexer_proto::indexer_node_server::IndexerNodeServer;
use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
use db3_proto::db3_node_proto::storage_node_server::StorageNodeServer;
use db3_proto::db3_storage_proto::storage_node_client::StorageNodeClient as StorageNodeV2Client;
use db3_proto::db3_storage_proto::storage_node_server::StorageNodeServer as StorageNodeV2Server;
use db3_proto::db3_storage_proto::{
    EventMessage as EventMessageV2, Subscription as SubscriptionV2,
};
use db3_sdk::mutation_sdk::MutationSDK;
use db3_sdk::store_sdk::StoreSDK;
use db3_sdk::store_sdk_v2::StoreSDKV2;
use db3_storage::db_store_v2::DBStoreV2Config;
use db3_storage::doc_store::DocStoreConfig;
use db3_storage::mutation_store::MutationStoreConfig;
use db3_storage::state_store::StateStoreConfig;
use http::Uri;
use merkdb::Merk;
use std::boxed::Box;
use std::io::{stderr, stdout};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tendermint_abci::ServerBuilder;
use tendermint_rpc::HttpClient;
use tokio::sync::mpsc::Sender;
use tonic::codegen::http::Method;
use tonic::transport::{ClientTlsConfig, Endpoint, Server};
use tonic::Status;
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};
use tracing_subscriber::filter::LevelFilter;

const ABOUT: &str = "
██████╗ ██████╗ ██████╗ 
██╔══██╗██╔══██╗╚════██╗
██║  ██║██████╔╝ █████╔╝
██║  ██║██╔══██╗ ╚═══██╗
██████╔╝██████╔╝██████╔╝
╚═════╝ ╚═════╝ ╚═════╝ 
@db3.network🚀🚀🚀";

#[derive(Debug, Parser)]
#[clap(name = "db3")]
#[clap(about = ABOUT, long_about = None)]
pub enum DB3Command {
    /// Start the store node
    #[clap(name = "store")]
    Store {
        /// Bind the gprc server to this .
        #[clap(long, default_value = "127.0.0.1")]
        public_host: String,
        /// The port of grpc api
        #[clap(long, default_value = "26619")]
        public_grpc_port: u16,
        /// Log more logs
        #[clap(short, long)]
        verbose: bool,
        /// The database path for mutation
        #[clap(long, default_value = "./mutation_db")]
        mutation_db_path: String,
        /// The database path for state
        #[clap(long, default_value = "./state_db")]
        state_db_path: String,
        /// The database path for doc db
        #[clap(long, default_value = "./doc_db")]
        doc_db_path: String,
        /// The network id
        #[clap(long, default_value = "10")]
        network_id: u64,
        /// The block interval
        #[clap(long, default_value = "2000")]
        block_interval: u64,
        /// The interval of rollup
        #[clap(long, default_value = "60000")]
        rollup_interval: u64,
        /// The min data byte size for rollup
        #[clap(long, default_value = "102400")]
        rollup_min_data_size: u64,
        /// The data path of rollup
        #[clap(long, default_value = "./rollup_data")]
        rollup_data_path: String,
        /// The Ar miner node
        #[clap(long, default_value = "http://127.0.0.1:1984/")]
        ar_node_url: String,
        /// The Ar wallet path
        #[clap(long, default_value = "./wallet.json")]
        ar_key_path: String,
        /// The min gc round offset
        #[clap(long, default_value = "8")]
        min_gc_round_offset: u64,
    },

    /// Start db3 network
    #[clap(name = "start")]
    Start {
        /// Bind the gprc server to this .
        #[clap(long, default_value = "127.0.0.1")]
        public_host: String,
        /// The port of grpc api
        #[clap(long, default_value = "26659")]
        public_grpc_port: u16,
        /// Bind the abci server to this port.
        #[clap(long, default_value = "26658")]
        abci_port: u16,
        /// The porf of tendemint
        #[clap(long, default_value = "26657")]
        tendermint_port: u16,
        /// The default server read buffer size, in bytes, for each incoming client
        /// connection.
        #[clap(short, long, default_value = "1048576")]
        read_buf_size: usize,
        /// Increase output logging verbosity to DEBUG level.
        #[clap(short, long)]
        verbose: bool,
        /// Suppress all output logging (overrides --verbose).
        #[clap(short, long)]
        quiet: bool,
        #[clap(short, long, default_value = "./db")]
        db_path: String,
        #[clap(long, default_value = "16")]
        db_tree_level_in_memory: u8,
        /// disable grpc-web
        #[clap(long, default_value = "false")]
        disable_grpc_web: bool,
        /// disable query session
        /// the node will be free if you disable the query session
        #[clap(long, default_value = "false")]
        disable_query_session: bool,
    },

    /// Start db3 interactive console
    #[clap(name = "console")]
    Console {
        /// the url of db3 grpc api
        #[clap(long = "url", global = true, default_value = "http://127.0.0.1:26659")]
        public_grpc_url: String,
    },

    /// Start db3 indexer
    #[clap(name = "indexer")]
    Indexer {
        /// Bind the gprc server to this .
        #[clap(long, default_value = "127.0.0.1")]
        public_host: String,
        /// The port of grpc api
        #[clap(long, default_value = "26639")]
        public_grpc_port: u16,
        /// the store grpc url
        #[clap(
            long = "db3_storage_grpc_url",
            default_value = "http://127.0.0.1:26619"
        )]
        db3_storage_grpc_url: String,
        #[clap(short, long, default_value = "./index_meta_db")]
        meta_db_path: String,
        #[clap(short, long, default_value = "./index_doc_db")]
        doc_db_path: String,
        #[clap(long, default_value = "10")]
        network_id: u64,
        #[clap(short, long)]
        verbose: bool,
    },

    /// Run db3 client
    #[clap(name = "client")]
    Client {
        /// the url of db3 grpc api
        #[clap(long = "url", global = true, default_value = "http://127.0.0.1:26659")]
        public_grpc_url: String,
        /// the subcommand
        #[clap(subcommand)]
        cmd: Option<DB3ClientCommand>,
    },
}

impl DB3Command {
    fn build_context(public_grpc_url: &str) -> DB3ClientContext {
        let uri = public_grpc_url.parse::<Uri>().unwrap();
        let endpoint = match uri.scheme_str() == Some("https") {
            true => {
                let rpc_endpoint = Endpoint::new(public_grpc_url.to_string())
                    .unwrap()
                    .tls_config(ClientTlsConfig::new())
                    .unwrap();
                rpc_endpoint
            }
            false => {
                let rpc_endpoint = Endpoint::new(public_grpc_url.to_string()).unwrap();
                rpc_endpoint
            }
        };
        let channel = endpoint.connect_lazy();
        let node = Arc::new(StorageNodeClient::new(channel));
        if !db3_cmd::keystore::KeyStore::has_key(None) {
            db3_cmd::keystore::KeyStore::recover_keypair(None).unwrap();
        }
        let kp = db3_cmd::keystore::KeyStore::get_keypair(None).unwrap();
        let signer = Db3MultiSchemeSigner::new(kp);
        let mutation_sdk = MutationSDK::new(node.clone(), signer, true);
        let kp = db3_cmd::keystore::KeyStore::get_keypair(None).unwrap();
        let signer = Db3MultiSchemeSigner::new(kp);
        let store_sdk = StoreSDK::new(node, signer, true);
        DB3ClientContext {
            mutation_sdk: Some(mutation_sdk),
            store_sdk: Some(store_sdk),
        }
    }
    fn build_context_v2(public_grpc_url: &str) -> DB3ClientContextV2 {
        let uri = public_grpc_url.parse::<Uri>().unwrap();
        let endpoint = match uri.scheme_str() == Some("https") {
            true => {
                let rpc_endpoint = Endpoint::new(public_grpc_url.to_string())
                    .unwrap()
                    .tls_config(ClientTlsConfig::new())
                    .unwrap();
                rpc_endpoint
            }
            false => {
                let rpc_endpoint = Endpoint::new(public_grpc_url.to_string()).unwrap();
                rpc_endpoint
            }
        };
        let channel = endpoint.connect_lazy();
        let node = Arc::new(StorageNodeV2Client::new(channel));
        if !db3_cmd::keystore::KeyStore::has_key(None) {
            db3_cmd::keystore::KeyStore::recover_keypair(None).unwrap();
        }
        let kp = db3_cmd::keystore::KeyStore::get_keypair(None).unwrap();
        let signer = Db3MultiSchemeSigner::new(kp);
        let store_sdk = StoreSDKV2::new(node, signer);
        DB3ClientContextV2 {
            store_sdk: Some(store_sdk),
        }
    }

    pub async fn execute(self) {
        match self {
            DB3Command::Store {
                public_host,
                public_grpc_port,
                verbose,
                mutation_db_path,
                state_db_path,
                doc_db_path,
                network_id,
                block_interval,
                rollup_interval,
                rollup_min_data_size,
                rollup_data_path,
                ar_node_url,
                ar_key_path,
                min_gc_round_offset,
            } => {
                let log_level = if verbose {
                    LevelFilter::DEBUG
                } else {
                    LevelFilter::INFO
                };
                tracing_subscriber::fmt().with_max_level(log_level).init();
                info!("{ABOUT}");
                Self::start_store_grpc_service(
                    public_host.as_str(),
                    public_grpc_port,
                    mutation_db_path.as_str(),
                    state_db_path.as_str(),
                    doc_db_path.as_str(),
                    network_id,
                    block_interval,
                    rollup_interval,
                    rollup_min_data_size,
                    rollup_data_path.as_str(),
                    ar_node_url.as_str(),
                    ar_key_path.as_str(),
                    min_gc_round_offset,
                )
                .await;
                let running = Arc::new(AtomicBool::new(true));
                let r = running.clone();
                ctrlc::set_handler(move || {
                    r.store(false, Ordering::SeqCst);
                })
                .expect("Error setting Ctrl-C handler");
                loop {
                    if running.load(Ordering::SeqCst) {
                        let ten_millis = Duration::from_millis(10);
                        thread::sleep(ten_millis);
                    } else {
                        info!("stop db3 store node...");
                        break;
                    }
                }
            }

            DB3Command::Console { public_grpc_url } => {
                let ctx = Self::build_context(public_grpc_url.as_ref());
                db3_cmd::console::start_console(ctx, &mut stdout(), &mut stderr())
                    .await
                    .unwrap();
            }

            DB3Command::Indexer {
                public_host,
                public_grpc_port,
                db3_storage_grpc_url,
                meta_db_path,
                doc_db_path,
                network_id,
                verbose,
            } => {
                let log_level = if verbose {
                    LevelFilter::DEBUG
                } else {
                    LevelFilter::INFO
                };

                tracing_subscriber::fmt().with_max_level(log_level).init();
                info!("{ABOUT}");

                let ctx = Self::build_context_v2(db3_storage_grpc_url.as_ref());

                let doc_store_conf = DocStoreConfig {
                    db_root_path: doc_db_path,
                    in_memory_db_handle_limit: 16,
                };
                let db_store_config = DBStoreV2Config {
                    db_path: meta_db_path.to_string(),
                    db_store_cf_name: "db_store_cf".to_string(),
                    doc_store_cf_name: "doc_store_cf".to_string(),
                    collection_store_cf_name: "col_store_cf".to_string(),
                    index_store_cf_name: "idx_store_cf".to_string(),
                    doc_owner_store_cf_name: "doc_owner_store_cf".to_string(),
                    db_owner_store_cf_name: "db_owner_cf".to_string(),
                    scan_max_limit: 1000,
                    enable_doc_store: true,
                    doc_store_conf,
                };

                let indexer = IndexerNodeImpl::new(db_store_config, network_id).unwrap();
                let addr = format!("{public_host}:{public_grpc_port}");
                let indexer_for_syncing = indexer.clone();
                let listen = tokio::spawn(async move {
                    info!("start syncing data from storage node");
                    indexer_for_syncing
                        .start(ctx.store_sdk.unwrap())
                        .await
                        .unwrap();
                });
                info!("start db3 indexer node on public addr {}", addr);
                let cors_layer = CorsLayer::new()
                    .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                    .allow_headers(Any)
                    .allow_origin(Any);
                Server::builder()
                    .accept_http1(true)
                    .layer(cors_layer)
                    .layer(tonic_web::GrpcWebLayer::new())
                    .add_service(IndexerNodeServer::new(indexer))
                    .serve(addr.parse().unwrap())
                    .await
                    .unwrap();
                let (r1,) = tokio::join!(listen);
                r1.unwrap();
                info!("exit standalone indexer")
            }

            DB3Command::Client {
                cmd,
                public_grpc_url,
            } => {
                let mut ctx = Self::build_context(public_grpc_url.as_ref());
                if let Some(c) = cmd {
                    match c.execute(&mut ctx).await {
                        Ok(table) => table.printstd(),
                        Err(e) => println!("{}", e),
                    }
                }
            }
            DB3Command::Start {
                public_host,
                public_grpc_port,
                abci_port,
                tendermint_port,
                read_buf_size,
                verbose,
                quiet,
                db_path,
                db_tree_level_in_memory,
                disable_grpc_web,
                disable_query_session,
            } => {
                let log_level = if quiet {
                    LevelFilter::OFF
                } else if verbose {
                    LevelFilter::DEBUG
                } else {
                    LevelFilter::INFO
                };
                tracing_subscriber::fmt().with_max_level(log_level).init();
                info!("{ABOUT}");
                let opts = Merk::default_db_opts();
                let merk = Merk::open_opt(&db_path, opts, db_tree_level_in_memory).unwrap();
                let node_store = Arc::new(Mutex::new(Box::pin(NodeStorage::new(
                    AuthStorage::new(merk),
                ))));
                match node_store.lock() {
                    Ok(mut store) => {
                        if store.get_auth_store().init().is_err() {
                            warn!("Fail to init auth storage!");
                            return;
                        }
                    }
                    _ => todo!(),
                }
                let abci_handler =
                    Self::start_abci_service(abci_port, read_buf_size, node_store.clone());
                let tm_addr = format!("http://127.0.0.1:{tendermint_port}");
                let ws_tm_addr = format!("ws://127.0.0.1:{tendermint_port}/websocket");
                let client = HttpClient::new(tm_addr.as_str()).unwrap();
                let context = Context {
                    node_store: node_store.clone(),
                    client,
                    ws_url: ws_tm_addr,
                    disable_query_session,
                };
                Self::start_grpc_service(&public_host, public_grpc_port, disable_grpc_web, context)
                    .await;
                let running = Arc::new(AtomicBool::new(true));
                let r = running.clone();
                ctrlc::set_handler(move || {
                    r.store(false, Ordering::SeqCst);
                })
                .expect("Error setting Ctrl-C handler");
                loop {
                    if running.load(Ordering::SeqCst) {
                        let ten_millis = Duration::from_millis(10);
                        thread::sleep(ten_millis);
                    } else {
                        info!("stop db3...");
                        abci_handler.join().unwrap();
                        break;
                    }
                }
            }
        }
    }
    /// Start store grpc service
    async fn start_store_grpc_service(
        public_host: &str,
        public_grpc_port: u16,
        mutation_db_path: &str,
        state_db_path: &str,
        doc_db_path: &str,
        network_id: u64,
        block_interval: u64,
        rollup_interval: u64,
        rollup_min_data_size: u64,
        rollup_data_path: &str,
        ar_node_url: &str,
        ar_key_path: &str,
        min_gc_round_offset: u64,
    ) {
        let addr = format!("{public_host}:{public_grpc_port}");
        let rollup_config = RollupExecutorConfig {
            rollup_interval,
            temp_data_path: rollup_data_path.to_string(),
            ar_node_url: ar_node_url.to_string(),
            ar_key_path: ar_key_path.to_string(),
            min_rollup_size: rollup_min_data_size,
            min_gc_round_offset,
        };
        let store_config = MutationStoreConfig {
            db_path: mutation_db_path.to_string(),
            block_store_cf_name: "block_store_cf".to_string(),
            tx_store_cf_name: "tx_store_cf".to_string(),
            rollup_store_cf_name: "rollup_store_cf".to_string(),
            gc_cf_name: "gc_store_cf".to_string(),
            message_max_buffer: 4 * 1024,
            scan_max_limit: 50,
            block_state_cf_name: "block_state_cf".to_string(),
        };
        let state_config = StateStoreConfig {
            db_path: state_db_path.to_string(),
        };
        let db_store_config = DBStoreV2Config {
            db_path: doc_db_path.to_string(),
            db_store_cf_name: "db_store_cf".to_string(),
            doc_store_cf_name: "doc_store_cf".to_string(),
            collection_store_cf_name: "col_store_cf".to_string(),
            index_store_cf_name: "idx_store_cf".to_string(),
            doc_owner_store_cf_name: "doc_owner_store_cf".to_string(),
            db_owner_store_cf_name: "db_owner_cf".to_string(),
            scan_max_limit: 1000,
            enable_doc_store: false,
            doc_store_conf: DocStoreConfig::default(),
        };

        let (sender, receiver) = tokio::sync::mpsc::channel::<(
            DB3Address,
            SubscriptionV2,
            Sender<std::result::Result<EventMessageV2, Status>>,
        )>(1024);
        let config = StorageNodeV2Config {
            store_config,
            state_config,
            rollup_config,
            db_store_config,
            network_id,
            block_interval,
        };
        let storage_node = StorageNodeV2Impl::new(config, sender).unwrap();
        info!(
            "start db3 store node on public addr {} and network {}",
            addr, network_id
        );
        std::fs::create_dir_all(rollup_data_path).unwrap();
        storage_node.keep_subscription(receiver).await.unwrap();
        storage_node.start_to_produce_block().await;
        storage_node.start_to_rollup().await;
        let cors_layer = CorsLayer::new()
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers(Any)
            .allow_origin(Any);
        Server::builder()
            .accept_http1(true)
            .layer(cors_layer)
            .layer(tonic_web::GrpcWebLayer::new())
            .add_service(StorageNodeV2Server::new(storage_node))
            .serve(addr.parse().unwrap())
            .await
            .unwrap();
    }

    /// Start GRPC Service
    async fn start_grpc_service(
        public_host: &str,
        public_grpc_port: u16,
        disable_grpc_web: bool,
        context: Context,
    ) {
        let addr = format!("{public_host}:{public_grpc_port}");
        let kp = crate::node_key::get_key_pair(None).unwrap();
        let signer = Db3MultiSchemeSigner::new(kp);
        // config it
        let (sender, receiver) = tokio::sync::mpsc::channel::<(
            DB3Address,
            Subscription,
            Sender<std::result::Result<EventMessage, Status>>,
        )>(1024);
        let storage_node = StorageNodeImpl::new(context, signer, sender);
        storage_node.keep_subscription(receiver).await.unwrap();
        info!("start db3 storage node on public addr {}", addr);
        if disable_grpc_web {
            Server::builder()
                .add_service(StorageNodeServer::new(storage_node))
                .serve(addr.parse().unwrap())
                .await
                .unwrap();
        } else {
            let cors_layer = CorsLayer::new()
                .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                .allow_headers(Any)
                .allow_origin(Any);
            Server::builder()
                .accept_http1(true)
                .layer(cors_layer)
                .layer(tonic_web::GrpcWebLayer::new())
                .add_service(StorageNodeServer::new(storage_node))
                .serve(addr.parse().unwrap())
                .await
                .unwrap();
        }
        info!("db3 storage node exit");
    }

    ///
    /// Start ABCI service
    ///
    fn start_abci_service(
        abci_port: u16,
        read_buf_size: usize,
        store: Arc<Mutex<Pin<Box<NodeStorage>>>>,
    ) -> JoinHandle<()> {
        let addr = format!("{}:{}", "127.0.0.1", abci_port);
        let abci_impl = AbciImpl::new(store);
        let handler = thread::spawn(move || {
            let server = ServerBuilder::new(read_buf_size).bind(addr, abci_impl);
            match server {
                Ok(s) => {
                    if let Err(e) = s.listen() {
                        warn!("fail to listen addr for error {}", e);
                    }
                }
                Err(e) => {
                    warn!("fail to bind addr for error {}", e);
                }
            }
        });
        handler
    }
}

#[cfg(test)]
mod tests {
    use crate::command::DB3Command;
    use db3_cmd::command::DB3ClientCommand;
    use std::time;

    #[tokio::test]
    async fn client_cmd_smoke_test() {
        let mut ctx = DB3Command::build_context("http://127.0.0.1:26659");
        let cmd = DB3ClientCommand::Init {};
        if let Ok(_) = cmd.execute(&mut ctx).await {
        } else {
            assert!(false);
        }

        let cmd = DB3ClientCommand::ShowKey {};
        if let Ok(_) = cmd.execute(&mut ctx).await {
        } else {
            assert!(false);
        }

        let mut addr = String::new();
        let cmd = DB3ClientCommand::NewDB {
            desc: "".to_string(),
        };
        if let Ok(table) = cmd.execute(&mut ctx).await {
            assert_eq!(1, table.len());
            addr = table.get_row(0).unwrap().get_cell(0).unwrap().get_content();
        } else {
            assert!(false)
        }
        let collection_books = "test_books";
        let cmd = DB3ClientCommand::NewCollection {
            addr: addr.clone(),
            name: collection_books.to_string(),
            index_list: vec![
                r#"{"name":"idx1","fields":[{"field_path":"name","value_mode":{"Order":1}}]}"#
                    .to_string(),
                r#"{"name":"idx2","fields":[{"field_path":"age","value_mode":{"Order":1}}]}"#
                    .to_string(),
                r#"{"name":"idx_name_age","fields":[
                    {"field_path":"name","value_mode":{"Order":1}},
                    {"field_path":"age","value_mode":{"Order":1}}
                ]}"#
                .to_string(),
            ],
        };

        if let Ok(_) = cmd.execute(&mut ctx).await {
        } else {
            assert!(false)
        }

        std::thread::sleep(time::Duration::from_millis(2000));

        let collection_student = "test_student";
        let cmd = DB3ClientCommand::NewCollection {
            addr: addr.clone(),
            name: collection_student.to_string(),
            index_list: vec![
                r#"{"name":"idx1","fields":[{"field_path":"name","value_mode":{"Order":1}}]}"#
                    .to_string(),
            ],
        };

        if let Ok(_) = cmd.execute(&mut ctx).await {
        } else {
            assert!(false)
        }

        std::thread::sleep(time::Duration::from_millis(2000));

        let cmd = DB3ClientCommand::ShowCollection { addr: addr.clone() };
        if let Ok(table) = cmd.execute(&mut ctx).await {
            assert_eq!(2, table.len());
        } else {
            assert!(false)
        }

        let cmd = DB3ClientCommand::NewDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            documents: vec![
                r#"{"name": "John Doe","age": 43,"phones": ["+44 1234567","+44 2345678"]}"#
                    .to_string(),
            ],
        };
        if let Ok(table) = cmd.execute(&mut ctx).await {
            assert_eq!(1, table.len());
        } else {
            assert!(false)
        }
        std::thread::sleep(time::Duration::from_millis(2000));

        // add 3 documents
        let cmd = DB3ClientCommand::NewDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            documents: vec![
                r#"{"name": "Mike","age": 44,"phones": ["+44 1234567","+44 2345678"]}"#.to_string(),
                r#"{"name": "Bill","age": 44,"phones": ["+44 1234567","+44 2345678"]}"#.to_string(),
                r#"{"name": "Bill","age": 45,"phones": ["+44 1234567","+44 2345678"]}"#.to_string(),
            ],
        };
        assert!(cmd.execute(&mut ctx).await.is_ok());
        std::thread::sleep(time::Duration::from_millis(2000));

        // run show document no limit
        // r#"{"name": "John Doe","age": 43,"phones": ["+44 1234567","+44 2345678"]}"#
        // r#"{"name": "Mike","age": 44,"phones": ["+44 1234567","+44 2345678"]}"#.to_string(),
        // r#"{"name": "Bill","age": 44,"phones": ["+44 1234567","+44 2345678"]}"#.to_string(),
        // r#"{"name": "Bill","age": 45,"phones": ["+44 1234567","+44 2345678"]}"#.to_string(),
        let cmd = DB3ClientCommand::ShowDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            filter: "".to_string(),
            limit: -1,
        };
        let table = cmd.execute(&mut ctx).await.unwrap();
        assert_eq!(4, table.len());
        let doc_id2 = table.get_row(1).unwrap().get_cell(0).unwrap().get_content();
        let doc_id3 = table.get_row(2).unwrap().get_cell(0).unwrap().get_content();
        let doc_id4 = table.get_row(3).unwrap().get_cell(0).unwrap().get_content();

        // run show document limit 3
        let cmd = DB3ClientCommand::ShowDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            filter: "".to_string(),
            limit: 3,
        };
        assert_eq!(3, cmd.execute(&mut ctx).await.unwrap().len());

        // run show document --filter = '{"field": "name", "value": "Bill", "op": "=="}'

        for (filter, exp) in [
            (
                r#"{"field": "name", "value": "Bill", "op": "=="}"#,
                vec![r#""name": String("Bill")"#, r#""name": String("Bill")"#],
            ),
            (
                r#"{"field": "name", "value": "John Doe", "op": "<"}"#,
                vec![r#""name": String("Bill")"#, r#""name": String("Bill")"#],
            ),
            (
                r#"{"field": "name", "value": "John Doe", "op": "<="}"#,
                vec![
                    r#""name": String("Bill")"#,
                    r#""name": String("Bill")"#,
                    r#""name": String("John Doe")"#,
                ],
            ),
            (
                r#"{"field": "name", "value": "John Doe", "op": ">="}"#,
                vec![r#""name": String("John Doe")"#, r#""name": String("Mike")"#],
            ),
            (
                r#"{"field": "name", "value": "John Doe", "op": ">"}"#,
                vec![r#""name": String("Mike")"#],
            ),
            (
                r#"{"and":
                [
                    {"field": "name", "value": "Bill", "op": "=="},
                    {"field": "age", "value": 44, "op": "=="}
                ]}"#,
                vec![r#""name": String("Bill")"#],
            ),
            (
                r#"{"and":
                [
                    {"field": "name", "value": "Bill", "op": "=="},
                    {"field": "age", "value": 46, "op": "=="}
                ]}"#,
                vec![],
            ),
        ] {
            let cmd = DB3ClientCommand::ShowDocument {
                addr: addr.clone(),
                collection_name: collection_books.to_string(),
                filter: filter.to_string(),
                limit: -1,
            };
            let res = cmd.execute(&mut ctx).await;
            if let Ok(table) = res {
                assert_eq!(exp.len(), table.len());
                for i in 0..exp.len() {
                    assert!(
                        table
                            .get_row(i)
                            .unwrap()
                            .get_cell(2)
                            .unwrap()
                            .get_content()
                            .contains(exp[i]),
                        "expect contains {} but {}",
                        exp[i],
                        table.get_row(i).unwrap().get_cell(2).unwrap().get_content()
                    );
                }
            } else {
                assert!(false, "{:?}", res);
            }
        }
        // run show document --filter = '{"field": "age", "value": 44, "op": "=="}'
        let cmd = DB3ClientCommand::ShowDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            filter: r#"{"field": "age", "value": 44, "op": "=="}"#.to_string(),
            limit: -1,
        };
        let res = cmd.execute(&mut ctx).await;
        assert!(res.is_ok(), "{:?}", res);

        // r#"{"name": "Doe","age": 43,"phones": ["+44 1234567","+44 2345678"]}"#
        // r#"{"name": "Mike","age": 44,"phones": ["+44 1234567","+44 2345678"]}"#.to_string(),
        // r#"{"name": "Jack","age": 44,"phones": ["+1234567","+2345678"]}"#.to_string(),
        // r#"{"name": "Bill","age": 46,"phones": ["+1234567","+2345678"]}"#.to_string(),
        // update documents
        let cmd = DB3ClientCommand::UpdateDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            ids: vec![doc_id3.clone(), doc_id4.clone()],
            documents: vec![
                r#"{"name": "Jack"}"#.to_string(),
                r#"{"age": 46}"#.to_string(),
            ],
            masks: vec![
                r#"name, age"#.to_string(),   // update name ,remove age, keep phones
                r#"age, phones"#.to_string(), // keep name, update age, remove phones
            ],
        };
        assert!(cmd.execute(&mut ctx).await.is_ok());
        std::thread::sleep(time::Duration::from_millis(2000));

        let cmd = DB3ClientCommand::GetDocument {
            id: doc_id3.clone(),
        };

        let table = cmd.execute(&mut ctx).await.unwrap();
        table.printstd();
        assert!(table
            .get_row(0)
            .unwrap()
            .get_cell(2)
            .unwrap()
            .get_content()
            .contains(r#""name": String("Jack"), "phones": Array([String("+44 1234567"), String("+44 2345678")])"#));
        let cmd = DB3ClientCommand::GetDocument {
            id: doc_id4.clone(),
        };

        let table = cmd.execute(&mut ctx).await.unwrap();
        table.printstd();
        assert!(table
            .get_row(0)
            .unwrap()
            .get_cell(2)
            .unwrap()
            .get_content()
            .contains(r#"{"name": String("Bill"), "age": Int64(46)}"#));
        // verify document is added
        let cmd = DB3ClientCommand::GetDocument {
            id: doc_id2.to_string(),
        };
        let table = cmd.execute(&mut ctx).await.unwrap();
        assert_eq!(
            doc_id2,
            table.get_row(0).unwrap().get_cell(0).unwrap().get_content()
        );

        // verify test delete with empty ids
        let cmd = DB3ClientCommand::DeleteDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            ids: vec![],
        };
        let res = cmd.execute(&mut ctx).await;
        assert!(res.is_err(), "{:?}", res.unwrap());

        // test delete document cmd
        let cmd = DB3ClientCommand::DeleteDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            ids: vec![doc_id2.to_string()],
        };
        let table = cmd.execute(&mut ctx).await.unwrap();
        assert_eq!(1, table.len());
        std::thread::sleep(time::Duration::from_millis(2000));
        // verify document is deleted
        let cmd = DB3ClientCommand::GetDocument {
            id: doc_id2.to_string(),
        };
        let res = cmd.execute(&mut ctx).await;
        assert!(res.is_err(), "{:?}", res.unwrap());
    }
}
