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
use crate::json_rpc_impl;
use crate::node_storage::NodeStorage;
use crate::storage_node_impl::StorageNodeImpl;
use actix_cors::Cors;
use actix_web::{rt, web, App, HttpServer};
use clap::Parser;
use db3_bridge::evm_chain_watcher::{EvmChainConfig, EvmChainWatcher};
use db3_cmd::command::{DB3ClientCommand, DB3ClientContext};
use db3_crypto::db3_signer::Db3MultiSchemeSigner;
use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
use db3_proto::db3_node_proto::storage_node_server::StorageNodeServer;
use db3_sdk::mutation_sdk::MutationSDK;
use db3_sdk::store_sdk::StoreSDK;
use http::Uri;
use merkdb::Merk;
use redb::Database;
use std::boxed::Box;
use std::io::{stderr, stdout};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use tendermint_abci::ServerBuilder;
use tendermint_rpc::HttpClient;
use tonic::codegen::http::Method;
use tonic::transport::{ClientTlsConfig, Endpoint, Server};
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
    /// Start db3 network
    #[clap(name = "start")]
    Start {
        /// Bind the gprc server to this .
        #[clap(long, default_value = "127.0.0.1")]
        public_host: String,
        /// The port of grpc api
        #[clap(long, default_value = "26659")]
        public_grpc_port: u16,
        #[clap(long, default_value = "26670")]
        public_json_rpc_port: u16,
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
    },

    /// Start db3 interactive console
    #[clap(name = "console")]
    Console {
        /// the url of db3 grpc api
        #[clap(long = "url", global = true, default_value = "http://127.0.0.1:26659")]
        public_grpc_url: String,
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

    /// Run db3 bridge
    #[clap(name = "bridge")]
    Bridge {
        /// the websocket addres of evm chain
        #[clap(long)]
        evm_chain_ws: String,
        /// the evm chain id
        #[clap(long, default_value = "1")]
        evm_chain_id: u32,
        /// the roll contract address
        #[clap(long)]
        contract_address: String,
        /// the db3 storage chain grpc url
        #[clap(
            long = "db3_storage_grpc_url",
            default_value = "http://127.0.0.1:26659"
        )]
        db3_storage_grpc_url: String,
        /// the database path to store all events
        #[clap(long = "db_path", default_value = "./db")]
        db_path: String,
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
        if !db3_cmd::keystore::KeyStore::has_key() {
            db3_cmd::keystore::KeyStore::recover_keypair().unwrap();
        }
        let kp = db3_cmd::keystore::KeyStore::get_keypair().unwrap();
        let signer = Db3MultiSchemeSigner::new(kp);
        let mutation_sdk = MutationSDK::new(node.clone(), signer);
        let kp = db3_cmd::keystore::KeyStore::get_keypair().unwrap();
        let signer = Db3MultiSchemeSigner::new(kp);
        let store_sdk = StoreSDK::new(node, signer);
        DB3ClientContext {
            mutation_sdk: Some(mutation_sdk),
            store_sdk: Some(store_sdk),
        }
    }

    pub async fn execute(self) {
        match self {
            DB3Command::Bridge {
                evm_chain_ws,
                evm_chain_id,
                contract_address,
                db3_storage_grpc_url,
                db_path,
            } => {
                let path = Path::new(&db_path);
                let db = Arc::new(Database::create(&path).unwrap());
                let node_list: Vec<String> = vec![evm_chain_ws];
                let config = EvmChainConfig {
                    chain_id: evm_chain_id,
                    node_list,
                    contract_address: contract_address.to_string(),
                };
                let watcher = EvmChainWatcher::new(config, db).await.unwrap();
                watcher.start().await.unwrap();
            }

            DB3Command::Console { public_grpc_url } => {
                let ctx = Self::build_context(public_grpc_url.as_ref());
                db3_cmd::console::start_console(ctx, &mut stdout(), &mut stderr())
                    .await
                    .unwrap();
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
                public_json_rpc_port,
                abci_port,
                tendermint_port,
                read_buf_size,
                verbose,
                quiet,
                db_path,
                db_tree_level_in_memory,
                disable_grpc_web,
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
                info!("db3 json rpc server will connect to tendermint {tm_addr}");
                let client = HttpClient::new(tm_addr.as_str()).unwrap();
                let context = Context {
                    node_store: node_store.clone(),
                    client,
                };
                let json_rpc_handler = Self::start_json_rpc_service(
                    &public_host,
                    public_json_rpc_port,
                    context.clone(),
                );
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
                        json_rpc_handler.join().unwrap();
                        break;
                    }
                }
            }
        }
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
        let storage_node = StorageNodeImpl::new(context, signer);
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
    }

    ///
    /// Start JSON RPC Service
    ///
    fn start_json_rpc_service(
        public_host: &str,
        public_json_rpc_port: u16,
        context: Context,
    ) -> JoinHandle<()> {
        let local_public_host = public_host.to_string();
        let addr = format!("{local_public_host}:{public_json_rpc_port}");
        info!("start json rpc server with addr {}", addr.as_str());
        let handler = thread::spawn(move || {
            rt::System::new()
                .block_on(async {
                    HttpServer::new(move || {
                        let cors = Cors::default()
                            .allow_any_origin()
                            .allow_any_method()
                            .allow_any_header()
                            .max_age(3600);
                        App::new()
                            .app_data(web::Data::new(context.clone()))
                            .wrap(cors)
                            .service(
                                web::resource("/").route(web::post().to(json_rpc_impl::rpc_router)),
                            )
                    })
                    .disable_signals()
                    .bind((local_public_host, public_json_rpc_port))
                    .unwrap()
                    .run()
                    .await
                })
                .unwrap();
        });
        handler
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
        let cmd = DB3ClientCommand::NewDB {};
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
                r#"{"id":1,"name":"idx1","fields":[{"field_path":"name","value_mode":{"Order":1}}]}"#.to_string(),
                r#"{"id":2,"name":"idx2","fields":[{"field_path":"age","value_mode":{"Order":1}}]}"#.to_string(),
            ]
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
            index_list: vec![r#"{"id":1,"name":"idx1","fields":[{"field_path":"name","value_mode":{"Order":1}}]}"#.to_string()]
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

        // run show document limit 2
        let mut doc_id1 = String::new();
        let mut doc_id2 = String::new();
        let mut doc_id3 = String::new();
        let mut doc_id4 = String::new();
        let cmd = DB3ClientCommand::ShowDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            filter: "".to_string(),
            limit: -1,
        };
        let table = cmd.execute(&mut ctx).await.unwrap();
        assert_eq!(4, table.len());
        doc_id1 = table.get_row(0).unwrap().get_cell(0).unwrap().get_content();
        doc_id2 = table.get_row(1).unwrap().get_cell(0).unwrap().get_content();
        doc_id3 = table.get_row(2).unwrap().get_cell(0).unwrap().get_content();
        doc_id4 = table.get_row(3).unwrap().get_cell(0).unwrap().get_content();

        // run show document limit 2
        let cmd = DB3ClientCommand::ShowDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            filter: "".to_string(),
            limit: 3,
        };
        assert_eq!(3, cmd.execute(&mut ctx).await.unwrap().len());

        // run show document --filter = '{"field": "name", "value": "Bill", "op": "=="}'
        let cmd = DB3ClientCommand::ShowDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            filter: r#"{"field": "name", "value": "Bill", "op": "=="}"#.to_string(),
            limit: -1,
        };
        if let Ok(table) = cmd.execute(&mut ctx).await {
            assert_eq!(2, table.len());
            assert!(table
                .get_row(0)
                .unwrap()
                .get_cell(2)
                .unwrap()
                .get_content()
                .contains(r#""name": String("Bill")"#));
            assert!(table
                .get_row(1)
                .unwrap()
                .get_cell(2)
                .unwrap()
                .get_content()
                .contains(r#""name": String("Bill")"#));
        } else {
            assert!(false)
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

        // update documents
        let cmd = DB3ClientCommand::UpdateDocument {
            addr: addr.clone(),
            collection_name: collection_books.to_string(),
            ids: vec![doc_id3.clone(), doc_id4.clone()],
            documents: vec![
                r#"{"name": "Jack","age": 44,"phones": ["+1234567","+2345678"]}"#.to_string(),
                r#"{"name": "Bill","age": 46,"phones": ["+1234567","+2345678"]}"#.to_string(),
            ],
        };
        assert!(cmd.execute(&mut ctx).await.is_ok());
        std::thread::sleep(time::Duration::from_millis(2000));

        let cmd = DB3ClientCommand::GetDocument {
            id: doc_id3.clone(),
        };

        let table = cmd.execute(&mut ctx).await.unwrap();
        assert!(table
            .get_row(0)
            .unwrap()
            .get_cell(2)
            .unwrap()
            .get_content()
            .contains(r#""name": String("Jack")"#));

        let cmd = DB3ClientCommand::GetDocument {
            id: doc_id4.clone(),
        };

        let table = cmd.execute(&mut ctx).await.unwrap();
        assert!(table
            .get_row(0)
            .unwrap()
            .get_cell(2)
            .unwrap()
            .get_content()
            .contains(r#""age": Int64(46)"#));
    }
}
