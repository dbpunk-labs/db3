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

use crate::indexer_impl::IndexerNodeImpl;
use crate::recover::{Recover, RecoverConfig, RecoverType};
use crate::rollup_executor::RollupExecutorConfig;
use crate::storage_node_light_impl::{StorageNodeV2Config, StorageNodeV2Impl};
use crate::system_impl::SystemImpl;
use clap::Parser;
use db3_crypto::db3_address::DB3Address;
use db3_error::DB3Error;
use db3_proto::db3_indexer_proto::indexer_node_server::IndexerNodeServer;
use db3_proto::db3_storage_proto::storage_node_client::StorageNodeClient as StorageNodeV2Client;
use db3_proto::db3_storage_proto::storage_node_server::StorageNodeServer as StorageNodeV2Server;
use db3_proto::db3_storage_proto::{
    EventMessage as EventMessageV2, Subscription as SubscriptionV2,
};
use db3_proto::db3_system_proto::system_server::SystemServer;
use db3_sdk::store_sdk_v2::StoreSDKV2;
use db3_storage::db_store_v2::{DBStoreV2, DBStoreV2Config};
use db3_storage::doc_store::DocStoreConfig;
use db3_storage::key_store::KeyStore;
use db3_storage::key_store::KeyStoreConfig;
use db3_storage::mutation_store::{MutationStore, MutationStoreConfig};
use db3_storage::state_store::{StateStore, StateStoreConfig};
use db3_storage::system_store::{SystemRole, SystemStore, SystemStoreConfig};
use ethers::prelude::LocalWallet;
use http::Uri;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tonic::codegen::http::Method;
use tonic::transport::{ClientTlsConfig, Endpoint, Server};
use tonic::Status;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;
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
    /// Start the data rollup node
    #[clap(name = "rollup")]
    Rollup {
        /// the public address
        #[clap(long, default_value = "http://127.0.0.1:26619")]
        public_url: String,
        /// Bind the gprc server to this .
        #[clap(long, default_value = "127.0.0.1")]
        bind_host: String,
        /// The port of grpc api
        #[clap(long, default_value = "26619")]
        listening_port: u16,
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
        #[clap(long, default_value = "2000")]
        block_interval: u64,
        /// The data path of rollup
        #[clap(long, default_value = "./rollup_data")]
        rollup_data_path: String,
        /// The wallet path
        #[clap(long, default_value = "./keys")]
        key_root_path: String,
        /// the admin address which can change the configuration this node
        #[clap(long, default_value = "0x0000000000000000000000000000000000000000")]
        admin_addr: String,
        /// this is just for upgrade the node
        #[clap(long, default_value = "100000")]
        doc_id_start: i64,
        /// use the legacy transaction format
        #[clap(long, default_value = "false")]
        use_legacy_tx: bool,
    },

    /// Start the data index node
    #[clap(name = "index")]
    Index {
        /// the public address
        #[clap(long, default_value = "http://127.0.0.1:26639")]
        public_url: String,
        /// Bind the gprc server to this .
        #[clap(long, default_value = "127.0.0.1")]
        bind_host: String,
        /// The port of grpc api
        #[clap(long, default_value = "26639")]
        listening_port: u16,
        #[clap(short, long, default_value = "./index_meta_db")]
        meta_db_path: String,
        #[clap(long, default_value = "./index_state_db")]
        state_db_path: String,
        #[clap(short, long, default_value = "./index_doc_db")]
        doc_db_path: String,
        #[clap(short, long, default_value = "./keys")]
        key_root_path: String,
        #[clap(
            long = "db3_storage_grpc_url",
            default_value = "http://127.0.0.1:26619"
        )]
        db3_storage_grpc_url: String,
        #[clap(
            short,
            long,
            default_value = "0x0000000000000000000000000000000000000000"
        )]
        admin_addr: String,
        #[clap(short, long)]
        verbose: bool,
        /// this is just for upgrade the node
        #[clap(long, default_value = "100000")]
        doc_id_start: i64,
    },

    /// Recover rollup/index data
    #[clap(name = "recover")]
    Recover {
        #[clap(subcommand)]
        cmd: RecoverCommand,
    },
}

#[derive(Debug, Parser)]
#[clap(rename_all = "kebab-case")]
pub enum RecoverCommand {
    #[clap(name = "index")]
    Index {
        #[clap(short, long, default_value = "./index_meta_db")]
        meta_db_path: String,
        #[clap(long, default_value = "./index_state_db")]
        state_db_path: String,
        #[clap(short, long, default_value = "./index_doc_db")]
        doc_db_path: String,
        #[clap(short, long, default_value = "./keys")]
        key_root_path: String,
        #[clap(short, long, default_value = "./recover_index_temp")]
        recover_temp_path: String,
        #[clap(
            short,
            long,
            default_value = "0x0000000000000000000000000000000000000000"
        )]
        admin_addr: String,
        /// this is just for upgrade the node
        #[clap(long, default_value = "100000")]
        doc_id_start: i64,
        #[clap(short, long)]
        verbose: bool,
    },
    // TODO: support recover rollup
    #[clap(name = "rollup")]
    Rollup {
        /// The database path for mutation
        #[clap(long, default_value = "./mutation_db")]
        mutation_db_path: String,
        /// The database path for state
        #[clap(long, default_value = "./state_db")]
        state_db_path: String,
        /// The database path for doc db
        #[clap(long, default_value = "./doc_db")]
        doc_db_path: String,
        #[clap(short, long, default_value = "./rollup_meta_db")]
        meta_db_path: String,
        #[clap(short, long, default_value = "./keys")]
        key_root_path: String,
        #[clap(short, long, default_value = "./recover_rollup_temp")]
        recover_temp_path: String,
        #[clap(
            short,
            long,
            default_value = "0x0000000000000000000000000000000000000000"
        )]
        admin_addr: String,
        /// this is just for upgrade the node
        #[clap(long, default_value = "100000")]
        doc_id_start: i64,
        #[clap(short, long)]
        verbose: bool,
    },
}
impl DB3Command {
    fn build_wallet(key_root_path: &str) -> std::result::Result<LocalWallet, DB3Error> {
        let config = KeyStoreConfig {
            key_root_path: key_root_path.to_string(),
        };
        let key_store = KeyStore::new(config);
        match key_store.has_key("evm") {
            true => {
                let data = key_store.get_key("evm")?;
                let data_ref: &[u8] = &data;
                let wallet = LocalWallet::from_bytes(data_ref)
                    .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
                Ok(wallet)
            }
            false => {
                let mut rng = rand::thread_rng();
                let wallet = LocalWallet::new(&mut rng);
                let data = wallet.signer().to_bytes();
                key_store.write_key("evm", data.deref())?;
                Ok(wallet)
            }
        }
    }

    fn build_store_sdk(public_grpc_url: &str, key_root_path: &str) -> StoreSDKV2 {
        let wallet = Self::build_wallet(key_root_path).unwrap();
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
        StoreSDKV2::new(node, wallet)
    }

    pub async fn execute(self) {
        match self {
            DB3Command::Rollup {
                public_url,
                bind_host,
                listening_port,
                verbose,
                mutation_db_path,
                state_db_path,
                doc_db_path,
                block_interval,
                rollup_data_path,
                key_root_path,
                admin_addr,
                doc_id_start,
                use_legacy_tx,
            } => {
                let log_level = if verbose {
                    LevelFilter::DEBUG
                } else {
                    LevelFilter::INFO
                };
                tracing_subscriber::fmt().with_max_level(log_level).init();
                info!("{ABOUT}");
                Self::start_rollup_grpc_service(
                    public_url.as_str(),
                    bind_host.as_str(),
                    listening_port,
                    mutation_db_path.as_str(),
                    state_db_path.as_str(),
                    doc_db_path.as_str(),
                    block_interval,
                    rollup_data_path.as_str(),
                    key_root_path.as_str(),
                    admin_addr.as_str(),
                    doc_id_start,
                    use_legacy_tx,
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

            DB3Command::Index {
                public_url,
                bind_host,
                listening_port,
                meta_db_path,
                state_db_path,
                doc_db_path,
                key_root_path,
                db3_storage_grpc_url,
                verbose,
                admin_addr,
                doc_id_start,
            } => {
                let log_level = if verbose {
                    LevelFilter::DEBUG
                } else {
                    LevelFilter::INFO
                };

                tracing_subscriber::fmt().with_max_level(log_level).init();
                info!("{ABOUT}");
                let store_sdk =
                    Self::build_store_sdk(db3_storage_grpc_url.as_ref(), key_root_path.as_str());
                let system_store_config = SystemStoreConfig {
                    key_root_path: key_root_path.to_string(),
                    evm_wallet_key: "evm".to_string(),
                    ar_wallet_key: "ar".to_string(),
                };

                let state_config = StateStoreConfig {
                    db_path: state_db_path.to_string(),
                };
                let (update_sender, update_receiver) = tokio::sync::mpsc::channel::<()>(8);
                let state_store = Arc::new(StateStore::new(state_config).unwrap());
                let system_store = Arc::new(SystemStore::new(system_store_config, state_store));
                info!("Arweave address {}", system_store.get_ar_address().unwrap());
                info!("Evm address 0x{}", system_store.get_evm_address().unwrap());
                let system_impl = SystemImpl::new(
                    update_sender,
                    system_store.clone(),
                    SystemRole::DataIndexNode,
                    public_url.to_string(),
                    admin_addr.as_str(),
                )
                .unwrap();

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
                    doc_start_id: doc_id_start,
                };

                let addr = format!("{bind_host}:{listening_port}");
                let db_store = DBStoreV2::new(db_store_config.clone()).unwrap();
                let indexer = IndexerNodeImpl::new(db_store.clone(), system_store).unwrap();
                let indexer_for_syncing = indexer.clone();
                if let Err(_e) = indexer.recover(&store_sdk).await {}
                indexer.subscribe_update(update_receiver).await;
                let listen = tokio::spawn(async move {
                    info!("start syncing data from storage node");
                    indexer_for_syncing.start(store_sdk).await.unwrap();
                });
                info!(
                    "start db3 indexer node on public {} and listen addr {}",
                    public_url, addr
                );
                let cors_layer = CorsLayer::new()
                    .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
                    .allow_headers(Any)
                    .allow_origin(Any);
                Server::builder()
                    .accept_http1(true)
                    .layer(cors_layer)
                    .layer(tonic_web::GrpcWebLayer::new())
                    .add_service(IndexerNodeServer::new(indexer))
                    .add_service(SystemServer::new(system_impl))
                    .serve(addr.parse().unwrap())
                    .await
                    .unwrap();
                let (r1,) = tokio::join!(listen);
                r1.unwrap();
                info!("exit standalone indexer")
            }
            DB3Command::Recover { cmd } => match cmd {
                RecoverCommand::Rollup {
                    mutation_db_path,
                    state_db_path,
                    doc_db_path,
                    meta_db_path,
                    key_root_path,
                    recover_temp_path,
                    admin_addr,
                    doc_id_start,
                    verbose,
                } => {
                    let log_level = if verbose {
                        LevelFilter::DEBUG
                    } else {
                        LevelFilter::INFO
                    };

                    tracing_subscriber::fmt().with_max_level(log_level).init();
                    info!("{ABOUT}");
                    let recover = Self::create_recover(
                        mutation_db_path,
                        meta_db_path,
                        state_db_path,
                        doc_db_path,
                        key_root_path,
                        recover_temp_path,
                        admin_addr,
                        doc_id_start,
                        RecoverType::Rollup,
                    )
                    .await;
                    info!("start recovering index node");
                    recover.recover_stat().unwrap();
                    recover.recover_from_ar().await.unwrap();
                }
                RecoverCommand::Index {
                    meta_db_path,
                    state_db_path,
                    doc_db_path,
                    key_root_path,
                    recover_temp_path,
                    admin_addr,
                    doc_id_start,
                    verbose,
                } => {
                    let log_level = if verbose {
                        LevelFilter::DEBUG
                    } else {
                        LevelFilter::INFO
                    };

                    tracing_subscriber::fmt().with_max_level(log_level).init();
                    info!("{ABOUT}");
                    let recover = Self::create_recover(
                        "".to_string(),
                        meta_db_path,
                        state_db_path,
                        doc_db_path,
                        key_root_path,
                        recover_temp_path,
                        admin_addr,
                        doc_id_start,
                        RecoverType::Index,
                    )
                    .await;
                    info!("start recovering index node");
                    recover.recover_from_ar().await.unwrap();
                }
            },
        }
    }
    async fn create_recover(
        mutation_db_path: String,
        meta_db_path: String,
        state_db_path: String,
        doc_db_path: String,
        key_root_path: String,
        recover_temp_path: String,
        _admin_addr: String,
        doc_id_start: i64,
        recover_type: RecoverType,
    ) -> Recover {
        let system_store_config = SystemStoreConfig {
            key_root_path: key_root_path.to_string(),
            evm_wallet_key: "evm".to_string(),
            ar_wallet_key: "ar".to_string(),
        };

        let state_config = StateStoreConfig {
            db_path: state_db_path.to_string(),
        };
        let state_store = Arc::new(StateStore::new(state_config).unwrap());
        let system_store = Arc::new(SystemStore::new(system_store_config, state_store));
        info!("Arweave address {}", system_store.get_ar_address().unwrap());
        info!("Evm address 0x{}", system_store.get_evm_address().unwrap());
        let doc_store_conf = DocStoreConfig {
            db_root_path: doc_db_path,
            in_memory_db_handle_limit: 16,
        };

        let enable_doc_store = match recover_type {
            RecoverType::Index => true,
            RecoverType::Rollup => false,
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
            enable_doc_store,
            doc_store_conf,
            doc_start_id: doc_id_start,
        };

        let db_store = DBStoreV2::new(db_store_config.clone()).unwrap();

        let storage = match recover_type {
            RecoverType::Rollup => {
                let mutation_store_config = MutationStoreConfig {
                    db_path: mutation_db_path.to_string(),
                    block_store_cf_name: "block_store_cf".to_string(),
                    tx_store_cf_name: "tx_store_cf".to_string(),
                    rollup_store_cf_name: "rollup_store_cf".to_string(),
                    gc_cf_name: "gc_store_cf".to_string(),
                    message_max_buffer: 4 * 1024,
                    scan_max_limit: 50,
                    block_state_cf_name: "block_state_cf".to_string(),
                };
                let store = MutationStore::new(mutation_store_config).unwrap();
                Some(Arc::new(store))
            }
            RecoverType::Index => None,
        };

        std::fs::create_dir_all(recover_temp_path.as_str()).unwrap();
        let recover_config = RecoverConfig {
            key_root_path: key_root_path.to_string(),
            temp_data_path: recover_temp_path.to_string(),
            recover_type,
        };
        Recover::new(recover_config, db_store, system_store, storage)
            .await
            .unwrap()
    }
    /// Start rollup grpc service
    async fn start_rollup_grpc_service(
        public_url: &str,
        bind_host: &str,
        listening_port: u16,
        mutation_db_path: &str,
        state_db_path: &str,
        doc_db_path: &str,
        block_interval: u64,
        rollup_data_path: &str,
        key_root_path: &str,
        admin_addr: &str,
        doc_start_id: i64,
        use_legacy_tx: bool,
    ) {
        let listen_addr = format!("{bind_host}:{listening_port}");
        let rollup_config = RollupExecutorConfig {
            temp_data_path: rollup_data_path.to_string(),
            key_root_path: key_root_path.to_string(),
            use_legacy_tx,
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
        let state_store = Arc::new(StateStore::new(state_config).unwrap());

        let system_store_config = SystemStoreConfig {
            key_root_path: key_root_path.to_string(),
            evm_wallet_key: "evm".to_string(),
            ar_wallet_key: "ar".to_string(),
        };

        let system_store = Arc::new(SystemStore::new(system_store_config, state_store.clone()));
        info!("Arweave address {}", system_store.get_ar_address().unwrap());
        info!(
            "Evm address 0x{}",
            hex::encode(system_store.get_evm_address().unwrap())
        );
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
            doc_start_id,
        };
        let (update_sender, update_receiver) = tokio::sync::mpsc::channel::<()>(8);
        let (sender, receiver) = tokio::sync::mpsc::channel::<(
            DB3Address,
            SubscriptionV2,
            Sender<std::result::Result<EventMessageV2, Status>>,
        )>(1024);

        let config = StorageNodeV2Config {
            store_config,
            rollup_config,
            db_store_config,
            block_interval,
        };
        let storage_node =
            StorageNodeV2Impl::new(config, system_store.clone(), state_store.clone(), sender)
                .await
                .unwrap();
        info!(
            "start db3 store node on public addr {} and listen_addr {}",
            public_url, listen_addr
        );
        std::fs::create_dir_all(rollup_data_path).unwrap();
        storage_node.recover().unwrap();
        let system_impl = SystemImpl::new(
            update_sender,
            system_store.clone(),
            SystemRole::DataRollupNode,
            public_url.to_string(),
            admin_addr,
        )
        .unwrap();
        storage_node
            .keep_subscription(receiver, update_receiver)
            .await
            .unwrap();
        storage_node.start_bg_task().await;
        let cors_layer = CorsLayer::new()
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers(Any)
            .allow_origin(Any);
        Server::builder()
            .accept_http1(true)
            .layer(cors_layer)
            .layer(tonic_web::GrpcWebLayer::new())
            .add_service(StorageNodeV2Server::new(storage_node))
            .add_service(SystemServer::new(system_impl))
            .serve(listen_addr.parse().unwrap())
            .await
            .unwrap();
    }
}
