use actix_cors::Cors;
use actix_web::{rt, web, App, HttpServer};
use clap::Parser;
use db3_node::abci_impl::{AbciImpl, NodeState};
use db3_node::auth_storage::AuthStorage;
use db3_node::json_rpc_impl;
use db3_node::storage_node_impl::StorageNodeImpl;
use db3_proto::db3_node_proto::storage_node_server::StorageNodeServer;
use merk::Merk;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use tendermint_abci::ServerBuilder;
use tendermint_rpc::HttpClient;
use tonic::transport::Server;
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

#[derive(Parser, Debug, Clone)]
#[clap(about = ABOUT, long_about = None)]
struct Opt {
    /// Bind the gprc server to this .
    #[clap(long, default_value = "127.0.0.1")]
    public_host: String,
    /// The port for grpc
    #[clap(long, default_value = "26659")]
    public_grpc_port: u16,
    #[clap(long, default_value = "26670")]
    public_json_rpc_port: u16,
    /// Bind the abci server to this port.
    #[clap(long, default_value = "26658")]
    abci_port: u16,
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
}

///  start abci server
fn start_abci_service(opt: Opt, store: Arc<Mutex<Pin<Box<AuthStorage>>>>) -> Arc<NodeState> {
    let addr = format!("{}:{}", "127.0.0.1", opt.abci_port);
    let abci_impl = AbciImpl::new(store);
    let node_state = abci_impl.get_node_state().clone();
    thread::spawn(move || {
        let server = ServerBuilder::new(opt.read_buf_size)
            .bind(addr, abci_impl)
            .unwrap();
        server.listen().unwrap();
    });
    node_state
}

fn start_json_rpc_service(opt: Opt, context: json_rpc_impl::Context) {
    let addr = format!("{}:{}", opt.public_host, opt.public_json_rpc_port);
    info!("start json rpc server with addr {}", addr.as_str());
    thread::spawn(move || {
        rt::System::new()
            .block_on(async {
                HttpServer::new(move || {
                    let cors = Cors::default()
                        .send_wildcard()
                        .allowed_methods(vec!["GET", "POST"])
                        .max_age(3600);
                    App::new()
                        .app_data(web::Data::new(context.clone()))
                        .wrap(cors)
                        .service(
                            web::resource("/").route(web::post().to(json_rpc_impl::rpc_router)),
                        )
                })
                .bind((opt.public_host.to_string(), opt.public_json_rpc_port))
                .unwrap()
                .run()
                .await
            })
            .unwrap();
    });
}

#[tokio::main]
async fn main() {
    let opt: Opt = Opt::parse();
    let log_level = if opt.quiet {
        LevelFilter::OFF
    } else if opt.verbose {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    };
    tracing_subscriber::fmt().with_max_level(log_level).init();
    info!("{}", ABOUT);
    let merk = Merk::open(&opt.db_path).unwrap();
    let store = Arc::new(Mutex::new(Box::pin(AuthStorage::new(merk))));
    //TODO recover storage
    let store_for_abci = store.clone();
    let _node_state = start_abci_service(opt.clone(), store_for_abci);

    let client = HttpClient::new("http://127.0.0.1:26657").unwrap();
    let context = json_rpc_impl::Context {
        store: store.clone(),
        client,
    };
    start_json_rpc_service(opt.clone(), context);
    let addr = format!("{}:{}", opt.public_host, opt.public_grpc_port);
    let storage_node = StorageNodeImpl::new(store);
    info!("start db3 storage node on public addr {}", addr);
    Server::builder()
        .add_service(StorageNodeServer::new(storage_node))
        .serve(addr.parse().unwrap())
        .await
        .unwrap();
}
