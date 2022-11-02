use clap::Parser;
use db3_node::abci_impl::AbciImpl;
use db3_node::auth_storage::AuthStorage;
use db3_node::storage_node_impl::StorageNodeImpl;
use db3_proto::db3_node_proto::storage_node_server::StorageNodeServer;
use merk::Merk;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use tendermint_abci::ServerBuilder;
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

#[derive(Parser, Debug)]
#[clap(about = ABOUT, long_about = None)]
struct Opt {
    /// Bind the gprc server to this .
    #[clap(long, default_value = "127.0.0.1")]
    public_grpc_host: String,
    /// The port for grpc
    #[clap(long, default_value = "26659")]
    public_grpc_port: u16,
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
    let merk = Merk::open(opt.db_path).unwrap();
    let store = Arc::new(Mutex::new(Box::pin(AuthStorage::new(merk))));
    //TODO recover storage
    let store_for_abci = store.clone();
    let _abci_handler = thread::spawn(move || {
        let abci_impl = AbciImpl::new(store_for_abci);
        let server = ServerBuilder::new(opt.read_buf_size)
            .bind(format!("{}:{}", "127.0.0.1", opt.abci_port), abci_impl)
            .unwrap();
        server.listen().unwrap();
    });
    let addr = format!("{}:{}", opt.public_grpc_host, opt.public_grpc_port);
    let storage_node = StorageNodeImpl::new(store);
    info!("start db3 storage node on public addr {}", addr);
    Server::builder()
        .add_service(StorageNodeServer::new(storage_node))
        .serve(addr.parse().unwrap())
        .await
        .unwrap();
}
