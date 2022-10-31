//! In-memory key/value store application for Tendermint.

use db3_kvstore::KeyValueStoreApp;
use merk::Merk;
use structopt::StructOpt;
use tendermint_abci::ServerBuilder;
use tracing_subscriber::filter::LevelFilter;

#[derive(Debug, StructOpt)]
struct Opt {
    /// Bind the TCP server to this host.
    #[structopt(short, long, default_value = "127.0.0.1")]
    host: String,
    /// Bind the TCP server to this port.
    #[structopt(short, long, default_value = "26658")]
    port: u16,
    /// The default server read buffer size, in bytes, for each incoming client
    /// connection.
    #[structopt(short, long, default_value = "1048576")]
    read_buf_size: usize,
    /// Increase output logging verbosity to DEBUG level.
    #[structopt(short, long)]
    verbose: bool,
    /// Suppress all output logging (overrides --verbose).
    #[structopt(short, long)]
    quiet: bool,
    #[structopt(short, long, default_value = "./db")]
    db_path: String,
}

fn main() {
    let opt: Opt = Opt::from_args();
    let log_level = if opt.quiet {
        LevelFilter::OFF
    } else if opt.verbose {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    };
    tracing_subscriber::fmt().with_max_level(log_level).init();
    let mut merk = Merk::open(opt.db_path).unwrap();
    let app = KeyValueStoreApp::new(merk);
    let server = ServerBuilder::new(opt.read_buf_size)
        .bind(format!("{}:{}", opt.host, opt.port), app)
        .unwrap();
    server.listen().unwrap();
}
