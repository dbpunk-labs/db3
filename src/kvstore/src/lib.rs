//! In-memory key/value store ABCI application.

use std::{
    collections::HashMap,
    sync::mpsc::{channel, Receiver, Sender},
};

use bytes::BytesMut;
use db3_crypto::verifier;
use db3_proto::db3_mutation_proto::{Mutation, WriteRequest};
use hex;
use prost::Message;
use tendermint_abci::codec::MAX_VARINT_LENGTH;
use tendermint_abci::{codec, Application, Error};
use tendermint_proto::abci::{
    Event, RequestCheckTx, RequestDeliverTx, RequestInfo, RequestQuery, ResponseCheckTx,
    ResponseCommit, ResponseDeliverTx, ResponseInfo, ResponseQuery,
};
use tracing::{debug, info};
use db3_crypto::account_id::AccountId;

pub const UNIT_CONST: i64 = 1;

/// In-memory, hashmap-backed key/value store ABCI application.
///
/// This structure effectively just serves as a handle to the actual key/value
/// store - the [`KeyValueStoreDriver`].
///
#[derive(Debug, Clone)]
pub struct KeyValueStoreApp {
    cmd_tx: Sender<Command>,
}

impl KeyValueStoreApp {
    /// Constructor.
    pub fn new() -> (Self, KeyValueStoreDriver) {
        let (cmd_tx, cmd_rx) = channel();
        (Self { cmd_tx }, KeyValueStoreDriver::new(cmd_rx))
    }


    /// Attempt to retrieve the bill associated with the given addr.
    pub fn get_bill(&self, addr: String) -> Result<(i64, Option<i64>), Error> {
        let (result_tx, result_rx) = channel();
        channel_send(
            &self.cmd_tx,
            Command::GetBill {
                addr: addr,
                result_tx,
            },
        )?;
        channel_recv(&result_rx)
    }

    /// Attempt to retrieve the value associated with the given key.
    pub fn get<K: AsRef<str>>(&self, key: K) -> Result<(i64, Option<String>), Error> {
        let (result_tx, result_rx) = channel();
        channel_send(
            &self.cmd_tx,
            Command::Get {
                key: key.as_ref().to_string(),
                result_tx,
            },
        )?;
        channel_recv(&result_rx)
    }

    /// Attempt to set the value associated with the given key.
    ///
    /// Optionally returns any pre-existing value associated with the given
    /// key.
    pub fn set<K, V>(&self, addr: &str, key: K, value: V) -> Result<Option<String>, Error>
        where
            K: AsRef<str>,
            V: AsRef<str>,
    {
        let (result_tx, result_rx) = channel();
        channel_send(
            &self.cmd_tx,
            Command::Set {
                addr: addr.to_string(),
                key: key.as_ref().to_string(),
                value: value.as_ref().to_string(),
                result_tx,
            },
        )?;
        channel_recv(&result_rx)
    }
}

impl Application for KeyValueStoreApp {
    fn info(&self, request: RequestInfo) -> ResponseInfo {
        debug!(
            "Got info request. Tendermint version: {}; Block version: {}; P2P version: {}",
            request.version, request.block_version, request.p2p_version
        );

        let (result_tx, result_rx) = channel();
        channel_send(&self.cmd_tx, Command::GetInfo { result_tx }).unwrap();
        let (last_block_height, last_block_app_hash) = channel_recv(&result_rx).unwrap();

        ResponseInfo {
            data: "kvstore-rs".to_string(),
            version: "0.1.0".to_string(),
            app_version: 1,
            last_block_height,
            last_block_app_hash,
        }
    }

    ///
    /// Query with path and data
    /// query /store
    /// query /bill
    fn query(&self, request: RequestQuery) -> ResponseQuery {
        let formatted_paht = request.path.to_lowercase();
        let tokens = formatted_paht.split('/').collect::<Vec<&str>>();


        if tokens.len() > 0 && tokens[0] == "bill" {
            // query bill
            let addr = match String::from_utf8(request.data.clone()) {
                Ok(s) => s,
                Err(e) => panic!("Failed to intepret key as UTF-8: {}", e),
            };
            debug!("Attempting to get bill for : {}", addr);
            match self.get_bill(addr.clone()) {
                Ok((height, value_opt)) => match value_opt {
                    Some(value) => {
                        println!("bill: {}", value);
                        ResponseQuery {
                            code: 0,
                            log: "exists".to_string(),
                            info: "".to_string(),
                            index: 0,
                            key: request.data,
                            value: value.to_string().into_bytes(),
                            proof_ops: None,
                            height,
                            codespace: "".to_string(),
                        }
                    }
                    None => ResponseQuery {
                        code: 0,
                        log: "bill does not exist".to_string(),
                        info: "".to_string(),
                        index: 0,
                        key: request.data,
                        value: vec![],
                        proof_ops: None,
                        height,
                        codespace: "".to_string(),
                    },
                },
                Err(e) => panic!("Failed to get bill for \"{}\": {:?}", addr, e),
            }
        } else {
            // query store
            let key = match String::from_utf8(request.data.clone()) {
                Ok(s) => s,
                Err(e) => panic!("Failed to intepret key as UTF-8: {}", e),
            };
            debug!("Attempting to get key: {}", key);
            match self.get(key.clone()) {
                Ok((height, value_opt)) => match value_opt {
                    Some(value) => ResponseQuery {
                        code: 0,
                        log: "exists".to_string(),
                        info: "".to_string(),
                        index: 0,
                        key: request.data,
                        value: value.into_bytes(),
                        proof_ops: None,
                        height,
                        codespace: "".to_string(),
                    },
                    None => ResponseQuery {
                        code: 0,
                        log: "key does not exist".to_string(),
                        info: "".to_string(),
                        index: 0,
                        key: request.data,
                        value: vec![],
                        proof_ops: None,
                        height,
                        codespace: "".to_string(),
                    },
                },
                Err(e) => panic!("Failed to get key \"{}\": {:?}", key, e),
            }
        }
    }

    fn check_tx(&self, request: RequestCheckTx) -> ResponseCheckTx {
        let tx = String::from_utf8(request.tx).unwrap();
        let buf = hex::decode(tx).unwrap();
        let request = WriteRequest::decode(buf.as_ref()).unwrap();
        let account_id = verifier::MutationVerifier::verify(&request);
        match account_id {
            Ok(_) => ResponseCheckTx {
                code: 0,
                data: vec![],
                log: "".to_string(),
                info: "".to_string(),
                gas_wanted: 1,
                gas_used: 0,
                events: vec![],
                codespace: "".to_string(),
                ..Default::default()
            },
            Err(_) => ResponseCheckTx {
                code: 1,
                data: vec![],
                log: "".to_string(),
                info: "".to_string(),
                gas_wanted: 1,
                gas_used: 0,
                events: vec![],
                codespace: "".to_string(),
                ..Default::default()
            },
        }
    }
    fn deliver_tx(&self, request: RequestDeliverTx) -> ResponseDeliverTx {
        let tx = String::from_utf8(request.tx).unwrap();
        println!("deliver tx {}", tx);
        let buf = hex::decode(tx).unwrap();
        let request = WriteRequest::decode(buf.as_ref()).unwrap();
        let mutation = Mutation::decode(request.mutation.as_ref()).unwrap();
        let account_id = verifier::MutationVerifier::verify(&request).unwrap();
        let addr = hex::encode(account_id.addr.as_bytes());
        println!("addr: {}", addr);
        for kv_pair in mutation.kv_pairs {
            let key = String::from_utf8(kv_pair.key).unwrap();
            let value = String::from_utf8(kv_pair.value).unwrap();
            match self.set(&addr, key, value) {
                Ok(res) => {}
                Err(e) => {
                    return ResponseDeliverTx {
                        code: 1,
                        data: vec![],
                        log: "".to_string(),
                        info: "".to_string(),
                        gas_wanted: 0,
                        gas_used: 0,
                        events: vec![Event {
                            r#type: "app".to_string(),
                            attributes: vec![],
                        }],
                        codespace: "".to_string(),
                    };
                }
            }
        }
        ResponseDeliverTx {
            code: 0,
            data: vec![],
            log: "".to_string(),
            info: "".to_string(),
            gas_wanted: 0,
            gas_used: 0,
            events: vec![Event {
                r#type: "app".to_string(),
                attributes: vec![],
            }],
            codespace: "".to_string(),
        }
    }

    fn commit(&self) -> ResponseCommit {
        let (result_tx, result_rx) = channel();
        channel_send(&self.cmd_tx, Command::Commit { result_tx }).unwrap();
        let (height, app_hash) = channel_recv(&result_rx).unwrap();
        info!("Committed height {}", height);
        ResponseCommit {
            data: app_hash,
            retain_height: height - 1,
        }
    }
}

/// Manages key/value store state.
#[derive(Debug)]
pub struct KeyValueStoreDriver {
    store: HashMap<String, String>,
    account_bill: HashMap<String, i64>,
    height: i64,
    app_hash: Vec<u8>,
    cmd_rx: Receiver<Command>,
}

impl KeyValueStoreDriver {
    fn new(cmd_rx: Receiver<Command>) -> Self {
        Self {
            store: HashMap::new(),
            account_bill: HashMap::new(),
            height: 0,
            app_hash: vec![0_u8; MAX_VARINT_LENGTH],
            cmd_rx,
        }
    }

    /// Run the driver in the current thread (blocking).
    pub fn run(mut self) -> Result<(), Error> {
        loop {
            let cmd = self.cmd_rx.recv().map_err(Error::channel_recv)?;
            match cmd {
                Command::GetInfo { result_tx } => {
                    channel_send(&result_tx, (self.height, self.app_hash.clone()))?
                }
                Command::GetBill { addr, result_tx } => {
                    debug!("Getting bill for \"{}\"", addr);
                    channel_send(
                        &result_tx,
                        (self.height, self.account_bill.get(&addr).map(Clone::clone)),
                    )?;
                }
                Command::Get { key, result_tx } => {
                    debug!("Getting value for \"{}\"", key);
                    channel_send(
                        &result_tx,
                        (self.height, self.store.get(&key).map(Clone::clone)),
                    )?;
                }
                Command::Set {
                    addr,
                    key,
                    value,
                    result_tx,
                } => {
                    debug!("Setting \"{}\" = \"{}\"", key, value);
                    let value_size = value.as_bytes().len() as i64;
                    let mut payload_cost = value_size * UNIT_CONST;
                    if let Some(cost) = self.account_bill.get(&addr) {
                        payload_cost += cost;
                    }
                    println!("account bill insert {} {}", addr.clone(), payload_cost);
                    self.account_bill.insert(addr, payload_cost);
                    channel_send(&result_tx, self.store.insert(key, value));
                }
                Command::Commit { result_tx } => self.commit(result_tx)?,
            }
        }
    }

    fn commit(&mut self, result_tx: Sender<(i64, Vec<u8>)>) -> Result<(), Error> {
        // As in the Go-based key/value store, simply encode the number of
        // items as the "app hash"
        let mut app_hash = BytesMut::with_capacity(MAX_VARINT_LENGTH);
        codec::encode_varint(self.store.len() as u64 + self.account_bill.len() as u64, &mut app_hash);
        self.app_hash = app_hash.to_vec();
        self.height += 1;
        channel_send(&result_tx, (self.height, self.app_hash.clone()))
    }
}

#[derive(Debug, Clone)]
enum Command {
    /// Get the height of the last commit.
    GetInfo { result_tx: Sender<(i64, Vec<u8>)> },
    GetBill {
        addr: String,
        result_tx: Sender<(i64, Option<i64>)>,
    },
    /// Get the key associated with `key`.
    Get {
        key: String,
        result_tx: Sender<(i64, Option<String>)>,
    },
    /// Set the value of `key` to to `value`.
    Set {
        addr: String,
        key: String,
        value: String,
        result_tx: Sender<Option<String>>,
    },
    /// Commit the current state of the application, which involves recomputing
    /// the application's hash.
    Commit { result_tx: Sender<(i64, Vec<u8>)> },
}

fn channel_send<T>(tx: &Sender<T>, value: T) -> Result<(), Error> {
    tx.send(value).map_err(Error::send)
}

fn channel_recv<T>(rx: &Receiver<T>) -> Result<T, Error> {
    rx.recv().map_err(Error::channel_recv)
}
