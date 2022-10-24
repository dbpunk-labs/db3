//! In-memory key/value store ABCI application.

use std::{
    collections::HashMap,
    sync::mpsc::{channel, Receiver, Sender},
};

use bytes::BytesMut;
use db3_proto::db3_mutation_proto::{Mutation, WriteRequest};
use fastcrypto::secp256k1::{Secp256k1PublicKey, Secp256k1PublicKeyBytes, Secp256k1Signature};
use fastcrypto::traits::ToFromBytes;
use hex;
use prost::Message;
use signature::{Signature, Signer, Verifier};
use tendermint_abci::codec::MAX_VARINT_LENGTH;
use tendermint_abci::{codec, Application, Error};
use tendermint_proto::abci::{
    Event, EventAttribute, RequestCheckTx, RequestDeliverTx, RequestInfo, RequestQuery,
    ResponseCheckTx, ResponseCommit, ResponseDeliverTx, ResponseInfo, ResponseQuery,
};
use tracing::{debug, info};
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
    pub fn set<K, V>(&self, key: K, value: V) -> Result<Option<String>, Error>
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let (result_tx, result_rx) = channel();
        channel_send(
            &self.cmd_tx,
            Command::Set {
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

    fn query(&self, request: RequestQuery) -> ResponseQuery {
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
                    log: "does not exist".to_string(),
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

    fn check_tx(&self, request: RequestCheckTx) -> ResponseCheckTx {
        let tx = String::from_utf8(request.tx).unwrap();
        println!("check tx {}", tx);
        let buf = hex::decode(tx).unwrap();
        let request = WriteRequest::decode(buf.as_ref()).unwrap();
        let signature =
            <Secp256k1Signature as ToFromBytes>::from_bytes(request.signature.as_ref()).unwrap();
        let public_key =
            <Secp256k1PublicKey as ToFromBytes>::from_bytes(request.public_key.as_ref()).unwrap();
        match public_key.verify(request.mutation.as_ref(), &signature) {
            Ok(()) => ResponseCheckTx {
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
        for kv_pair in mutation.kv_pairs {
            let key = String::from_utf8(kv_pair.key).unwrap();
            let value = String::from_utf8(kv_pair.value).unwrap();
            let _ = self.set(key, value).unwrap();
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
    height: i64,
    app_hash: Vec<u8>,
    cmd_rx: Receiver<Command>,
}

impl KeyValueStoreDriver {
    fn new(cmd_rx: Receiver<Command>) -> Self {
        Self {
            store: HashMap::new(),
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
                Command::Get { key, result_tx } => {
                    debug!("Getting value for \"{}\"", key);
                    channel_send(
                        &result_tx,
                        (self.height, self.store.get(&key).map(Clone::clone)),
                    )?;
                }
                Command::Set {
                    key,
                    value,
                    result_tx,
                } => {
                    debug!("Setting \"{}\" = \"{}\"", key, value);
                    channel_send(&result_tx, self.store.insert(key, value))?;
                }
                Command::Commit { result_tx } => self.commit(result_tx)?,
            }
        }
    }

    fn commit(&mut self, result_tx: Sender<(i64, Vec<u8>)>) -> Result<(), Error> {
        // As in the Go-based key/value store, simply encode the number of
        // items as the "app hash"
        let mut app_hash = BytesMut::with_capacity(MAX_VARINT_LENGTH);
        codec::encode_varint(self.store.len() as u64, &mut app_hash);
        self.app_hash = app_hash.to_vec();
        self.height += 1;
        channel_send(&result_tx, (self.height, self.app_hash.clone()))
    }
}

#[derive(Debug, Clone)]
enum Command {
    /// Get the height of the last commit.
    GetInfo { result_tx: Sender<(i64, Vec<u8>)> },
    /// Get the key associated with `key`.
    Get {
        key: String,
        result_tx: Sender<(i64, Option<String>)>,
    },
    /// Set the value of `key` to to `value`.
    Set {
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
