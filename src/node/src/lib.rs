//! In-memory key/value store ABCI application.

use std::{
    collections::HashMap,
    sync::mpsc::{channel, Receiver, Sender},
};

use bytes::BytesMut;
use db3_crypto::verifier;
use db3_error::DB3Error;
use db3_proto::db3_bill_proto::{Bill, BillQueryRequest, BillType};
use db3_proto::db3_mutation_proto::{Mutation, WriteRequest};
use db3_storage::account_store::AccountStore;
use db3_storage::bill_store::BillStore;
use db3_storage::kv_store::KvStore;
use db3_types::{cost, gas};
use ethereum_types::Address as AccountAddress;
use hex;
use merk::{proofs::encode_into, Merk};
use prost::Message;
use rust_secp256k1::Message as HashMessage;
use serde_json::json;
use std::boxed::Box;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use tendermint_abci::codec::MAX_VARINT_LENGTH;
use tendermint_abci::{codec, Application, Error};
use tendermint_proto::abci::{
    Event, RequestBeginBlock, RequestCheckTx, RequestDeliverTx, RequestInfo, RequestQuery,
    ResponseBeginBlock, ResponseCheckTx, ResponseCommit, ResponseDeliverTx, ResponseInfo,
    ResponseQuery,
};
use tracing::{debug, info};
use tracing::{span, Level};

pub struct InternalState {
    last_block_height: i64,
    last_block_app_hash: Vec<u8>,
    db: Pin<Box<Merk>>,
    pending_mutation: Vec<(AccountAddress, Mutation, Bill)>,
    current_block_time: u64,
    current_block_height: i64,
    current_block_app_hash: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct NodeState {
    total_storage_bytes: Arc<AtomicU64>,
    total_mutations: Arc<AtomicU64>,
}

impl std::fmt::Debug for InternalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "state height {} ", self.last_block_height)
    }
}

#[derive(Debug, Clone)]
pub struct KeyValueStoreApp {
    state: Arc<Mutex<Pin<Box<InternalState>>>>,
    node_state: Arc<NodeState>,
}

impl KeyValueStoreApp {
    /// Constructor.
    pub fn new(merk: Merk) -> Self {
        Self {
            state: Arc::new(Mutex::new(Box::pin(InternalState {
                last_block_height: 0,
                last_block_app_hash: vec![],
                db: Box::pin(merk),
                pending_mutation: Vec::new(),
                current_block_height: 0,
                current_block_app_hash: vec![],
                current_block_time: 0,
            }))),
            node_state: Arc::new(NodeState {
                total_storage_bytes: Arc::new(AtomicU64::new(0)),
                total_mutations: Arc::new(AtomicU64::new(0)),
            }),
        }
    }
}

impl Application for KeyValueStoreApp {
    fn info(&self, request: RequestInfo) -> ResponseInfo {
        match self.state.lock() {
            Ok(s) => ResponseInfo {
                data: "db3".to_string(),
                version: "0.1.0".to_string(),
                app_version: 1,
                last_block_height: s.last_block_height,
                last_block_app_hash: s.last_block_app_hash.to_vec(),
            },
            Err(_) => ResponseInfo {
                data: "db3".to_string(),
                version: "0.1.0".to_string(),
                app_version: 1,
                last_block_height: 0,
                last_block_app_hash: vec![],
            },
        }
    }

    fn begin_block(&self, request: RequestBeginBlock) -> ResponseBeginBlock {
        match self.state.lock() {
            Ok(mut s) => {
                if let Some(header) = request.header {
                    s.current_block_height = header.height;
                    if let Some(time) = header.time {
                        s.current_block_time = time.seconds as u64;
                    }
                }
            }
            Err(_) => todo!(),
        }
        Default::default()
    }

    fn query(&self, request: RequestQuery) -> ResponseQuery {
        match request.path.as_ref() {
            "node" => {
                let node_status = json!({
                        "total_mutations": self.node_state.total_mutations.load(std::sync::atomic::Ordering::Relaxed),
                        "total_storage_bytes": self.node_state.total_storage_bytes.load(std::sync::atomic::Ordering::Relaxed),
                });
                ResponseQuery {
                    code: 0,
                    log: "".to_string(),
                    info: "".to_string(),
                    index: 0,
                    key: vec![],
                    value: node_status.to_string().as_bytes().to_vec(),
                    proof_ops: None,
                    height: 0,
                    codespace: "".to_string(),
                }
            }
            "bill" => {
                if let Ok(bq) = BillQueryRequest::decode(request.data.as_ref()) {
                    let bills_result = match self.state.lock() {
                        Ok(s) => {
                            BillStore::scan(s.db.as_ref(), bq.block_height, bq.start_id, bq.end_id)
                        }
                        Err(_) => Err(DB3Error::StateLockBusyError),
                    };
                    if let Ok(bills) = bills_result {
                        let mut buf = Vec::with_capacity(128);
                        encode_into(bills.iter(), &mut buf);
                        return ResponseQuery {
                            code: 0,
                            log: "".to_string(),
                            info: "".to_string(),
                            index: 0,
                            key: vec![],
                            value: buf,
                            proof_ops: None,
                            height: bq.block_height as i64,
                            codespace: "".to_string(),
                        };
                    } else {
                        return ResponseQuery {
                            code: 1,
                            log: "".to_string(),
                            info: "bad bill query format".to_string(),
                            index: 0,
                            key: vec![],
                            value: vec![],
                            proof_ops: None,
                            height: 0,
                            codespace: "".to_string(),
                        };
                    }
                } else {
                    return ResponseQuery {
                        code: 1,
                        log: "".to_string(),
                        info: "".to_string(),
                        index: 0,
                        key: vec![],
                        value: vec![],
                        proof_ops: None,
                        height: 0,
                        codespace: "".to_string(),
                    };
                }
            }
            "account" => {
                let addr_str = String::from_utf8(request.data).unwrap();
                let buf = hex::decode(addr_str).unwrap();
                let addr = AccountAddress::from_slice(buf.as_ref());
                match self.state.lock() {
                    Ok(s) => {
                        let account = AccountStore::get_account(s.db.as_ref(), &addr);
                        if let Ok(Some(a)) = account {
                            let content = serde_json::to_string(&a).unwrap();
                            return ResponseQuery {
                                code: 0,
                                log: "".to_string(),
                                info: "".to_string(),
                                index: 0,
                                key: vec![],
                                value: content.into_bytes().into(),
                                proof_ops: None,
                                height: s.last_block_height,
                                codespace: "".to_string(),
                            };
                        }
                    }
                    Err(_) => todo!(),
                }
                Default::default()
            }
            _ => Default::default(),
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
        //TODO match the hash fucntion with tendermint
        let mutation_id = HashMessage::from_hashed_data::<rust_secp256k1::hashes::sha256::Hash>(
            request.tx.as_ref(),
        );
        let tx = String::from_utf8(request.tx).unwrap();
        let buf = hex::decode(tx).unwrap();
        let wrequest = WriteRequest::decode(buf.as_ref()).unwrap();
        let account_id = verifier::MutationVerifier::verify(&wrequest).unwrap();
        let mutation = Mutation::decode(wrequest.mutation.as_ref()).unwrap();

        //TODO check nonce
        match self.state.lock() {
            Ok(mut s) => {
                let gas_fee = cost::estimate_gas(&mutation);
                let bill = Bill {
                    gas_fee: Some(gas_fee),
                    block_height: s.current_block_height as u64,
                    bill_id: s.current_block_time,
                    bill_type: BillType::BillForMutation.into(),
                    time: s.current_block_time,
                    bill_target_id: mutation_id.as_ref().to_vec(),
                    owner: account_id.addr.as_bytes().to_vec(),
                    query_addr: vec![],
                };
                s.pending_mutation.push((account_id.addr, mutation, bill));
            }
            Err(_) => {}
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
        match self.state.lock() {
            Ok(mut s) => {
                let span = span!(Level::INFO, "commit").entered();
                let mutations: Vec<_> = s.pending_mutation.drain(..).collect();
                for (addr, mutation, bill) in mutations {
                    let db: Pin<&mut Merk> = Pin::as_mut(&mut s.db);
                    let result = KvStore::apply(db, &addr, &mutation);
                    if let Ok((_gas, bytes)) = result {
                        //TODO compare gas with bill's
                        let db: Pin<&mut Merk> = Pin::as_mut(&mut s.db);
                        BillStore::apply(db, &bill).unwrap();
                        let account = AccountStore::get_account(s.db.as_ref(), &addr);
                        if let Ok(Some(mut a)) = account {
                            let new_total_bills = match a.total_bills {
                                Some(t) => gas::gas_add(&t, &bill.gas_fee.unwrap()),
                                None => bill.gas_fee.unwrap(),
                            };
                            a.total_bills = Some(new_total_bills);
                            a.total_storage_in_bytes = a.total_storage_in_bytes + bytes as u64;
                            a.total_mutation_count = a.total_mutation_count + 1;
                            let db: Pin<&mut Merk> = Pin::as_mut(&mut s.db);
                            AccountStore::apply(db, &addr, &a).unwrap();
                        }
                        self.node_state
                            .total_mutations
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.node_state
                            .total_storage_bytes
                            .fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                s.last_block_app_hash = s.db.root_hash().to_vec();
                s.current_block_app_hash = vec![];
                s.last_block_height = s.current_block_height;
                s.current_block_height = 0;
                span.exit();
                ResponseCommit {
                    data: s.last_block_app_hash.to_vec(),
                    retain_height: 0,
                }
            }
            Err(_) => {
                // never go to here
                ResponseCommit {
                    data: vec![],
                    retain_height: 0,
                }
            }
        }
    }
}
unsafe impl Send for KeyValueStoreApp {}

unsafe impl Sync for KeyValueStoreApp {}
