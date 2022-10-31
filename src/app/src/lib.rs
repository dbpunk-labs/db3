//! In-memory key/value store ABCI application.

use std::{
    collections::HashMap,
    sync::mpsc::{channel, Receiver, Sender},
};

use bytes::BytesMut;
use db3_crypto::verifier;
use db3_proto::db3_bill_proto::{Bill, BillType};
use db3_proto::db3_mutation_proto::{Mutation, WriteRequest};
use db3_storage::bill_store::BillStore;
use db3_storage::kv_store::KvStore;
use db3_types::cost;
/// In-memory, hashmap-backed key/value store ABCI application.
///
/// This structure effectively just serves as a handle to the actual key/value
/// store - the [`KeyValueStoreDriver`].
///
///
use ethereum_types::Address as AccountAddress;
use hex;
use merk::Merk;
use prost::Message;
use rust_secp256k1::Message as HashMessage;
use std::boxed::Box;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tendermint_abci::codec::MAX_VARINT_LENGTH;
use tendermint_abci::{codec, Application, Error};
use tendermint_proto::abci::{
    Event, RequestBeginBlock, RequestCheckTx, RequestDeliverTx, RequestInfo, RequestQuery,
    ResponseBeginBlock, ResponseCheckTx, ResponseCommit, ResponseDeliverTx, ResponseInfo,
    ResponseQuery,
};
use tracing::{debug, info};

pub struct InternalState {
    last_block_height: i64,
    last_block_app_hash: Vec<u8>,
    db: Pin<Box<Merk>>,
    pending_mutation: Vec<(AccountAddress, Mutation)>,
    tmp_id: u64,
    pending_bills: Vec<(AccountAddress, Bill)>,
    current_block_height: i64,
    current_block_app_hash: Vec<u8>,
}

impl std::fmt::Debug for InternalState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "state height {} ", self.last_block_height)
    }
}

#[derive(Debug, Clone)]
pub struct KeyValueStoreApp {
    state: Arc<Mutex<Pin<Box<InternalState>>>>,
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
                tmp_id: 0,
                pending_bills: Vec::new(),
                current_block_height: 0,
                current_block_app_hash: vec![],
            }))),
        }
    }
}

impl Application for KeyValueStoreApp {
    fn info(&self, request: RequestInfo) -> ResponseInfo {
        debug!(
            "Got info request. Tendermint version: {}; Block version: {}; P2P version: {}",
            request.version, request.block_version, request.p2p_version
        );
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
                }
            }
            Err(_) => todo!(),
        }
        Default::default()
    }

    fn query(&self, request: RequestQuery) -> ResponseQuery {
        Default::default()
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
        let buf = hex::decode(tx).unwrap();
        let request = WriteRequest::decode(buf.as_ref()).unwrap();
        let account_id = verifier::MutationVerifier::verify(&request).unwrap();
        let mutation = Mutation::decode(request.mutation.as_ref()).unwrap();
        let mutation_id = HashMessage::from_hashed_data::<rust_secp256k1::hashes::sha256::Hash>(
            request.mutation.as_ref(),
        );
        //TODO check nonce
        match self.state.lock() {
            Ok(mut s) => {
                // add mu
                let gas_fee = cost::estimate_gas(&mutation);
                s.pending_mutation.push((account_id.addr, mutation));
                s.tmp_id = s.tmp_id + 1;
                let bill = Bill {
                    gas_fee,
                    block_height: s.current_block_height as u64,
                    bill_id: s.tmp_id,
                    bill_type: BillType::BillForMutation.into(),
                    time: 0,
                    bill_target_id: mutation_id.as_ref().to_vec(),
                };
                s.pending_bills.push((account_id.addr, bill));
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
                let mutations = &s.pending_mutation.to_vec();
                let bills = &s.pending_bills.to_vec();
                for (addr, mutation) in mutations {
                    let db: Pin<&mut Merk> = Pin::as_mut(&mut s.db);
                    KvStore::apply(db, &addr, &mutation).unwrap();
                }
                for (addr, bill) in bills {
                    let db: Pin<&mut Merk> = Pin::as_mut(&mut s.db);
                    BillStore::apply(db, &addr, &bill).unwrap();
                }
                s.pending_mutation.clear();
                s.pending_bills.clear();
                s.last_block_app_hash = s.db.root_hash().to_vec();
                s.current_block_app_hash = vec![];
                s.last_block_height = s.current_block_height;
                s.current_block_height = 0;
                ResponseCommit {
                    data: s.last_block_app_hash.to_vec(),
                    retain_height: s.last_block_height,
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
