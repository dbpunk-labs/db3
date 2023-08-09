//
// mutation_store.rs
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

use bytes::{Bytes, BytesMut};
use db3_base::times;
use db3_crypto::db3_address::DB3Address;
use db3_crypto::id::TxId;
use db3_error::{DB3Error, Result};
use db3_proto::db3_base_proto::MutationState;
use db3_proto::db3_mutation_v2_proto::{MutationAction, MutationBody, MutationHeader};
use db3_proto::db3_rollup_proto::{GcRecord as GCRecord, RollupRecord};
use ethers::types::U256;
use prost::Message;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options, WriteBatch};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::info;

type StorageEngine = DBWithThreadMode<MultiThreaded>;
const STATE_CF: &str = "STATE_CF";

#[derive(Clone)]
pub struct MutationStoreConfig {
    pub db_path: String,
    pub block_store_cf_name: String,
    pub tx_store_cf_name: String,
    pub rollup_store_cf_name: String,
    pub gc_cf_name: String,
    pub message_max_buffer: usize,
    pub scan_max_limit: usize,
    pub block_state_cf_name: String,
}

impl Default for MutationStoreConfig {
    fn default() -> MutationStoreConfig {
        MutationStoreConfig {
            db_path: "./store".to_string(),
            block_store_cf_name: "block_store_cf".to_string(),
            tx_store_cf_name: "tx_store_cf".to_string(),
            rollup_store_cf_name: "rollup_store_cf".to_string(),
            gc_cf_name: "gc_store_cf".to_string(),
            message_max_buffer: 8 * 1024,
            scan_max_limit: 50,
            block_state_cf_name: "block_state_cf".to_string(),
        }
    }
}

struct BlockState {
    pub block: u64,
    pub order: u32,
}

struct MutationCost {
    pub rollup_ar_cost: U256,
    pub rollup_evm_cost: U256,
}

#[derive(Clone)]
pub struct MutationStore {
    config: MutationStoreConfig,
    se: Arc<StorageEngine>,
    block_state: Arc<Mutex<BlockState>>,
    mutation_count: Arc<AtomicU64>,
    total_mutation_bytes: Arc<AtomicU64>,
    total_rollup_bytes: Arc<AtomicU64>,
    gc_count: Arc<AtomicU64>,
    rollup_count: Arc<AtomicU64>,
    total_gc_bytes: Arc<AtomicU64>,
    rollup_cost: Arc<Mutex<MutationCost>>,
    total_rollup_raw_bytes: Arc<AtomicU64>,
    total_rollup_mutation_count: Arc<AtomicU64>,
}

impl MutationStore {
    pub fn new(config: MutationStoreConfig) -> Result<Self> {
        let mut cf_opts = Options::default();
        cf_opts.create_if_missing(true);
        cf_opts.create_missing_column_families(true);
        info!("open mutation store with path {}", config.db_path.as_str());
        let path = Path::new(config.db_path.as_str());
        let se = Arc::new(
            StorageEngine::open_cf(
                &cf_opts,
                &path,
                [
                    config.block_store_cf_name.as_str(),
                    config.tx_store_cf_name.as_str(),
                    config.rollup_store_cf_name.as_str(),
                    config.gc_cf_name.as_str(),
                    config.block_state_cf_name.as_str(),
                    STATE_CF,
                ],
            )
            .map_err(|e| {
                DB3Error::OpenStoreError(
                    config.db_path.to_string(),
                    format!("fail to open column family for mutation store with error {e}"),
                )
            })?,
        );
        Ok(Self {
            config,
            se,
            block_state: Arc::new(Mutex::new(BlockState { block: 0, order: 0 })),
            mutation_count: Arc::new(AtomicU64::new(0)),
            total_mutation_bytes: Arc::new(AtomicU64::new(0)),
            total_rollup_bytes: Arc::new(AtomicU64::new(0)),
            gc_count: Arc::new(AtomicU64::new(0)),
            rollup_count: Arc::new(AtomicU64::new(0)),
            total_gc_bytes: Arc::new(AtomicU64::new(0)),
            rollup_cost: Arc::new(Mutex::new(MutationCost {
                rollup_ar_cost: U256::from(0),
                rollup_evm_cost: U256::from(0),
            })),
            total_rollup_raw_bytes: Arc::new(AtomicU64::new(0)),
            total_rollup_mutation_count: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn recover(&self) -> Result<()> {
        let cf_handle = self
            .se
            .cf_handle(self.config.block_state_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError(
                "cf is not found when recovering mutation store".to_string(),
            ))?;
        let block_key: &str = "block_key";
        let value = self
            .se
            .get_cf(&cf_handle, block_key.as_bytes())
            .map_err(|e| DB3Error::ReadStoreError(format!("fail to read block key value {e}")))?;
        if let Some(v) = value {
            let data_array: [u8; 8] = v
                .try_into()
                .map_err(|_| DB3Error::KeyCodecError("invalid array length".to_string()))?;
            let block = u64::from_be_bytes(data_array) + 1;
            match self.block_state.lock() {
                Ok(mut state) => {
                    state.block = block;
                    state.order = 0;
                    info!("recover block {}", block);
                }
                _ => {}
            }
        }
        self.recover_last_state()?;
        Ok(())
    }
    pub fn flush_state(&self) -> Result<()> {
        let state = self.get_latest_state();
        // record a data point in every hour
        let key = times::get_current_time_in_secs() / 60 / 60;
        self.add_record::<MutationState>(STATE_CF, &key.to_be_bytes(), &state)?;
        Ok(())
    }

    pub fn get_latest_state(&self) -> MutationState {
        let (total_ar_cost, total_evm_cost) = match self.rollup_cost.lock() {
            Ok(cost) => (cost.rollup_ar_cost, cost.rollup_evm_cost),
            Err(_) => {
                todo!()
            }
        };
        let mut storage_cost_bytes: [u8; 32] = [0; 32];
        total_ar_cost.to_big_endian(&mut storage_cost_bytes);
        let mut evm_cost_bytes: [u8; 32] = [0; 32];
        total_evm_cost.to_big_endian(&mut evm_cost_bytes);
        MutationState {
            mutation_count: self.mutation_count.load(Ordering::Relaxed),
            total_mutation_bytes: self.total_mutation_bytes.load(Ordering::Relaxed),
            gc_count: self.gc_count.load(Ordering::Relaxed),
            total_gc_bytes: self.total_gc_bytes.load(Ordering::Relaxed),
            rollup_count: self.rollup_count.load(Ordering::Relaxed),
            total_rollup_bytes: self.total_rollup_bytes.load(Ordering::Relaxed),
            total_rollup_raw_bytes: self.total_rollup_raw_bytes.load(Ordering::Relaxed),
            total_storage_cost: storage_cost_bytes.into(),
            total_evm_cost: evm_cost_bytes.into(),
            total_rollup_mutation_count: self.total_rollup_mutation_count.load(Ordering::Relaxed),
        }
    }

    fn recover_last_state(&self) -> Result<()> {
        if let Some(m) = self.get_last_record::<MutationState>(STATE_CF)? {
            self.mutation_count
                .store(m.mutation_count, Ordering::Relaxed);
            self.total_mutation_bytes
                .store(m.total_mutation_bytes, Ordering::Relaxed);
            self.gc_count.store(m.gc_count, Ordering::Relaxed);
            self.rollup_count.store(m.rollup_count, Ordering::Relaxed);
            self.total_rollup_bytes
                .store(m.total_rollup_bytes, Ordering::Relaxed);
            self.total_gc_bytes
                .store(m.total_gc_bytes, Ordering::Relaxed);
            self.total_rollup_raw_bytes
                .store(m.total_rollup_raw_bytes, Ordering::Relaxed);
            self.total_rollup_mutation_count
                .store(m.total_rollup_mutation_count, Ordering::Relaxed);
            match self.rollup_cost.lock() {
                Ok(mut cost) => {
                    cost.rollup_ar_cost = U256::from(m.total_storage_cost.as_ref() as &[u8]);
                    cost.rollup_evm_cost = U256::from(m.total_evm_cost.as_ref() as &[u8]);
                }
                Err(_) => {
                    todo!()
                }
            }
        }
        Ok(())
    }

    fn add_record<T>(&self, cf: &str, key: &[u8], value: &T) -> Result<usize>
    where
        T: Message + std::default::Default,
    {
        let cf_handle = self
            .se
            .cf_handle(cf)
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut buf = BytesMut::with_capacity(1024);
        value
            .encode(&mut buf)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        let buf = buf.freeze();
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_handle, key, buf.as_ref());
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        Ok(buf.len())
    }

    pub fn add_gc_record(&self, record: &GCRecord) -> Result<()> {
        let id = record.start_block.to_be_bytes();
        let record_size =
            self.add_record::<GCRecord>(self.config.gc_cf_name.as_str(), &id, record)?;
        self.gc_count.fetch_add(1, Ordering::Relaxed);
        self.total_gc_bytes
            .fetch_add(record_size as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn has_enough_round_left(&self, start_block: u64, min_rounds: u64) -> Result<bool> {
        let cf_handle = self
            .se
            .cf_handle(self.config.rollup_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it = self.se.raw_iterator_cf(&cf_handle);
        let id = start_block.to_be_bytes();
        it.seek(id);
        let mut count: u64 = 0;
        while it.valid() {
            count += 1;
            if count >= min_rounds {
                return Ok(true);
            }
            it.next();
        }
        return Ok(false);
    }

    pub fn get_last_gc_record(&self) -> Result<Option<GCRecord>> {
        self.get_last_record::<GCRecord>(self.config.gc_cf_name.as_str())
    }

    pub fn get_next_rollup_record(&self, start_block: u64) -> Result<Option<RollupRecord>> {
        let id = start_block.to_be_bytes();
        self.get_next_record::<RollupRecord>(self.config.rollup_store_cf_name.as_str(), &id)
    }

    pub fn get_rollup_record(&self, start_block: u64) -> Result<Option<RollupRecord>> {
        let id = start_block.to_be_bytes();
        self.get_record::<RollupRecord>(self.config.rollup_store_cf_name.as_str(), &id)
    }

    pub fn add_rollup_record(&self, record: &RollupRecord) -> Result<()> {
        // validate the end block
        let rollup_cf_handle = self
            .se
            .cf_handle(self.config.rollup_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let id = record.start_block.to_be_bytes();
        let mut buf = BytesMut::with_capacity(1024);
        record
            .encode(&mut buf)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        let buf = buf.freeze();
        let mut batch = WriteBatch::default();
        // store the rollup record
        batch.put_cf(&rollup_cf_handle, &id, buf.as_ref());
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        self.rollup_count.fetch_add(1, Ordering::Relaxed);
        self.total_rollup_bytes
            .fetch_add(record.compress_data_size, Ordering::Relaxed);
        self.total_rollup_raw_bytes
            .fetch_add(record.raw_data_size, Ordering::Relaxed);
        match self.rollup_cost.lock() {
            Ok(mut cost) => {
                cost.rollup_ar_cost = cost.rollup_ar_cost + record.cost;
                cost.rollup_evm_cost = cost.rollup_ar_cost + record.cost;
            }
            Err(_) => {
                todo!()
            }
        }
        Ok(())
    }

    fn get_record<T>(&self, cf: &str, id: &[u8]) -> Result<Option<T>>
    where
        T: Message + std::default::Default,
    {
        let cf_handle = self
            .se
            .cf_handle(cf)
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let value = self
            .se
            .get_cf(&cf_handle, id)
            .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
        if let Some(v) = value {
            match T::decode(v.as_ref()) {
                Ok(m) => Ok(Some(m)),
                Err(e) => Err(DB3Error::ReadStoreError(format!("{e}"))),
            }
        } else {
            Ok(None)
        }
    }

    fn get_next_record<T>(&self, cf: &str, id: &[u8]) -> Result<Option<T>>
    where
        T: Message + std::default::Default,
    {
        let cf_handle = self
            .se
            .cf_handle(cf)
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it = self.se.raw_iterator_cf(&cf_handle);
        it.seek(id);
        it.next();
        if it.valid() {
            if let Some(v) = it.value() {
                match T::decode(v) {
                    Ok(r) => return Ok(Some(r)),
                    Err(e) => return Err(DB3Error::ReadStoreError(format!("{e}"))),
                }
            }
        }
        return Ok(None);
    }

    fn get_last_record<T>(&self, cf: &str) -> Result<Option<T>>
    where
        T: Message + std::default::Default,
    {
        let cf_handle = self
            .se
            .cf_handle(cf)
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it = self.se.raw_iterator_cf(&cf_handle);
        it.seek_to_last();
        if it.valid() {
            if let Some(v) = it.value() {
                match T::decode(v) {
                    Ok(r) => return Ok(Some(r)),
                    Err(e) => return Err(DB3Error::ReadStoreError(format!("{e}"))),
                }
            }
        }
        Ok(None)
    }

    pub fn get_last_rollup_record(&self) -> Result<Option<RollupRecord>> {
        self.get_last_record::<RollupRecord>(self.config.rollup_store_cf_name.as_str())
    }

    fn scan_records<T>(&self, cf: &str, from: u32, limit: u32) -> Result<Vec<T>>
    where
        T: Message + std::default::Default,
    {
        if limit > self.config.scan_max_limit as u32 {
            return Err(DB3Error::ReadStoreError(
                "reach the scan max limit".to_string(),
            ));
        }
        let cf_handle = self
            .se
            .cf_handle(cf)
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it = self.se.raw_iterator_cf(&cf_handle);
        it.seek_to_last();
        let mut records: Vec<T> = Vec::new();
        let mut count: u32 = 0;
        let start: u32 = from;
        let end: u32 = from + limit;
        while it.valid() && count < start {
            count += 1;
            it.prev();
        }
        while it.valid() && count < end {
            count += 1;
            if let Some(v) = it.value() {
                if let Ok(m) = T::decode(v) {
                    records.push(m);
                }
            }
            it.prev();
        }
        Ok(records)
    }

    pub fn scan_rollup_records(&self, from: u32, limit: u32) -> Result<Vec<RollupRecord>> {
        self.scan_records::<RollupRecord>(self.config.rollup_store_cf_name.as_str(), from, limit)
    }

    pub fn scan_mutation_headers(&self, from: u32, limit: u32) -> Result<Vec<MutationHeader>> {
        self.scan_records::<MutationHeader>(self.config.block_store_cf_name.as_str(), from, limit)
    }

    pub fn scan_gc_records(&self, from: u32, limit: u32) -> Result<Vec<GCRecord>> {
        self.scan_records::<GCRecord>(self.config.gc_cf_name.as_str(), from, limit)
    }

    /// increase the block number and reset the order to 0
    /// return the last block state
    pub fn increase_block_return_last_state(&self) -> Result<(u64, u32)> {
        match self.block_state.lock() {
            Ok(mut state) => {
                let block_key: &str = "block_key";
                let last_block_state = (state.block, state.order);
                state.block += 1;
                state.order = 0;
                let cf_handle = self
                    .se
                    .cf_handle(self.config.block_state_cf_name.as_str())
                    .ok_or(DB3Error::WriteStoreError("cf is not found".to_string()))?;
                let mut batch = WriteBatch::default();
                batch.put_cf(&cf_handle, block_key.as_bytes(), state.block.to_be_bytes());
                self.se
                    .write(batch)
                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                Ok(last_block_state)
            }
            Err(e) => Err(DB3Error::WriteStoreError(format!("{e}"))),
        }
    }

    pub fn get_current_block(&self) -> Result<u64> {
        match self.block_state.lock() {
            Ok(state) => Ok(state.block),
            Err(e) => Err(DB3Error::WriteStoreError(format!("{e}"))),
        }
    }

    fn increase_order(&self) -> Result<(u64, u32)> {
        match self.block_state.lock() {
            Ok(mut state) => {
                state.order += 1;
                Ok((state.block, state.order))
            }
            Err(e) => Err(DB3Error::WriteStoreError(format!("{e}"))),
        }
    }

    fn set_block_state(&self, block: u64, order: u32) -> Result<()> {
        match self.block_state.lock() {
            Ok(mut state) => {
                state.block = block;
                state.order = order;
                Ok(())
            }
            Err(e) => Err(DB3Error::WriteStoreError(format!("{e}"))),
        }
    }

    pub fn gc_range_mutation(&self, block_start: u64, block_end: u64) -> Result<()> {
        if block_start >= block_end {
            return Err(DB3Error::ReadStoreError("invalid block range".to_string()));
        }
        let tx_cf_handle = self
            .se
            .cf_handle(self.config.tx_store_cf_name.as_str())
            .ok_or(DB3Error::WriteStoreError("cf is not found".to_string()))?;
        let mutations = self.get_range_mutations(block_start, block_end)?;
        let mut batch = WriteBatch::default();
        mutations
            .iter()
            .for_each(|ref x| batch.delete_cf(&tx_cf_handle, x.0.id.as_str()));
        let block_cf_handle = self
            .se
            .cf_handle(self.config.block_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        batch.delete_range_cf(
            &block_cf_handle,
            &block_start.to_be_bytes(),
            &block_end.to_be_bytes(),
        );
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        Ok(())
    }

    pub fn get_range_mutations(
        &self,
        block_start: u64,
        block_end: u64,
    ) -> Result<Vec<(MutationHeader, MutationBody)>> {
        // the block_start should be less than the block end
        if block_start >= block_end {
            return Err(DB3Error::ReadStoreError("invalid block range".to_string()));
        }
        let block_cf_handle = self
            .se
            .cf_handle(self.config.block_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let mut it = self.se.raw_iterator_cf(&block_cf_handle);
        let mut start_id: Vec<u8> = Vec::new();
        start_id.extend_from_slice(&block_start.to_be_bytes());
        start_id.extend_from_slice(&0_u32.to_be_bytes());
        let mut end_id: Vec<u8> = Vec::new();
        end_id.extend_from_slice(&block_end.to_be_bytes());
        end_id.extend_from_slice(&0_u32.to_be_bytes());
        let end_id_ref: &[u8] = &end_id;
        it.seek(&start_id);
        let mut mutations: Vec<(MutationHeader, MutationBody)> = Vec::new();
        loop {
            if !it.valid() {
                break;
            }
            if let Some(k) = it.key() {
                if k >= end_id_ref {
                    break;
                }
                if let Some(v) = it.value() {
                    if let Ok(m) = MutationHeader::decode(v) {
                        let tx_id = TxId::try_from_hex(m.id.as_str())
                            .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
                        if let Ok(Some(mb)) = self.get_mutation(&tx_id) {
                            mutations.push((m, mb));
                        }
                    }
                }
            } else {
                break;
            }
            it.next()
        }
        Ok(mutations)
    }

    pub fn get_mutation_header(&self, block: u64, order: u32) -> Result<Option<MutationHeader>> {
        let mut encoded_id: Vec<u8> = Vec::new();
        encoded_id.extend_from_slice(&block.to_be_bytes());
        encoded_id.extend_from_slice(&order.to_be_bytes());
        let block_cf_handle = self
            .se
            .cf_handle(self.config.block_store_cf_name.as_str())
            .ok_or(DB3Error::ReadStoreError("cf is not found".to_string()))?;
        let value = self
            .se
            .get_cf(&block_cf_handle, &encoded_id)
            .map_err(|e| DB3Error::ReadStoreError(format!("{e}")))?;
        if let Some(v) = value {
            match MutationHeader::decode(v.as_ref()) {
                Ok(m) => Ok(Some(m)),
                Err(e) => Err(DB3Error::ReadStoreError(format!("{e}"))),
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_mutation(&self, tx_id: &TxId) -> Result<Option<MutationBody>> {
        self.get_record::<MutationBody>(self.config.tx_store_cf_name.as_str(), tx_id.as_ref())
    }

    pub fn generate_mutation_block_and_order(
        &self,
        payload: &[u8],
        signature: &str,
    ) -> Result<(String, u64, u32)> {
        let tx_id = TxId::from((payload, signature.as_bytes()));
        let hex_id = tx_id.to_hex();
        let (block, order) = self.increase_order()?;
        Ok((hex_id, block, order))
    }

    fn encode_mutation_header(
        &self,
        hex_id: &str,
        mutation_body_size: u32,
        doc_ids_map: &str,
        sender: &DB3Address,
        nonce: u64,
        block: u64,
        order: u32,
        network: u64,
        action: MutationAction,
    ) -> Result<Bytes> {
        let mutation_header = MutationHeader {
            block_id: block,
            order_id: order,
            sender: sender.as_ref().to_vec(),
            time: times::get_current_time_in_secs(),
            id: hex_id.to_string(),
            size: mutation_body_size,
            nonce,
            network,
            action: action.into(),
            doc_ids_map: doc_ids_map.to_string(),
        };
        let mut header_buf = BytesMut::with_capacity(1024);
        mutation_header
            .encode(&mut header_buf)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        Ok(header_buf.freeze())
    }

    fn encode_mutation_body(&self, payload: &[u8], signature: &str) -> Result<Bytes> {
        let mutation_body = MutationBody {
            payload: payload.to_vec(),
            signature: signature.to_string(),
        };
        let mut buf = BytesMut::with_capacity(self.config.message_max_buffer);
        mutation_body
            .encode(&mut buf)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        Ok(buf.freeze())
    }
    pub fn add_mutation(
        &self,
        payload: &[u8],
        signature: &str,
        doc_ids_map: &str,
        sender: &DB3Address,
        nonce: u64,
        block: u64,
        order: u32,
        network: u64,
        action: MutationAction,
    ) -> Result<(String, u64, u32)> {
        let tx_id = TxId::from((payload, signature.as_bytes()));
        let hex_id = tx_id.to_hex();
        let mut encoded_id: Vec<u8> = Vec::new();
        encoded_id.extend_from_slice(&block.to_be_bytes());
        encoded_id.extend_from_slice(&order.to_be_bytes());
        let buf = self.encode_mutation_body(payload, signature)?;
        let header_buf = self.encode_mutation_header(
            hex_id.as_str(),
            buf.len() as u32,
            doc_ids_map,
            sender,
            nonce,
            block,
            order,
            network,
            action,
        )?;
        let tx_cf_handle = self
            .se
            .cf_handle(self.config.tx_store_cf_name.as_str())
            .ok_or(DB3Error::WriteStoreError("cf is not found".to_string()))?;
        let block_cf_handle = self
            .se
            .cf_handle(self.config.block_store_cf_name.as_str())
            .ok_or(DB3Error::WriteStoreError("cf is not found".to_string()))?;
        let mut batch = WriteBatch::default();
        // store the mutation body
        batch.put_cf(&tx_cf_handle, &tx_id, buf.as_ref());
        // store the mutation header
        batch.put_cf(&block_cf_handle, &encoded_id, header_buf.as_ref());
        self.se
            .write(batch)
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
        self.total_mutation_bytes
            .fetch_add((buf.len() + header_buf.len()) as u64, Ordering::Relaxed);
        Ok((hex_id, block, order))
    }

    pub fn update_mutation_stat(
        &self,
        payload: &[u8],
        signature: &str,
        doc_ids_map: &str,
        sender: &DB3Address,
        nonce: u64,
        block: u64,
        order: u32,
        network: u64,
        action: MutationAction,
    ) -> Result<()> {
        let tx_id = TxId::from((payload, signature.as_bytes()));
        let hex_id = tx_id.to_hex();
        let buf = self.encode_mutation_body(payload, signature)?;
        let header_buf = self.encode_mutation_header(
            hex_id.as_str(),
            buf.len() as u32,
            doc_ids_map,
            sender,
            nonce,
            block,
            order,
            network,
            action,
        )?;
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
        self.total_mutation_bytes
            .fetch_add((buf.len() + header_buf.len()) as u64, Ordering::Relaxed);
        self.set_block_state(block, order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    #[test]
    fn test_new_mutation_store() {
        let tmp_dir_path = TempDir::new("new_mutation_store_path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = MutationStoreConfig {
            db_path: real_path,
            block_store_cf_name: "cf1".to_string(),
            tx_store_cf_name: "cf2".to_string(),
            rollup_store_cf_name: "rf3".to_string(),
            gc_cf_name: "gc".to_string(),
            message_max_buffer: 4 * 1024,
            scan_max_limit: 50,
            block_state_cf_name: "block_state_cf".to_string(),
        };
        if let Err(e) = MutationStore::new(config) {
            println!("{:?}", e);
        }
    }

    #[test]
    fn test_scan_mutation() {
        let tmp_dir_path = TempDir::new("scan mutation store path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = MutationStoreConfig {
            db_path: real_path,
            block_store_cf_name: "cf1".to_string(),
            tx_store_cf_name: "cf2".to_string(),
            rollup_store_cf_name: "rf3".to_string(),
            gc_cf_name: "gc".to_string(),
            message_max_buffer: 4 * 1024,
            scan_max_limit: 50,
            block_state_cf_name: "block_state_cf".to_string(),
        };
        let result = MutationStore::new(config);
        assert!(result.is_ok());
        if let Ok(store) = result {
            let payload: Vec<u8> = vec![1];
            let signature: &str = "0xasdasdsad";
            let (_id, block, order) = store
                .generate_mutation_block_and_order(payload.as_ref(), signature)
                .unwrap();
            let result = store.add_mutation(
                payload.as_ref(),
                signature,
                "",
                &DB3Address::ZERO,
                1,
                block,
                order,
                1,
                MutationAction::CreateDocumentDb,
            );
            assert!(result.is_ok());
            if let Ok(headers) = store.scan_mutation_headers(0, 1) {
                assert_eq!(1, headers.len());
            } else {
                assert!(false);
            }
            let (_id, block, order) = store
                .generate_mutation_block_and_order(payload.as_ref(), signature)
                .unwrap();
            let result = store.add_mutation(
                payload.as_ref(),
                signature,
                "",
                &DB3Address::ZERO,
                1,
                block,
                order,
                1,
                MutationAction::CreateDocumentDb,
            );
            assert!(result.is_ok());
            if let Ok(headers) = store.scan_mutation_headers(0, 1) {
                assert_eq!(1, headers.len());
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_add_and_get_rollup_record() {
        let tmp_dir_path = TempDir::new("rollup").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = MutationStoreConfig {
            db_path: real_path,
            block_store_cf_name: "cf1".to_string(),
            tx_store_cf_name: "cf2".to_string(),
            rollup_store_cf_name: "rf3".to_string(),
            gc_cf_name: "gc".to_string(),
            message_max_buffer: 4 * 1024,
            scan_max_limit: 50,
            block_state_cf_name: "block_state_cf".to_string(),
        };
        let result = MutationStore::new(config);
        assert!(result.is_ok());
        if let Ok(store) = result {
            let result = store.get_last_rollup_record();
            assert!(result.is_ok());
            if let Ok(None) = result {
            } else {
                assert!(false);
            }
            let record = RollupRecord {
                end_block: 1,
                raw_data_size: 10,
                compress_data_size: 1,
                processed_time: 1,
                arweave_tx: "xx".to_string(),
                time: 111,
                mutation_count: 1,
                cost: 11111,
                start_block: 1,
                evm_cost: 1,
                evm_tx: "".to_string(),
            };
            let result = store.add_rollup_record(&record);
            assert!(result.is_ok());
            let result = store.get_last_rollup_record();
            if let Ok(Some(r)) = result {
                assert_eq!(r.end_block, 1);
            } else {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_range_mutations() {
        let tmp_dir_path = TempDir::new("range store path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = MutationStoreConfig {
            db_path: real_path,
            block_store_cf_name: "cf1".to_string(),
            tx_store_cf_name: "cf2".to_string(),
            rollup_store_cf_name: "rf3".to_string(),
            gc_cf_name: "gc".to_string(),
            message_max_buffer: 4 * 1024,
            scan_max_limit: 50,
            block_state_cf_name: "block_state_cf".to_string(),
        };
        let result = MutationStore::new(config);
        assert!(result.is_ok());
        if let Ok(store) = result {
            let result = store.get_range_mutations(0, 10);
            if let Ok(r) = result {
                assert_eq!(0, r.len());
            } else {
                assert!(false);
            }
            let payload: Vec<u8> = vec![1];
            let signature: &str = "0xasdasdsad";
            let (_id, block, order) = store
                .generate_mutation_block_and_order(payload.as_ref(), signature)
                .unwrap();
            let result = store.add_mutation(
                payload.as_ref(),
                signature,
                "",
                &DB3Address::ZERO,
                1,
                block,
                order,
                1,
                MutationAction::CreateDocumentDb,
            );
            assert!(result.is_ok());
            let result = store.get_range_mutations(0, 1);
            if let Ok(r) = result {
                assert_eq!(1, r.len());
            } else {
                assert!(false);
            }
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_add_mutation() {
        let tmp_dir_path = TempDir::new("add mutation store path").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = MutationStoreConfig {
            db_path: real_path,
            block_store_cf_name: "cf1".to_string(),
            tx_store_cf_name: "cf2".to_string(),
            rollup_store_cf_name: "rf3".to_string(),
            gc_cf_name: "gc".to_string(),
            message_max_buffer: 4 * 1024,
            scan_max_limit: 50,
            block_state_cf_name: "block_state_cf".to_string(),
        };
        let result = MutationStore::new(config);
        assert!(result.is_ok());
        if let Ok(store) = result {
            let payload: Vec<u8> = vec![1];
            let signature: &str = "0xasdasdsad";
            let (_id, block, order) = store
                .generate_mutation_block_and_order(payload.as_ref(), signature)
                .unwrap();
            let result = store.add_mutation(
                payload.as_ref(),
                signature,
                "",
                &DB3Address::ZERO,
                1,
                block,
                order,
                1,
                MutationAction::CreateDocumentDb,
            );
            assert!(result.is_ok());
            if let Ok((id, block, order)) = result {
                if let Ok(Some(v)) = store.get_mutation_header(block, order) {
                    assert_eq!(DB3Address::ZERO.as_ref(), &v.sender);
                } else {
                    assert!(false);
                }
                println!("{id}");
                let tx_id = TxId::try_from_hex(id.as_str()).unwrap();
                if let Ok(Some(m)) = store.get_mutation(&tx_id) {
                    assert_eq!(m.signature.as_str(), signature);
                } else {
                    assert!(false);
                }
            }
            let state = store.get_latest_state();
            assert_eq!(1, state.mutation_count);
            assert!(state.total_mutation_bytes > 0);
        } else {
            assert!(false);
        }
    }
}
