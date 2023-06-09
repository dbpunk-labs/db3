//
// rollup_executor.rs
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

use arrow::array::{ArrayRef, BinaryBuilder, StringBuilder, UInt32Builder, UInt64Builder};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use arweave_rs::Arweave;
use db3_base::times;
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_v2_proto::{MutationBody, MutationHeader};
use db3_proto::db3_rollup_proto::RollupRecord;
use db3_storage::mutation_store::MutationStore;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::GzipLevel;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tempdir::TempDir;
use tracing::info;

#[derive(Clone)]
pub struct RollupExecutorConfig {
    // the interval in ms
    pub rollup_interval: u64,
    pub temp_data_path: String,
    pub ar_key_path: String,
    pub ar_node_url: String,
}

pub struct RollupExecutor {
    config: RollupExecutorConfig,
    storage: MutationStore,
    schema: SchemaRef,
    arweave: Arweave,
}

impl RollupExecutor {
    pub fn new(config: RollupExecutorConfig, storage: MutationStore) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("signature", DataType::Utf8, true),
            Field::new("block", DataType::UInt64, true),
            Field::new("order", DataType::UInt32, true),
        ]));
        let arweave_url = url::Url::from_str(config.ar_node_url.as_str())
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let path = Path::new(config.ar_key_path.as_str());
        let arweave = Arweave::from_keypair_path(path, arweave_url)
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        info!(
            "start rollup executor with ar account {}",
            arweave.get_wallet_address().as_str()
        );
        Ok(Self {
            config,
            storage,
            schema,
            arweave,
        })
    }

    fn convert_to_recordbatch(
        &self,
        mutations: &[(MutationHeader, MutationBody)],
    ) -> Result<RecordBatch> {
        //TODO limit the memory usage
        let mut payload_builder = BinaryBuilder::new();
        let mut signature_builder = StringBuilder::new();
        let mut block_builder = UInt64Builder::new();
        let mut order_builder = UInt32Builder::new();
        for (header, body) in mutations {
            let body_ref: &[u8] = &body.payload;
            payload_builder.append_value(body_ref);
            signature_builder.append_value(body.signature.as_str());
            block_builder.append_value(header.block_id);
            order_builder.append_value(header.order_id);
        }
        let array_refs: Vec<ArrayRef> = vec![
            Arc::new(payload_builder.finish()),
            Arc::new(signature_builder.finish()),
            Arc::new(block_builder.finish()),
            Arc::new(order_builder.finish()),
        ];
        let record_batch = RecordBatch::try_new(self.schema.clone(), array_refs)
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        info!(
            "convert {} into recordbatch with memory {}",
            mutations.len(),
            record_batch.get_array_memory_size()
        );
        Ok(record_batch)
    }

    fn dump_recordbatch(&self, path: &Path, recordbatch: &RecordBatch) -> Result<(u64, u64)> {
        let properties = WriterProperties::builder()
            .set_compression(Compression::GZIP(GzipLevel::default()))
            .build();
        let fd = File::create(path).map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let mut writer = ArrowWriter::try_new(fd, self.schema.clone(), Some(properties))
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        writer
            .write(recordbatch)
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let meta = writer
            .close()
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let metadata =
            std::fs::metadata(path).map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        Ok((meta.num_rows as u64, metadata.len()))
    }

    async fn upload_data(&self, path: &Path) -> Result<(String, u64)> {
        let metadata =
            std::fs::metadata(path).map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let fee = self
            .arweave
            .get_fee_by_size(metadata.len())
            .await
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        //TODO add app name
        self.arweave
            .upload_file_from_path(path, vec![], fee)
            .await
            .map_err(|e| DB3Error::RollupError(format!("{e}")))
    }

    pub async fn process(&self) -> Result<()> {
        let next_rollup_start_block = match self.storage.get_last_rollup_record()? {
            Some(r) => r.end_block + 1,
            _ => 0_u64,
        };
        let current_block = self.storage.get_current_block()?;
        if current_block <= next_rollup_start_block {
            info!("no block to rollup");
            return Ok(());
        }
        let now = Instant::now();
        info!("the next rollup start block {next_rollup_start_block} and the newest block {current_block}");
        let mutations = self
            .storage
            .get_range_mutations(next_rollup_start_block, current_block)?;
        if mutations.len() <= 0 {
            info!("no block to rollup");
            return Ok(());
        }
        let recordbatch = self.convert_to_recordbatch(&mutations)?;
        let memory_size = recordbatch.get_array_memory_size();
        let tmp_dir = TempDir::new_in(&self.config.temp_data_path, "compression")
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let file_path = tmp_dir.path().join("rollup.parquet.gz");
        let (num_rows, size) = self.dump_recordbatch(&file_path, &recordbatch)?;
        let (id, reward) = self.upload_data(&file_path).await?;
        info!("the process rollup done with num mutations {num_rows}, raw data size {memory_size}, compress data size {size} and processed time {} id {} cost {}", now.elapsed().as_secs(),
        id.as_str(), reward
        );
        let record = RollupRecord {
            end_block: current_block - 1,
            raw_data_size: memory_size as u64,
            compress_data_size: size,
            processed_time: now.elapsed().as_secs(),
            arweave_tx: id,
            time: times::get_current_time_in_secs(),
            mutation_count: num_rows,
            cost: reward,
        };
        self.storage
            .add_rollup_record(&record)
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        Ok(())
    }
}
