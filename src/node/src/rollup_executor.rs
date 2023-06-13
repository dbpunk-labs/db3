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
use db3_base::times;
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_v2_proto::{MutationBody, MutationHeader};
use db3_proto::db3_rollup_proto::{GcRecord, RollupRecord};
use db3_storage::ar_fs::{ArFileSystem, ArFileSystemConfig};
use db3_storage::mutation_store::MutationStore;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::basic::GzipLevel;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tempdir::TempDir;
use tracing::{info, warn};

#[derive(Clone)]
pub struct RollupExecutorConfig {
    // the interval in ms
    pub rollup_interval: u64,
    pub temp_data_path: String,
    pub ar_key_path: String,
    pub ar_node_url: String,
    pub min_rollup_size: u64,
    pub min_gc_round_offset: u64,
}

pub struct RollupExecutor {
    config: RollupExecutorConfig,
    storage: MutationStore,
    schema: SchemaRef,
    network_id: u64,
    ar_filesystem: ArFileSystem,
}

impl RollupExecutor {
    pub fn new(
        config: RollupExecutorConfig,
        storage: MutationStore,
        network_id: u64,
    ) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("signature", DataType::Utf8, true),
            Field::new("block", DataType::UInt64, true),
            Field::new("order", DataType::UInt32, true),
        ]));
        let ar_fs_config = ArFileSystemConfig {
            wallet_path: config.ar_key_path.to_string(),
            arweave_url: config.ar_node_url.to_string(),
        };
        let ar_filesystem = ArFileSystem::new(ar_fs_config)?;
        Ok(Self {
            config,
            storage,
            schema,
            ar_filesystem,
            network_id,
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

    fn gc_mutation(&self) -> Result<()> {
        let (last_start_block, last_end_block, first) = match self.storage.get_last_gc_record()? {
            Some(r) => (r.start_block, r.end_block, false),
            None => (0_u64, 0_u64, true),
        };

        info!(
            "last gc block range [{}, {})",
            last_start_block, last_end_block
        );

        let now = Instant::now();
        if self
            .storage
            .has_enough_round_left(last_start_block, self.config.min_gc_round_offset)?
        {
            if first {
                if let Some(r) = self.storage.get_rollup_record(last_start_block)? {
                    self.storage.gc_range_mutation(r.start_block, r.end_block)?;
                    let record = GcRecord {
                        start_block: r.start_block,
                        end_block: r.end_block,
                        data_size: r.raw_data_size,
                        time: times::get_current_time_in_secs(),
                        processed_time: now.elapsed().as_secs(),
                    };
                    self.storage.add_gc_record(&record)?;
                    info!(
                        "gc mutation from block range [{}, {}) done",
                        r.start_block, r.end_block
                    );
                    Ok(())
                } else {
                    // going here is not normal case
                    warn!(
                        "fail to get next rollup record with start block {}",
                        last_start_block
                    );
                    Ok(())
                }
            } else {
                if let Some(r) = self.storage.get_next_rollup_record(last_start_block)? {
                    self.storage.gc_range_mutation(r.start_block, r.end_block)?;
                    let record = GcRecord {
                        start_block: r.start_block,
                        end_block: r.end_block,
                        data_size: r.raw_data_size,
                        time: times::get_current_time_in_secs(),
                        processed_time: now.elapsed().as_secs(),
                    };
                    self.storage.add_gc_record(&record)?;
                    info!(
                        "gc mutation from block range [{}, {}) done",
                        r.start_block, r.end_block
                    );
                    Ok(())
                } else {
                    // going here is not normal case
                    warn!(
                        "fail to get next rollup record with start block {}",
                        last_start_block
                    );
                    Ok(())
                }
            }
        } else {
            info!("not enough round to run gc");
            Ok(())
        }
    }

    pub async fn process(&self) -> Result<()> {
        let (_last_start_block, last_end_block, tx) = match self.storage.get_last_rollup_record()? {
            Some(r) => (r.start_block, r.end_block, r.arweave_tx.to_string()),
            _ => (0_u64, 0_u64, "".to_string()),
        };

        let current_block = self.storage.get_current_block()?;
        if current_block <= last_end_block {
            info!("no block to rollup");
            return Ok(());
        }

        let now = Instant::now();
        info!(
            "the next rollup start block {} and the newest block {current_block}",
            last_end_block
        );

        let mutations = self
            .storage
            .get_range_mutations(last_end_block, current_block)?;

        if mutations.len() <= 0 {
            info!("no block to rollup");
            return Ok(());
        }

        let recordbatch = self.convert_to_recordbatch(&mutations)?;
        let memory_size = recordbatch.get_array_memory_size();

        if memory_size < self.config.min_rollup_size as usize {
            info!(
                "there not enough data to trigger rollup, the min_rollup_size {}, current size {}",
                self.config.min_rollup_size, memory_size
            );
            return Ok(());
        }

        let tmp_dir = TempDir::new_in(&self.config.temp_data_path, "compression")
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let file_path = tmp_dir.path().join("rollup.gz.parquet");
        let (num_rows, size) = self.dump_recordbatch(&file_path, &recordbatch)?;
        let filename = format!("{}_{}.gz.parquet", last_end_block, current_block);
        //TODO add tx status confirmation
        let (id, reward) = self
            .ar_filesystem
            .upload_file(
                &file_path,
                tx.as_str(),
                last_end_block,
                current_block,
                self.network_id,
                filename.as_str(),
            )
            .await?;
        info!("the process rollup done with num mutations {num_rows}, raw data size {memory_size}, compress data size {size} and processed time {} id {} cost {}", now.elapsed().as_secs(),
        id.as_str(), reward
        );
        let record = RollupRecord {
            end_block: current_block,
            raw_data_size: memory_size as u64,
            compress_data_size: size,
            processed_time: now.elapsed().as_secs(),
            arweave_tx: id,
            time: times::get_current_time_in_secs(),
            mutation_count: num_rows,
            cost: reward,
            start_block: last_end_block,
        };
        self.storage
            .add_rollup_record(&record)
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        self.gc_mutation()?;
        Ok(())
    }
}
