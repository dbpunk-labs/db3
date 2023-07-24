//
// ar_toolbox.rs
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

use arrow::array::{
    ArrayRef, BinaryArray, BinaryBuilder, StringArray, StringBuilder, UInt32Array, UInt32Builder,
    UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_v2_proto::{MutationBody, MutationHeader};
use db3_storage::ar_fs::ArFileSystem;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tempdir::TempDir;
use tracing::{debug, info};

pub struct ArToolBox {
    pub schema: SchemaRef,
    pub ar_filesystem: ArFileSystem,
    pub temp_data_path: String,
}

unsafe impl Send for ArToolBox {}
unsafe impl Sync for ArToolBox {}

impl ArToolBox {
    pub fn new(ar_filesystem: ArFileSystem, temp_data_path: String) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("signature", DataType::Utf8, true),
            Field::new("block", DataType::UInt64, true),
            Field::new("order", DataType::UInt32, true),
            Field::new("doc_ids", DataType::Utf8, true),
        ]));
        Ok(Self {
            schema,
            ar_filesystem,
            temp_data_path,
        })
    }

    pub async fn download_and_parse_record_batch(&self, tx: &str) -> Result<Vec<RecordBatch>> {
        debug!("Downloading tx {}", tx);
        let tmp_dir = TempDir::new_in(&self.temp_data_path, "download")
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let file_path = tmp_dir.path().join(format!("{}.gz.parquet", tx));
        self.ar_filesystem
            .download_file(file_path.as_path(), tx)
            .await?;
        Self::parse_gzip_file(file_path.as_path())
    }

    pub async fn get_tx_tags(
        &self,
        tx_id: &str,
    ) -> Result<(u64, u64, Option<String>, Option<String>)> {
        let tags = self.ar_filesystem.get_tags(tx_id).await?;
        let mut last_rollup_tx = None;
        let mut start_block = None;
        let mut end_block = None;
        let mut version_id = None;
        for tag in tags {
            if let Ok(name) = tag.name.to_utf8_string() {
                if name == "Last-Rollup-Tx" {
                    last_rollup_tx = Some(
                        tag.value
                            .to_utf8_string()
                            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?,
                    );
                }
                if name == "Start-Block" {
                    let start_block_str = tag
                        .value
                        .to_utf8_string()
                        .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                    start_block = Some(
                        start_block_str
                            .parse::<u64>()
                            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?,
                    );
                }
                if name == "End-Block" {
                    let end_block_str = tag
                        .value
                        .to_utf8_string()
                        .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                    end_block = Some(
                        end_block_str
                            .parse::<u64>()
                            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?,
                    );
                }
                if name == "Version-Id" {
                    version_id = Some(
                        tag.value
                            .to_utf8_string()
                            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?,
                    );
                }
            }
        }
        if start_block.is_none() || end_block.is_none() {
            return Err(DB3Error::ArwareOpError(format!(
                "Missing start or end block for tx {}",
                tx_id
            )));
        }
        Ok((
            start_block.unwrap(),
            end_block.unwrap(),
            last_rollup_tx,
            version_id,
        ))
    }
    pub async fn get_version_id(&self, tx_id: &str) -> Result<Option<String>> {
        let tags = self.ar_filesystem.get_tags(tx_id).await?;
        for tag in tags {
            if let Ok(name) = tag.name.to_utf8_string() {
                if name == "Version-Id" {
                    let version = tag
                        .value
                        .to_utf8_string()
                        .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                    return Ok(Some(version));
                }
            }
        }
        return Ok(None);
    }
    pub async fn get_start_block(&self, tx_id: &str) -> Result<Option<String>> {
        let tags = self.ar_filesystem.get_tags(tx_id).await?;
        for tag in tags {
            if let Ok(name) = tag.name.to_utf8_string() {
                if name == "Start-Block" {
                    return Ok(Some(
                        tag.value
                            .to_utf8_string()
                            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?,
                    ));
                }
            }
        }
        Ok(None)
    }

    pub async fn compress_and_upload_record_batch(
        &self,
        tx: String,
        last_end_block: u64,
        current_block: u64,
        recordbatch: &RecordBatch,
        network_id: u64,
    ) -> Result<(String, u64, u64, u64)> {
        let tmp_dir = TempDir::new_in(&self.temp_data_path, "compression")
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let file_path = tmp_dir.path().join("rollup.gz.parquet");
        let (num_rows, size) = Self::dump_recordbatch(&file_path, recordbatch)?;
        let filename = format!("{}_{}.gz.parquet", last_end_block, current_block);
        //TODO add tx status confirmation
        let balance = self.ar_filesystem.get_balance().await?;
        info!("Start to upload_file with balance: {:?}", balance);
        let (id, reward) = self
            .ar_filesystem
            .upload_file(
                &file_path,
                tx.as_str(),
                last_end_block,
                current_block,
                network_id,
                filename.as_str(),
            )
            .await?;
        Ok((id, reward, num_rows, size))
    }

    /// Compress recordbatch to parquet file
    pub fn dump_recordbatch(path: &Path, recordbatch: &RecordBatch) -> Result<(u64, u64)> {
        let properties = WriterProperties::builder()
            .set_compression(Compression::GZIP(GzipLevel::default()))
            .build();
        let fd = File::create(path).map_err(|e| DB3Error::RollupError(format!("{e}")))?;

        let mut writer = ArrowWriter::try_new(fd, recordbatch.schema(), Some(properties))
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

    /// Parse recordbatch from parquet file
    pub fn parse_gzip_file(path: &Path) -> Result<Vec<RecordBatch>> {
        let fd = File::open(path).map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        // Create a sync parquet reader with batch_size.
        // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(fd)
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?
            .with_batch_size(8192)
            .build()
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;

        let mut batches = Vec::new();

        for batch in parquet_reader {
            let each = batch.map_err(|e| DB3Error::RollupError(format!("{e}")))?;
            batches.push(each);
        }
        Ok(batches)
    }

    /// Parse mutation body, block and order from recordbatch
    pub fn convert_recordbatch_to_mutation(
        record_batch: &RecordBatch,
        version: Option<String>,
    ) -> Result<Vec<(MutationBody, u64, u32, String)>> {
        debug!("convert_recordbatch_to_mutation version {:?}", version);
        let mut mutations = Vec::new();
        let payloads = record_batch
            .column_by_name("payload")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let signatures = record_batch
            .column_by_name("signature")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let blocks = record_batch
            .column_by_name("block")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let orders = record_batch
            .column_by_name("order")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let doc_ids_opt = match version {
            Some(_) => Some(
                record_batch
                    .column_by_name("doc_ids")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap(),
            ),
            None => None,
        };

        for i in 0..record_batch.num_rows() {
            let payload = payloads.value(i);
            let signature = signatures.value(i);
            let block = blocks.value(i);
            let order = orders.value(i);
            let mutation = MutationBody {
                payload: payload.to_vec(),
                signature: signature.to_string(),
            };
            let doc_ids = match doc_ids_opt {
                Some(doc_ids) => doc_ids.value(i),
                None => "",
            };
            mutations.push((mutation, block, order, doc_ids.to_string()));
        }
        Ok(mutations)
    }

    /// convert mutation to recordbatch
    /// encode mutation body, block and order to recordbatch
    pub fn convert_mutations_to_recordbatch(
        &self,
        mutations: &[(MutationHeader, MutationBody)],
    ) -> Result<RecordBatch> {
        //TODO limit the memory usage
        let mut payload_builder = BinaryBuilder::new();
        let mut signature_builder = StringBuilder::new();
        let mut block_builder = UInt64Builder::new();
        let mut order_builder = UInt32Builder::new();
        let mut docids_builder = StringBuilder::new();
        for (header, body) in mutations {
            let body_ref: &[u8] = &body.payload;
            payload_builder.append_value(body_ref);
            signature_builder.append_value(body.signature.as_str());
            block_builder.append_value(header.block_id);
            order_builder.append_value(header.order_id);
            docids_builder.append_value(header.doc_ids_map.as_str());
        }
        let array_refs: Vec<ArrayRef> = vec![
            Arc::new(payload_builder.finish()),
            Arc::new(signature_builder.finish()),
            Arc::new(block_builder.finish()),
            Arc::new(order_builder.finish()),
            Arc::new(docids_builder.finish()),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray, StringArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use db3_storage::ar_fs::ArFileSystemConfig;
    use std::env;
    use std::path::PathBuf;
    use tempdir::TempDir;
    use tokio::time::{sleep, Duration as TokioDuration};

    fn mock_batch_record() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Binary, true),
            Field::new("signature", DataType::Utf8, true),
            Field::new("block", DataType::UInt64, true),
            Field::new("order", DataType::UInt32, true),
        ]));
        let mut payload_builder = BinaryBuilder::new();
        let mut signature_builder = StringBuilder::new();
        let mut block_builder = UInt64Builder::new();
        let mut order_builder = UInt32Builder::new();
        for block in 0..10 {
            let body_ref: &[u8] = "this is a payload sample".as_bytes();
            payload_builder.append_value(body_ref);
            signature_builder.append_value("0x1234567890");
            block_builder.append_value(block);
            order_builder.append_value((block * 10) as u32);
        }
        let array_refs: Vec<ArrayRef> = vec![
            Arc::new(payload_builder.finish()),
            Arc::new(signature_builder.finish()),
            Arc::new(block_builder.finish()),
            Arc::new(order_builder.finish()),
        ];
        RecordBatch::try_new(schema.clone(), array_refs).unwrap()
    }
    #[test]
    fn dump_recordbatch_ut() {
        let tmp_dir_path = TempDir::new("dump_recordbatch_ut").expect("create temp dir");
        let record_batch = mock_batch_record();
        let (num_rows, size) = ArToolBox::dump_recordbatch(
            Path::new(tmp_dir_path.path().join("test.parquet").to_str().unwrap()),
            &record_batch,
        )
        .unwrap();
        assert_eq!(num_rows, 10);
        assert_eq!(size, 1862);
    }

    #[test]
    fn parse_gzip_file_ut() {
        let tmp_dir_path = TempDir::new("dump_recordbatch_ut").expect("create temp dir");
        let parquet_file = tmp_dir_path.path().join("test.parquet");
        let record_batch = mock_batch_record();
        let (num_rows, size) = ArToolBox::dump_recordbatch(&parquet_file, &record_batch).unwrap();
        assert_eq!(num_rows, 10);
        assert_eq!(size, 1862);
        let res = ArToolBox::parse_gzip_file(parquet_file.as_path()).unwrap();
        assert_eq!(res.len(), 1);
        let rec = res[0].clone();
        assert!(rec.num_columns() == 4);
        assert_eq!(rec.num_rows(), 10);
        let payloads = rec
            .column_by_name("payload")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(payloads.len(), 10);
        assert_eq!(payloads.value(5), "this is a payload sample".as_bytes());

        let signatures = rec
            .column_by_name("signature")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(signatures.len(), 10);
        assert_eq!(signatures.value(5), "0x1234567890");

        let blocks = rec
            .column_by_name("block")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(blocks.len(), 10);
        assert_eq!(blocks.value(5), 5);

        let orders = rec
            .column_by_name("order")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(orders.len(), 10);
        assert_eq!(orders.value(5), 50);
    }

    #[test]
    fn parse_sample_ar_parquet_ut() {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/test/37829_37968.gz.parquet");
        let res = ArToolBox::parse_gzip_file(path.as_path()).unwrap();
        assert_eq!(res.len(), 1);
        let rec = res[0].clone();
        assert_eq!(rec.num_columns(), 4);
        assert_eq!(rec.num_rows(), 204);

        let mutations = ArToolBox::convert_recordbatch_to_mutation(&rec, None).unwrap();
        assert_eq!(mutations.len(), 204);
        let (mutation, block, order, doc_ids) = mutations[0].clone();
        assert_eq!(block, 37829);
        assert_eq!(order, 1);
        assert_eq!(mutation.signature, "0xf6afe1165ae87fa09375eabccdedc61f3e5af4ed1e5c6456f1b63d397862252667e1f13f0f076f30609754f787c80135c52f7c249e95c9b8fab1b9ed27846c1b1c");
        assert!(doc_ids.is_empty())
    }

    #[tokio::test]
    async fn upload_to_ar_test() {
        let last_tx = "TY5SMaPPRk_TMvSDROaQWyc_WHyJrEL760-UhiNnHG4";
        let last_end_block: u64 = 0;
        let current_block: u64 = 1000;
        let network_id: u64 = 1;
        let record_batch = mock_batch_record();
        let temp_dir = TempDir::new("upload_arware_tx_ut").expect("create temp dir");
        let arweave_url = "http://127.0.0.1:1984";
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let key_root_path = path
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tools/keys")
            .to_str()
            .unwrap()
            .to_string();
        let config = ArFileSystemConfig {
            arweave_url: arweave_url.to_string(),
            key_root_path,
        };
        let ar_filesystem = ArFileSystem::new(config).unwrap();
        println!("ar address {}", ar_filesystem.get_address());
        let ar_toolbox =
            ArToolBox::new(ar_filesystem, temp_dir.path().to_str().unwrap().to_string()).unwrap();
        let result = ar_toolbox
            .compress_and_upload_record_batch(
                last_tx.to_string(),
                last_end_block,
                current_block,
                &record_batch,
                network_id,
            )
            .await;
        assert_eq!(true, result.is_ok());
        let (tx, _, rows, _) = result.unwrap();
        println!("{tx}");
        sleep(TokioDuration::from_millis(5 * 1000)).await;
        let res = ar_toolbox
            .download_and_parse_record_batch(tx.as_str())
            .await
            .unwrap();
        let rec1 = res[0].clone();
        assert_eq!(rows, rec1.num_rows() as u64);
        {
            let (start_block, end_block, last_rollup_tx, _) =
                ar_toolbox.get_tx_tags(tx.as_str()).await.unwrap();
            assert_eq!(start_block, last_end_block);
            assert_eq!(end_block, current_block);
            assert_eq!(last_rollup_tx, Some(last_tx.to_string()));
        }
    }
}
