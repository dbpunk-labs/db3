use arrow::array::{ArrayRef, BinaryBuilder, StringBuilder, UInt32Builder, UInt64Builder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_v2_proto::{MutationBody, MutationHeader};
use db3_storage::ar_fs::{ArFileSystem, ArFileSystemConfig};
use parquet::arrow::arrow_reader::{
    ArrowReaderBuilder, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel};
use parquet::file::properties::{ReaderProperties, WriterProperties};
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempdir::TempDir;
use tracing::info;
pub struct ArToolBox {
    pub network_id: Arc<AtomicU64>,
    pub schema: SchemaRef,
    pub ar_filesystem: ArFileSystem,
    pub temp_data_path: String,
}

impl ArToolBox {
    pub fn new(
        key_root_path: String,
        arweave_url: String,
        temp_data_path: String,
        schema: SchemaRef,
        network_id: Arc<AtomicU64>,
    ) -> Result<Self> {
        let ar_fs_config = ArFileSystemConfig {
            key_root_path,
            arweave_url,
        };
        let ar_filesystem = ArFileSystem::new(ar_fs_config)?;
        Ok(Self {
            network_id,
            schema,
            ar_filesystem,
            temp_data_path,
        })
    }
    pub async fn get_ar_account(&self) -> Result<(String, String)> {
        let addr = self.ar_filesystem.get_address();
        let balance = self.ar_filesystem.get_balance().await?;
        Ok((addr, balance.to_string()))
    }

    pub async fn compress_and_upload_record_batch(
        &self,
        tx: String,
        last_end_block: u64,
        current_block: u64,
        recordbatch: &RecordBatch,
    ) -> Result<(String, u64, u64, u64)> {
        let tmp_dir = TempDir::new_in(&self.temp_data_path, "compression")
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let file_path = tmp_dir.path().join("rollup.gz.parquet");
        let (num_rows, size) = Self::dump_recordbatch(&file_path, recordbatch)?;
        let filename = format!("{}_{}.gz.parquet", last_end_block, current_block);
        //TODO add tx status confirmation
        let (id, reward) = self
            .ar_filesystem
            .upload_file(
                &file_path,
                tx.as_str(),
                last_end_block,
                current_block,
                self.network_id.load(Ordering::Relaxed),
                filename.as_str(),
            )
            .await?;
        Ok((id, reward, num_rows, size))
    }

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

    pub fn convert_to_recordbatch(
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, StringArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    #[test]
    fn it_works() {}

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
        println!("rec: {:?}", rec);
    }

    // #[test]
    // fn parse_sample_ar_parquet_ut() {
    //     let res = ArToolBox::parse_gzip_file(Path::new("/Users/chenjing/work/dbpunk/db3/37829_37968.gz.parquet")).unwrap();
    //     assert!(res.num_columns() == 4);
    //     println!("res: {:?}", res);
    // }
}
