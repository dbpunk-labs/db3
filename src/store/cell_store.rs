//
//
// cell_store.rs
// Copyright (C) 2022 rtstore.io Author imotai <codego.me@gmail.com>
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

use crate::base::filesystem::{FileSystem, SyncPosixFileSystem, WritableFileWriter};
use crate::base::linked_list::LinkedList;
use crate::codec::row_codec::{encode, Data, RowRecordBatch};
use crate::error::{RTStoreError, Result};
use arc_swap::ArcSwap;
use arrow::datatypes::SchemaRef;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::region::Region;
use std::fs;
use std::path::Path;
use std::str::Utf8Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
uselog!(info);

/// Config for CellStore
/// TODO add config for compaction
pub struct CellStoreConfig {
    // the bucket_name for cell store
    bucket_name: String,
    // the region of datacenter
    region: Region,
    // the schema of cell store
    schema: SchemaRef,
    // the path prefix of binlog
    local_binlog_path_prefix: String,
    // the auth config for s3
    auth: Credentials,
    //
    row_buffer_size: u32,
}

impl CellStoreConfig {
    fn new(
        bucket_name: &str,
        region: &str,
        schema: &SchemaRef,
        local_binlog_path_prefix: &str,
        auth: Credentials,
    ) -> Result<Self> {
        if bucket_name.is_empty() {
            return Err(RTStoreError::CellStoreInvalidConfigError {
                name: String::from("bucket_name"),
                err: String::from("empty name"),
            });
        }

        if region.is_empty() {
            return Err(RTStoreError::CellStoreInvalidConfigError {
                name: String::from("region"),
                err: String::from("empty string"),
            });
        }

        if schema.fields().is_empty() {
            return Err(RTStoreError::CellStoreInvalidConfigError {
                name: String::from("schema"),
                err: String::from("empty schema"),
            });
        }

        if local_binlog_path_prefix.is_empty() {
            return Err(RTStoreError::CellStoreInvalidConfigError {
                name: String::from("local_binlog_path_prefix"),
                err: String::from("empty string"),
            });
        }

        let result: std::result::Result<Region, Utf8Error> = region.parse();
        match result {
            Ok(s3_region) => Ok(Self {
                bucket_name: bucket_name.to_string(),
                region: s3_region,
                schema: schema.clone(),
                local_binlog_path_prefix: local_binlog_path_prefix.to_string(),
                auth,
                row_buffer_size: 10 * 1024,
            }),
            Err(e) => Err(RTStoreError::CellStoreInvalidConfigError {
                name: String::from("region"),
                err: e.to_string(),
            }),
        }
    }
}

struct CellStoreLockData {
    // the writer of binlog
    writer: Box<WritableFileWriter>,
}

/// the smallest unit for storing table data
pub struct CellStore {
    // the config of cell store
    config: CellStoreConfig,
    // the total rows that in memory
    total_rows_in_memory: AtomicU64,
    total_rows_on_external_storage: AtomicU64,
    // total data in bytes stored in memory
    total_data_in_memory: AtomicU64,
    // total data in bytes stored on external storage
    total_data_on_external_storage: AtomicU64,
    // the binlog size in bytes
    binlog_data_size: AtomicU64,
    // the index for log name, eg 00001.log
    log_counter: AtomicU64,
    // the handler of s3 bucket
    bucket: Bucket,
    lock_data: Arc<Mutex<CellStoreLockData>>,
    row_memtable: ArcSwap<LinkedList<RowRecordBatch>>,
}

impl CellStore {
    fn new(config: CellStoreConfig) -> Result<Self> {
        info!(
            "init a new cell store with bucket {} and region {}",
            config.bucket_name, config.region
        );

        let bucket = Bucket::new(
            &config.bucket_name,
            config.region.clone(),
            config.auth.clone(),
        )?;

        fs::create_dir_all(&config.local_binlog_path_prefix)?;
        let log_path_str = format!("{}/0000.binlog", config.local_binlog_path_prefix);
        let log_path = Path::new(&log_path_str);
        let fs = SyncPosixFileSystem {};
        let writer = fs.open_writable_file_writer(log_path)?;
        let bucket = bucket.with_path_style();
        //TODO recover some status data from persistence
        Ok(CellStore {
            config,
            total_rows_in_memory: AtomicU64::new(0),
            total_rows_on_external_storage: AtomicU64::new(0),
            total_data_in_memory: AtomicU64::new(0),
            total_data_on_external_storage: AtomicU64::new(0),
            binlog_data_size: AtomicU64::new(0),
            log_counter: AtomicU64::new(0),
            bucket,
            lock_data: Arc::new(Mutex::new(CellStoreLockData { writer })),
            row_memtable: ArcSwap::from(Arc::new(LinkedList::new())),
        })
    }

    async fn put(&self, path: &str, content: &[u8]) -> Result<(Vec<u8>, u16)> {
        let result = self.bucket.put_object(path, content).await;
        match result {
            Ok((v, size)) => Ok((v, size)),
            Err(e) => Err(RTStoreError::CellStoreS3Error(e)),
        }
    }

    pub async fn put_records(&self, records: RowRecordBatch) -> Result<()> {
        // load a row memtable reference
        let table = self.row_memtable.load();
        // encode row records to byte data
        let data = encode(&records)?;
        let size = records.batch.len();
        table.push_front(records)?;
        self.total_rows_in_memory
            .fetch_add(size as u64, Ordering::Relaxed);
        // save record to binlog
        if let Ok(mut guard) = self.lock_data.lock() {
            guard.writer.append(&data)
        } else {
            Err(RTStoreError::BaseBusyError(
                "fail to obtain lock".to_string(),
            ))
        }
    }

    #[inline(always)]
    pub fn get_total_rows_in_memory(&self) -> u64 {
        self.total_rows_in_memory.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use arrow::datatypes::*;
    use tempdir::TempDir;

    #[test]
    fn test_invalid_config() {
        let valid_schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int64, true)]));
        let auth = Credentials::from_env_specific(
            Some("AWS_S3_ACCESS_KEY"),
            Some("AWS_S3_SECRET_KEY"),
            None,
            None,
        )
        .unwrap();
        let bucket_name = "test_bk";
        let region = "cn";
        let schema = Arc::new(Schema::empty());
        let local_binlog_path_prefix = "/test/binlog";
        if CellStoreConfig::new(
            bucket_name,
            region,
            &schema,
            local_binlog_path_prefix,
            auth.clone(),
        )
        .is_ok()
        {
            panic!("should has some config error");
        }

        if CellStoreConfig::new(
            "",
            region,
            &valid_schema,
            local_binlog_path_prefix,
            auth.clone(),
        )
        .is_ok()
        {
            panic!("should has some config error");
        }

        if CellStoreConfig::new(
            bucket_name,
            "",
            &valid_schema,
            local_binlog_path_prefix,
            auth.clone(),
        )
        .is_ok()
        {
            panic!("should has some config error");
        }
        if CellStoreConfig::new(bucket_name, region, &valid_schema, "", auth.clone()).is_ok() {
            panic!("should has some config error");
        }
    }

    fn gen_a_normal_config() -> Result<CellStoreConfig> {
        let valid_schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int64, true)]));
        let auth = Credentials::from_env_specific(
            Some("AWS_S3_ACCESS_KEY"),
            Some("AWS_S3_SECRET_KEY"),
            None,
            None,
        )
        .unwrap();
        let bucket_name = "test_bk";
        let region = "http://127.0.0.1:9090";
        let local_binlog_path_prefix = "./test/binlog";
        CellStoreConfig::new(
            bucket_name,
            region,
            &valid_schema,
            local_binlog_path_prefix,
            auth,
        )
    }

    #[test]
    fn test_normal_config() {
        if gen_a_normal_config().is_err() {
            panic!("should be ok");
        }
    }

    #[test]
    fn test_init_cell_store() {
        let config = gen_a_normal_config().unwrap();
        if CellStore::new(config).is_err() {
            panic!("should be ok");
        }
    }

    #[tokio::test]
    async fn test_put_record() -> Result<()> {
        let mut config = gen_a_normal_config()?;
        let tmp_dir_path = TempDir::new("put_records").expect("create temp dir");
        if let Some(tmp_dir_path_str) = tmp_dir_path.path().to_str() {
            config.local_binlog_path_prefix = tmp_dir_path_str.to_string();
            if let Ok(c) = CellStore::new(config) {
                let batch = gen_sample_row_batch();
                if c.put_records(batch).await.is_err() {
                    panic!("should be ok")
                }
                assert_eq!(2, c.get_total_rows_in_memory());
            } else {
                panic!("should not be here");
            }
        } else {
            panic!("should not be here");
        }
        Ok(())
    }

    fn gen_sample_row_batch() -> RowRecordBatch {
        let batch = vec![
            vec![Data::Bool(true), Data::Int32(12)],
            vec![Data::Bool(false), Data::Int32(11)],
        ];
        RowRecordBatch {
            batch,
            schema_version: 1,
            id: "eth.price".to_string(),
        }
    }

    #[tokio::test]
    async fn test_put() {
        let config = gen_a_normal_config().unwrap();
        if let Ok(c) = CellStore::new(config) {
            let data = "hello".as_bytes();
            let path = "test/part_0000.parquet.gz";
            if let Err(e) = c.put(path, data).await {
                info!("put error {}", e);
                panic!("should put data ok");
            }
        } else {
            panic!("should be ok");
        }
    }
}
