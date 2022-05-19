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

use crate::base::filesystem::{FileSystem, SyncPosixFileSystem};
use crate::base::linked_list::LinkedList;
use crate::base::{arrow_parquet_utils, log::LogWriter, strings};
use crate::codec::row_codec::{encode, RowRecordBatch};
use crate::error::{RTStoreError, Result};
use arc_swap::ArcSwap;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use s3::bucket::Bucket;
use s3::bucket_ops::BucketConfiguration;
use s3::command::Command;
use s3::creds::Credentials;
use s3::region::Region;
use s3::request::Reqwest as RequestImpl;
use s3::request_trait::Request;

use std::fs;
use std::path::Path;
use std::str::Utf8Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tempdir::TempDir;
uselog!(info, debug, warn);

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
    // the limit rows in row memory table
    l1_rows_limit: u32,
    // the limit rows in column memory table
    l2_rows_limit: u32,
    // tmp dir path
    tmp_dir_path_prefix: String,
    // object key prefix
    object_key_prefix: String,
}

impl CellStoreConfig {
    pub fn new(
        bucket_name: &str,
        region: Region,
        schema: &SchemaRef,
        local_binlog_path_prefix: &str,
        auth: Credentials,
        tmp_dir_path_prefix: &str,
        object_key_prefix: &str,
    ) -> Result<Self> {
        if bucket_name.is_empty() {
            return Err(RTStoreError::CellStoreInvalidConfigError {
                name: String::from("bucket_name"),
                err: String::from("empty name"),
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
        if tmp_dir_path_prefix.is_empty() {
            return Err(RTStoreError::CellStoreInvalidConfigError {
                name: String::from("tmp_dir_path_prefix"),
                err: String::from("empty string"),
            });
        }
        if object_key_prefix.is_empty() {
            return Err(RTStoreError::CellStoreInvalidConfigError {
                name: String::from("object_key_prefix"),
                err: String::from("empty string"),
            });
        }
        Ok(Self {
            bucket_name: bucket_name.to_string(),
            region,
            schema: schema.clone(),
            local_binlog_path_prefix: local_binlog_path_prefix.to_string(),
            auth,
            l1_rows_limit: 10 * 1024,
            l2_rows_limit: 10 * 1024 * 5,
            tmp_dir_path_prefix: tmp_dir_path_prefix.to_string(),
            object_key_prefix: object_key_prefix.to_string(),
        })
    }
}

struct CellStoreLockData {
    // the writer of binlog
    log_writer: Box<LogWriter>,
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
    // lock for binlog
    lock_data: Arc<Mutex<CellStoreLockData>>,
    // memory table for row store
    row_memtable: ArcSwap<LinkedList<RowRecordBatch>>,
    row_memtable_size: AtomicU64,
    // memory table for column store
    column_memtable: ArcSwap<LinkedList<RecordBatch>>,
    column_memtable_size: AtomicU64,
    parquet_file_counter: AtomicU64,
}

unsafe impl Send for CellStore {}

unsafe impl Sync for CellStore {}

impl CellStore {
    pub fn new(config: CellStoreConfig) -> Result<Self> {
        info!(
            "init a new cell store with bucket {} , region {}, tmp_dir_path_prefix {}, object_key_prefix {}",
            config.bucket_name, config.region, config.tmp_dir_path_prefix, config.object_key_prefix
        );

        let bucket = match &config.region {
            Region::Custom { .. } => {
                let b = Bucket::new(
                    &config.bucket_name,
                    config.region.clone(),
                    config.auth.clone(),
                )?;
                b.with_path_style()
            }
            _ => Bucket::new(
                &config.bucket_name,
                config.region.clone(),
                config.auth.clone(),
            )?,
        };
        fs::create_dir_all(&config.local_binlog_path_prefix)?;
        fs::create_dir_all(&config.tmp_dir_path_prefix)?;
        let log_path_str = format!("{}/0000.binlog", config.local_binlog_path_prefix);
        let log_path = Path::new(&log_path_str);
        let fs = SyncPosixFileSystem {};
        let writer = fs.open_writable_file_writer(log_path)?;
        let log_writer = Box::new(LogWriter::new(writer, 0));
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
            lock_data: Arc::new(Mutex::new(CellStoreLockData { log_writer })),
            row_memtable: ArcSwap::from(Arc::new(LinkedList::new())),
            row_memtable_size: AtomicU64::new(0),
            column_memtable: ArcSwap::from(Arc::new(LinkedList::new())),
            column_memtable_size: AtomicU64::new(0),
            parquet_file_counter: AtomicU64::new(0),
        })
    }

    pub async fn create_bucket(&self) -> Result<()> {
        let mut config = BucketConfiguration::default();
        config.set_region(self.config.region.clone());
        let command = Command::CreateBucket { config };
        let bucket = match &self.config.region {
            Region::Custom { .. } => {
                let b = Bucket::new(
                    &self.config.bucket_name,
                    self.config.region.clone(),
                    self.config.auth.clone(),
                )?;
                b.with_path_style()
            }
            _ => Bucket::new(
                &self.config.bucket_name,
                self.config.region.clone(),
                self.config.auth.clone(),
            )?,
        };
        let request = RequestImpl::new(&bucket, "", command);
        request.response_data(false).await?;
        info!("create bucket {} ok", &self.config.bucket_name);
        Ok(())
    }

    pub fn row_memtable_size(&self) -> u64 {
        self.row_memtable_size.load(Ordering::Relaxed)
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
        self.row_memtable_size
            .fetch_add(size as u64, Ordering::Relaxed);
        self.do_l1_compaction_maybe();
        // save record to binlog
        if let Ok(mut guard) = self.lock_data.lock() {
            guard.log_writer.add_record(&data)
        } else {
            Err(RTStoreError::BaseBusyError(
                "fail to obtain lock".to_string(),
            ))
        }
    }

    fn do_l1_compaction_maybe(&self) {
        let local_row_memtable = self.row_memtable.load();
        if self.row_memtable_size.load(Ordering::Acquire) as u32 >= self.config.l1_rows_limit {
            self.row_memtable.store(Arc::new(LinkedList::new()));
            self.row_memtable_size.store(0, Ordering::Relaxed);
            match arrow_parquet_utils::rows_to_columns(
                &self.config.schema,
                local_row_memtable.as_ref(),
            ) {
                Ok(record_batch) => {
                    debug!("record batch row num {}", record_batch.num_rows());
                    // update size of memory table
                    self.column_memtable_size
                        .fetch_add(record_batch.num_rows() as u64, Ordering::Relaxed);
                    let local_column_memtable = self.column_memtable.load();
                    if let Ok(_) = local_column_memtable.push_front(record_batch) {
                        debug!("compaction ok for cell store");
                    }
                }
                Err(e) => {
                    warn!("fail to dump parquet for {}", e);
                }
            }
        }
    }

    pub async fn do_l2_compaction(&self) -> Result<()> {
        let local_column_memtable = self.column_memtable.load();
        debug!(
            "column memtable size {}",
            self.column_memtable_size.load(Ordering::Relaxed)
        );
        if self.column_memtable_size.load(Ordering::Acquire) as u32 >= self.config.l2_rows_limit {
            self.column_memtable.store(Arc::new(LinkedList::new()));
            let previous = self.column_memtable_size.swap(0, Ordering::Relaxed);
            self.total_rows_in_memory
                .fetch_sub(previous, Ordering::Relaxed);
            // write record to local file
            let tmp_dir = TempDir::new_in(&self.config.tmp_dir_path_prefix, "l2_compaction")
                .expect("fail to create tmp dir for l2 compaction");
            let file_path = tmp_dir.path().join("l2.parquet.gz");
            if arrow_parquet_utils::dump_recordbatch(
                &file_path,
                local_column_memtable.as_ref(),
                &self.config.schema,
            )
            .is_ok()
            {
                debug!("dump parquet to {} done", file_path.display());
                let readable_str = strings::to_readable_num_str(
                    self.parquet_file_counter.fetch_add(1, Ordering::Relaxed) as usize,
                    8,
                );
                let object_key = format!(
                    "{}/{}.gz.parquet",
                    self.config.object_key_prefix, readable_str
                );
                debug!("plan to store file to {}", object_key);
                let mut stream_fd = tokio::fs::File::open(file_path).await?;
                self.bucket
                    .put_object_stream(&mut stream_fd, object_key)
                    .await?;
            }
        }
        Ok(())
    }

    #[inline(always)]
    pub fn get_total_rows_in_memory(&self) -> u64 {
        self.total_rows_in_memory.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::row_codec::Data;
    use arrow::datatypes::Schema;
    use arrow::datatypes::*;

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
        let region = Region::Custom {
            region: "".to_string(),
            endpoint: "http://127.0.0.1:9090".to_string(),
        };

        let schema = Arc::new(Schema::empty());
        let local_binlog_path_prefix = "/test/binlog";
        let tmp_dir_path_prefix = "/test/tmp";
        let object_key_prefix = "/test/object";
        if CellStoreConfig::new(
            bucket_name,
            region.clone(),
            &schema,
            local_binlog_path_prefix,
            auth.clone(),
            tmp_dir_path_prefix,
            object_key_prefix,
        )
        .is_ok()
        {
            panic!("should has some config error");
        }

        if CellStoreConfig::new(
            "",
            region.clone(),
            &valid_schema,
            local_binlog_path_prefix,
            auth.clone(),
            tmp_dir_path_prefix,
            object_key_prefix,
        )
        .is_ok()
        {
            panic!("should has some config error");
        }

        if CellStoreConfig::new(
            bucket_name,
            region.clone(),
            &valid_schema,
            "",
            auth.clone(),
            tmp_dir_path_prefix,
            object_key_prefix,
        )
        .is_ok()
        {
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
        let bucket_name = "test_bucket";
        let region = Region::Custom {
            region: "".to_string(),
            endpoint: "http://127.0.0.1:9090".to_string(),
        };
        let local_binlog_path_prefix = "./test/binlog";
        let tmp_dir_path_prefix = "./test/tmp";
        let object_key_prefix = "test/object";
        CellStoreConfig::new(
            bucket_name,
            region,
            &valid_schema,
            local_binlog_path_prefix,
            auth,
            tmp_dir_path_prefix,
            object_key_prefix,
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
                assert_eq!(3, c.get_total_rows_in_memory());
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
            vec![Data::Int64(12)],
            vec![Data::Int64(11)],
            vec![Data::Int64(10)],
        ];
        RowRecordBatch {
            batch,
            schema_version: 1,
            id: "eth.price".to_string(),
        }
    }

    #[tokio::test]
    async fn test_l1_compaction() {
        let config = gen_a_normal_config().unwrap();
        if let Ok(c) = CellStore::new(config) {
            for _ in 1..20480 {
                let batch = gen_sample_row_batch();
                if c.put_records(batch).await.is_err() {
                    panic!("should be ok")
                }
            }
            assert_eq!(61437, c.get_total_rows_in_memory());
            assert_eq!(10227, c.row_memtable_size());
        } else {
            panic!("should be ok");
        }
    }

    #[tokio::test]
    async fn test_l2_compaction() {
        let config = gen_a_normal_config().unwrap();
        if let Ok(c) = CellStore::new(config) {
            if let Err(e) = c.create_bucket().await {
                panic!("should not be here {}", e);
            }
            let range_end = 1024 * 10 * 20;
            for _ in 1..range_end {
                let batch = gen_sample_row_batch();
                if c.put_records(batch).await.is_err() {
                    panic!("should be ok")
                }
            }
            if let Err(e) = c.do_l2_compaction().await {
                panic!("should be ok {}", e)
            }
            assert_eq!(10237, c.get_total_rows_in_memory());
        } else {
            panic!("should be ok");
        }
    }
}
