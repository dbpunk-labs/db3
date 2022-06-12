//
//
// object_store.rs
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

use crate::base::strings;
use crate::error::{RTStoreError, Result};
use async_trait::async_trait;
use bytes::{Buf, Bytes};

use crate::store::meta_store::MetaStore;
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::datafusion_data_access::object_store::{
    FileMetaStream, ListEntryStream, ObjectReader, ObjectStore,
};
use datafusion::datafusion_data_access::{FileMeta, Result as DFResult, SizedFile};
use futures::{stream, AsyncRead};
use s3::bucket::Bucket;
use s3::bucket_ops::BucketConfiguration;
use s3::command::Command;
use s3::creds::Credentials;
use s3::region::Region;
use s3::request::Reqwest as RequestImpl;
use s3::request_trait::Request;
use std::env;
use std::io::{Error, ErrorKind, Read};
use std::path::Path;
use std::sync::{mpsc, Arc};
use std::time::Duration;
uselog!(info, warn, debug);

const ACCESS_KEY: &str = "AWS_ACCESS_KEY_ID";
const SECRET_KEY: &str = "AWS_SECRET_ACCESS_KEY";
#[inline]
pub fn build_region(name: &str, endpoint: Option<String>) -> Region {
    match endpoint {
        Some(e) => Region::Custom {
            region: name.to_string(),
            endpoint: e,
        },
        _ => name.parse().unwrap(),
    }
}

#[inline]
pub fn build_bucket(bucket_name: &str, region: &Region, credentials: &Credentials) -> Bucket {
    match region {
        Region::Custom { .. } => {
            let b = Bucket::new(bucket_name, region.clone(), credentials.clone()).unwrap();
            b.with_path_style()
        }
        _ => Bucket::new(bucket_name, region.clone(), credentials.clone()).unwrap(),
    }
}

pub fn build_credentials(
    access_key: Option<&str>,
    secret_key: Option<&str>,
) -> Result<Credentials> {
    {
        if let (Some(ak), Some(sk)) = (access_key, secret_key) {
            // try build credentials from inputs
            Credentials::new(Some(ak), Some(sk), None, None, None)
        } else if let (Ok(_), Ok(_)) = (env::var(ACCESS_KEY), env::var(SECRET_KEY)) {
            // try build credentials from environment
            Credentials::from_env_specific(Some(ACCESS_KEY), Some(SECRET_KEY), None, None)
        } else {
            // use default profile
            Credentials::from_env()
        }
    }
    .or_else(|e| {
        warn!("fail to create s3 credentials for error {}", e);
        Err(RTStoreError::S3AuthError)
    })
}

#[derive(Debug)]
pub struct S3FileSystem {
    region: Region,
    credentials: Credentials,
}

impl S3FileSystem {
    pub fn new(region: Region, credentials: Credentials) -> Self {
        Self {
            region,
            credentials,
        }
    }

    pub fn new_bucket_fs(&self, bucket_name: &str) -> BucketFileSystem {
        BucketFileSystem::new(bucket_name, &self.region, &self.credentials)
    }

    pub async fn create_bucket(&self, bucket_name: &str) -> Result<()> {
        let mut config = BucketConfiguration::default();
        config.set_region(self.region.clone());
        let command = Command::CreateBucket { config };
        let bucket = match &self.region {
            Region::Custom { .. } => {
                let b = Bucket::new(bucket_name, self.region.clone(), self.credentials.clone())?;
                b.with_path_style()
            }
            _ => Bucket::new(bucket_name, self.region.clone(), self.credentials.clone())?,
        };
        let request = RequestImpl::new(&bucket, "", command);
        request.response_data(false).await?;
        Ok(())
    }
}

pub struct BucketFileSystem {
    bucket: Bucket,
    bucket_name: String,
}

impl BucketFileSystem {
    pub fn new(bucket_name: &str, region: &Region, credentials: &Credentials) -> Self {
        Self {
            bucket: build_bucket(bucket_name, region, credentials),
            bucket_name: bucket_name.to_string(),
        }
    }

    pub async fn put_with_file(&self, file_path: &Path, object_key: &str) -> Result<()> {
        let mut stream_fd = tokio::fs::File::open(file_path).await?;
        self.bucket
            .put_object_stream(&mut stream_fd, object_key)
            .await?;
        Ok(())
    }

    pub async fn create_bucket(&self) -> Result<()> {
        let config = BucketConfiguration::default();
        let command = Command::CreateBucket { config };
        let request = RequestImpl::new(&self.bucket, "", command);
        request.response_data(false).await?;
        Ok(())
    }
}

struct S3FileReader {
    bucket: Bucket,
    file: SizedFile,
    key: String,
}

impl S3FileReader {
    pub fn new(bucket: Bucket, file: SizedFile, key: String) -> Self {
        S3FileReader { bucket, file, key }
    }
}

#[async_trait]
impl ObjectReader for S3FileReader {
    async fn chunk_reader(&self, _start: u64, _length: usize) -> DFResult<Box<dyn AsyncRead>> {
        todo!("implement once async file readers are available (arrow-rs#78, arrow-rs#111)")
    }

    fn sync_chunk_reader(
        &self,
        start: u64,
        length: usize,
    ) -> DFResult<Box<dyn Read + Send + Sync>> {
        let end: Option<u64> = match length {
            0 => None,
            _ => Some(start + length as u64 - 1),
        };
        let (tx, rx) = mpsc::channel();
        let local_bucket = self.bucket.clone();
        let local_key = self.key.to_string();
        std::thread::spawn(move || {
            let result = local_bucket
                .get_object_range_blocking(local_key, start, end)
                .or_else(|e| {
                    Err(Error::new(
                        ErrorKind::Other,
                        format!("fail to get object range for error {}", e),
                    ))
                });
            tx.send(result).unwrap();
        });
        let (data, _) = rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|err| std::io::Error::new(ErrorKind::TimedOut, err))??;
        let bytes_buf = Bytes::from(data);
        Ok(Box::new(bytes_buf.reader()))
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

#[async_trait]
impl ObjectStore for S3FileSystem {
    fn file_reader(&self, file: SizedFile) -> DFResult<Arc<dyn ObjectReader>> {
        let file_path = file.path.clone();
        let (bucket, key) = match file_path.split_once('/') {
            Some((bucket, prefix)) => (bucket, prefix),
            None => (file_path.as_str(), ""),
        };
        let bucket_req = build_bucket(bucket, &self.region, &self.credentials);
        Ok(Arc::new(S3FileReader::new(
            bucket_req,
            file,
            key.to_string(),
        )))
    }

    async fn list_file(&self, url: &str) -> DFResult<FileMetaStream> {
        let (bucket, key) = strings::parse_s3_url(url).or_else(|_e| {
            Err(Error::new(
                ErrorKind::InvalidInput,
                format!("the url {} is invalid", url),
            ))
        })?;
        debug!("list file {} in bucket {} , key {}", url, &bucket, &key);
        let bucket_req = build_bucket(&bucket, &self.region, &self.credentials);
        let objects = bucket_req
            .list(key, None)
            .await
            .map_err(|e| Error::new(ErrorKind::Other, format!("fail to list bucket for {}", e)))?;
        let result = stream::iter(objects.into_iter().flat_map(|s| s.contents).map(
            move |object| {
                debug!("object {}", &format!("{}/{}", &bucket, &object.key));
                let time: Option<DateTime<Utc>> =
                    match NaiveDateTime::parse_from_str(&object.last_modified, "%Y-%m-%d %H:%M:%S")
                    {
                        Ok(t) => Some(DateTime::<Utc>::from_utc(t, Utc)),
                        _ => None,
                    };
                Ok(FileMeta {
                    sized_file: SizedFile {
                        path: format!("{}/{}", &bucket, object.key),
                        size: object.size as u64,
                    },
                    last_modified: time,
                })
            },
        ));
        Ok(Box::pin(result))
    }

    async fn list_dir(
        &self,
        _prefix: &str,
        _delimiter: Option<String>,
    ) -> DFResult<ListEntryStream> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;
    use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
    use datafusion::datasource::TableProvider;
    use datafusion::error::DataFusionError;

    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::prelude::*;
    use futures::TryStreamExt;
    use std::path::Path;

    #[tokio::test]
    async fn test_fs_create_bucket() -> Result<()> {
        let region = build_region("", Some("http://127.0.0.1:9000".to_string()));
        if let Region::Custom { .. } = region {
            assert!(true);
        } else {
            panic!("should not be here");
        }
        let credentials = build_credentials(None, None)?;
        let s3 = S3FileSystem::new(region, credentials);
        s3.create_bucket("test2").await?;
        Ok(())
    }

    #[tokio::test]
    async fn simple_flow_test() -> Result<()> {
        let region = build_region("", Some("http://127.0.0.1:9000".to_string()));
        if let Region::Custom { .. } = region {
            assert!(true);
        } else {
            panic!("should not be here");
        }
        let credentials = build_credentials(None, None)?;
        let s3 = S3FileSystem::new(region, credentials);
        s3.create_bucket("test3").await?;
        let bucket_fs = s3.new_bucket_fs("test3");
        let file_path = Path::new("thirdparty/parquet-testing/data/repeated_no_annotation.parquet");
        bucket_fs
            .put_with_file(&file_path, "ttt/test_key.parquet")
            .await?;
        let s3_path = "test3/";
        if let Ok(stream) = s3.list_file(&s3_path).await {
            let ret: DFResult<Vec<FileMeta>> = stream.try_collect().await;
            match ret {
                Ok(files) => {
                    let size = files.len();
                    for file in files {
                        info!("file path {}", file.path());
                    }
                    assert_eq!(size, 1);
                }
                _ => {
                    panic!("no files");
                }
            }
        } else {
            panic!("no files");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_sql_query() -> Result<()> {
        let region = build_region("", Some("http://127.0.0.1:9000".to_string()));
        if let Region::Custom { .. } = region {
            assert!(true);
        } else {
            panic!("should not be here");
        }
        let credentials = build_credentials(None, None)?;
        let s3 = S3FileSystem::new(region, credentials);
        s3.create_bucket("test4").await?;
        let bucket_fs = s3.new_bucket_fs("test4");
        let file_path = Path::new("thirdparty/parquet-testing/data/alltypes_plain.parquet");
        bucket_fs
            .put_with_file(&file_path, "t1/alltypes_plain.parquet")
            .await?;
        let session_config = SessionConfig::default();
        let runtime_config = RuntimeConfig::new();
        let runtime = RuntimeEnv::new(runtime_config)?;
        runtime.register_object_store("s3", Arc::new(s3));
        let ctx = SessionContext::with_config_rt(session_config, Arc::new(runtime));
        let filename = "s3://test4/";
        let table_url = ListingTableUrl::parse(filename)?;
        let state = ctx.state.read().clone();
        let config = ListingTableConfig::new(table_url)
            .infer(&state)
            .await
            .map_err(map_datafusion_error_to_io_error)?;
        let table = ListingTable::try_new(config).map_err(map_datafusion_error_to_io_error)?;
        ctx.register_table("t1", Arc::new(table)).unwrap();
        let batches = ctx
            .sql("SELECT * FROM t1")
            .await
            .map_err(map_datafusion_error_to_io_error)?
            .collect()
            .await
            .map_err(map_datafusion_error_to_io_error)?;
        let expected = vec![
 "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
    "| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |",
    "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
    "| 4  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30332f30312f3039 | 30         | 2009-03-01 00:00:00 |",
    "| 5  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30332f30312f3039 | 31         | 2009-03-01 00:01:00 |",
    "| 6  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30342f30312f3039 | 30         | 2009-04-01 00:00:00 |",
    "| 7  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30342f30312f3039 | 31         | 2009-04-01 00:01:00 |",
    "| 2  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30322f30312f3039 | 30         | 2009-02-01 00:00:00 |",
    "| 3  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30322f30312f3039 | 31         | 2009-02-01 00:01:00 |",
    "| 0  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30312f30312f3039 | 30         | 2009-01-01 00:00:00 |",
    "| 1  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30312f30312f3039 | 31         | 2009-01-01 00:01:00 |",
    "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }
    fn map_datafusion_error_to_io_error(err: DataFusionError) -> std::io::Error {
        std::io::Error::new(ErrorKind::Other, err)
    }
}
