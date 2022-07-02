//
//
// table.rs
// Copyright (C) 2022 db3.network Author imotai <codego.me@gmail.com>
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
//

uselog!(info, warn);
use super::table_scanner::TableScannerExec;
use crate::codec::flight_codec::flight_data_to_arrow_batch;
use crate::error::{DB3Error, Result};
use crate::proto::db3_base_proto::Db3TableDesc;
use crate::sdk::memory_node_sdk::MemoryNodeSDK;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use crossbeam_skiplist_piedb::SkipMap;
use datafusion::datasource::TableType;
use datafusion::datasource::{
    file_format::parquet::ParquetFormat,
    get_statistics_with_limit,
    listing::{ListingOptions, ListingTableUrl, PartitionedFile},
    TableProvider,
};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_plan::{combine_filters, Expr};
use datafusion::physical_plan::project_schema;
use datafusion::physical_plan::{empty::EmptyExec, memory::MemoryExec};

use datafusion::physical_plan::{file_format::FileScanConfig, ExecutionPlan, Statistics};
use futures::stream::StreamExt;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub struct MemoryTableState {
    pub num_rows: usize,
    pub total_bytes: usize,
}

#[derive(Clone)]
pub struct Table {
    desc: Db3TableDesc,
    parquet_schema: SchemaRef,
    // pid -> endpoint of node
    partition_to_nodes: Arc<SkipMap<i32, MemoryNodeSDK>>,
    options: ListingOptions,
}

unsafe impl Send for Table {}
unsafe impl Sync for Table {}

impl Table {
    pub fn new(desc: &Db3TableDesc, schema: SchemaRef) -> Self {
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        Self {
            desc: desc.clone(),
            parquet_schema: schema,
            partition_to_nodes: Arc::new(SkipMap::new()),
            options,
        }
    }

    pub fn assign_partition_to_node(&self, pid: i32, sdk: MemoryNodeSDK) -> Result<()> {
        self.partition_to_nodes.remove(&pid);
        self.partition_to_nodes.get_or_insert_with(pid, || sdk);
        Ok(())
    }

    #[inline]
    pub fn get_table_desc(&self) -> &Db3TableDesc {
        &self.desc
    }

    #[inline]
    pub fn get_db(&self) -> &str {
        &self.desc.db
    }

    #[inline]
    pub fn get_schema(&self) -> &SchemaRef {
        &self.parquet_schema
    }

    #[inline]
    pub fn get_ctime(&self) -> i64 {
        self.desc.ctime
    }

    #[inline]
    pub fn get_name(&self) -> &str {
        &self.desc.name
    }

    #[inline]
    pub fn get_node_by_partition(&self, pid: i32) -> Option<MemoryNodeSDK> {
        let node_entry = self.partition_to_nodes.get(&pid);
        match node_entry {
            Some(entry) => {
                let node = entry.value().clone();
                Some(node)
            }
            _ => None,
        }
    }

    async fn get_memory_records(&self) -> Result<(Vec<RecordBatch>, MemoryTableState)> {
        //TODO support table partition
        let sdk = self.get_node_by_partition(0).ok_or_else(|| {
            warn!("fail to get memory node for table {} ", self.get_name());
            Db3Error::RPCInternalError(format!(
                "fail to get node by partition for table {}",
                self.get_name()
            ))
        })?;
        let resp = sdk
            .get_head_batch_of_partition(self.get_db(), self.get_name(), 0)
            .await
            .map_err(|e| {
                Db3Error::RPCInternalError(format!(
                    "fail to get  partition stream for table {} with err {}",
                    self.get_name(),
                    e
                ))
            })?;
        let mut stream = resp.into_inner();
        // skip the first message
        stream.message().await.map_err(|e| {
            Db3Error::RPCInternalError(format!(
                "fail to get iterator stream for table {} with err {}",
                self.get_name(),
                e
            ))
        })?;
        let mut results = vec![];
        let dictionaries_by_field = HashMap::new();
        let mut num_rows: usize = 0;
        let mut total_bytes: usize = 0;
        while let Some(flight_data) = stream.message().await.map_err(|e| {
            DB3Error::RPCInternalError(format!(
                "fail to get iterator stream for table {} with err {}",
                self.get_name(),
                e
            ))
        })? {
            let record_batch = flight_data_to_arrow_batch(
                &flight_data,
                self.get_schema().clone(),
                &dictionaries_by_field,
            )?;
            num_rows += record_batch.num_rows();
            let byte_size: usize = record_batch
                .columns()
                .iter()
                .map(|array| array.get_array_memory_size())
                .sum();
            total_bytes += byte_size;
            results.push(record_batch);
        }
        Ok((
            results,
            MemoryTableState {
                num_rows,
                total_bytes,
            },
        ))
    }

    async fn list_files(
        &self,
        ctx: &SessionState,
        limit: Option<usize>,
    ) -> Result<(Vec<Vec<PartitionedFile>>, Statistics)> {
        //TODO cache the table path as member
        let table_path = format!("{}/{}", self.get_db(), self.get_name());
        let table_url = ListingTableUrl::parse("s3://").map_err(|e| {
            warn!("fail to parse url {} with err {}", &table_path, e);
            DB3Error::TableBadUrl(table_path.to_string())
        })?;
        let store = ctx.runtime_env.object_store(&table_url).map_err(|e| {
            warn!("fail to get object store {} with err {}", &table_path, e);
            DB3Error::TableBadUrl(table_path.to_string())
        })?;
        let stream = store.list_file(&table_path).await.map_err(|e| {
            warn!("fail to get object store {} with err {}", &table_path, e);
            DB3Error::TableBadUrl(table_path.to_string())
        })?;
        let pin_stream = Box::pin(stream);
        let files = pin_stream.then(|file_meta| async {
            let part_file: PartitionedFile = file_meta?.into();
            let statistics = if self.options.collect_stat {
                self.options
                    .format
                    .infer_stats(&store, self.get_schema().clone(), &part_file.file_meta)
                    .await?
            } else {
                Statistics::default()
            };
            Ok((part_file, statistics)) as DFResult<(PartitionedFile, Statistics)>
        });
        let (files, statistics) =
            get_statistics_with_limit(files, self.get_schema().clone(), limit)
                .await
                .map_err(|e| {
                    warn!(
                        "fail to get statistics for table {} with err {}",
                        self.get_name(),
                        e
                    );
                    DB3Error::TableBadUrl(table_path.to_string())
                })?;
        info!(
            "files size {} rows {}",
            files.len(),
            statistics.num_rows.unwrap()
        );
        Ok((
            self.split_files(files, self.options.target_partitions),
            statistics,
        ))
    }

    fn split_files(
        &self,
        partitioned_files: Vec<PartitionedFile>,
        n: usize,
    ) -> Vec<Vec<PartitionedFile>> {
        if partitioned_files.is_empty() {
            return vec![];
        }
        // effectively this is div with rounding up instead of truncating
        let chunk_size = (partitioned_files.len() + n - 1) / n;
        partitioned_files
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect()
    }
}

#[async_trait]
impl TableProvider for Table {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.get_schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let (records, memory_state) = self.get_memory_records().await.map_err(|e| {
            DataFusionError::Internal(format!("fail to get memory records for err {}", e))
        })?;
        info!("memory records size {}", records.len());
        let (partition_files, mut statistics) = self
            .list_files(ctx, limit)
            .await
            .map_err(|e| DataFusionError::Internal(format!("fail to list files for err {}", e)))?;
        if partition_files.is_empty() && memory_state.num_rows == 0 {
            let schema = self.get_schema();
            let projected_schema = project_schema(&schema, projection.as_ref())?;
            return Ok(Arc::new(EmptyExec::new(false, projected_schema)));
        }

        if partition_files.is_empty() && memory_state.num_rows != 0 {
            let memory_exec =
                MemoryExec::try_new(&[records], self.get_schema().clone(), projection.clone())?;
            return Ok(Arc::new(memory_exec));
        }

        let new_rows = match statistics.num_rows {
            Some(old) => Some(old + memory_state.num_rows),
            None => Some(0),
        };

        let new_total_byte_size = match statistics.total_byte_size {
            Some(old) => Some(old + memory_state.total_bytes),
            None => Some(0),
        };
        statistics.num_rows = new_rows;
        statistics.total_byte_size = new_total_byte_size;
        let table_path = format!("s3://{}/{}", self.get_db(), self.get_name());
        let table_url = ListingTableUrl::parse(&table_path)?;
        let predicate = combine_filters(filters);
        let file_config = FileScanConfig {
            object_store_url: table_url.object_store(),
            file_schema: self.get_schema().clone(),
            file_groups: partition_files,
            statistics,
            projection: projection.clone(),
            limit,
            table_partition_cols: self.options.table_partition_cols.clone(),
        };
        let exec =
            TableScannerExec::new(file_config, self.get_schema().clone(), records, predicate)?;
        Ok(Arc::new(exec))
    }
}
