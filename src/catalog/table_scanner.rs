//
//
// table_scanner.rs
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
//

use crate::catalog::table::Table;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use core::fmt;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::execution::context::TaskContext;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::{
    expressions::PhysicalSortExpr, file_format::FileScanConfig, file_format::ParquetExec,
    project_schema, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::Stream;
uselog!(debug, info);
use std::any::Any;
use std::sync::Arc;
use std::task::{Context, Poll};

struct MemTableIterator {
    it: Vec<RecordBatch>,
    index: usize,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
}

impl MemTableIterator {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>, projection: Option<Vec<usize>>) -> Self {
        Self {
            it: batches,
            index: 0,
            projection,
            schema,
        }
    }
}

unsafe impl Send for MemTableIterator {}
unsafe impl Sync for MemTableIterator {}

impl RecordBatchStream for MemTableIterator {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for MemTableIterator {
    type Item = ArrowResult<RecordBatch>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.index += 1;
        Poll::Ready(if self.index <= self.it.len() {
            let batch = &self.it[self.index - 1];
            let projected_batch = match self.projection.as_ref() {
                Some(columns) => batch.project(columns)?,
                None => batch.clone(),
            };
            Some(Ok(projected_batch))
        } else {
            None
        })
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.it.len(), Some(self.it.len()))
    }
}

pub struct TableScannerExec {
    parquet_exec: ParquetExec,
    config: FileScanConfig,
    projected_schema: SchemaRef,
    partition_cnt: usize,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl TableScannerExec {
    pub fn new(
        config: FileScanConfig,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        predicate: Option<Expr>,
    ) -> Result<Self> {
        let partition_cnt = config.file_groups.len() + 1;
        let projected_schema = project_schema(&schema, config.projection.as_ref())?;
        let parquet_exec = ParquetExec::new(config.clone(), predicate);
        Ok(Self {
            parquet_exec,
            config,
            projected_schema,
            partition_cnt,
            schema,
            batches,
        })
    }
}

#[async_trait]
impl ExecutionPlan for TableScannerExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partition_cnt)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {:?}",
            self
        )))
    }

    fn execute(
        &self,
        partition_index: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        match partition_index {
            0 => Ok(Box::pin(MemTableIterator::new(
                self.schema.clone(),
                self.batches.clone(),
                self.config.projection.clone(),
            ))),
            _ => self.parquet_exec.execute(partition_index - 1, context),
        }
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "TableScannerExec:p size {:?}", self.parquet_exec)
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.config.statistics.clone()
    }
}

impl fmt::Debug for TableScannerExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "partitions: [...]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn it_test_piedb_table_scanner() -> Result<()> {
        Ok(())
    }
}
