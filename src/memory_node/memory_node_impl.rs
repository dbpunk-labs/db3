//
//
// memory_node_impl.rs
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

use crate::base::arrow_parquet_utils;
use crate::codec::row_codec::decode;
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::{
    RtStoreNodeType, RtStoreTableDesc, StorageBackendConfig, StorageRegion,
};
use crate::proto::rtstore_memory_proto::memory_node_server::MemoryNode;
use crate::proto::rtstore_memory_proto::{
    AppendRecordsRequest, AppendRecordsResponse, AssignPartitionRequest, AssignPartitionResponse,
};
use crate::sdk::meta_node_sdk::MetaNodeSDK;
use crate::store::cell_store::{CellStore, CellStoreConfig};
use arc_swap::ArcSwap;
use s3::creds::Credentials;
use s3::region::Region;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;
use tonic::{Request, Response, Status};

uselog!(info);

pub struct MemoryNodeConfig {
    pub binlog_root_dir: String,
    pub tmp_store_root_dir: String,
    pub meta_node_endpoint: String,
    pub my_endpoint: String,
}

pub struct MemoryNodeState {
    // table->partition->cell
    cells: HashMap<String, HashMap<i32, Arc<CellStore>>>,
    //cells_compaction_state: HashMap<String, HashMap<i32, bool>>,
}

impl MemoryNodeState {
    fn new() -> Self {
        Self {
            cells: HashMap::new(),
        }
    }
    fn build_region(storage: &Option<StorageRegion>) -> Result<Region> {
        if let Some(storage_region) = storage {
            if let Ok(r) = Region::from_str(&storage_region.region) {
                match r {
                    Region::Custom { .. } => Ok(Region::Custom {
                        region: storage_region.region.to_string(),
                        endpoint: storage_region.endpoint.to_string(),
                    }),
                    _ => Ok(r),
                }
            } else {
                Ok(Region::Custom {
                    region: storage_region.region.to_string(),
                    endpoint: storage_region.endpoint.to_string(),
                })
            }
        } else {
            Err(RTStoreError::CellStoreInvalidConfigError {
                name: "storage region".to_string(),
                err: "is null".to_string(),
            })
        }
    }

    fn build_storage_auth() -> Credentials {
        Credentials::from_env_specific(
            Some("AWS_S3_ACCESS_KEY"),
            Some("AWS_S3_SECRET_KEY"),
            None,
            None,
        )
        .unwrap()
    }

    pub async fn build_cell_store(
        table_id: &str,
        partition_ids: &[i32],
        table_desc: &RtStoreTableDesc,
        storage_config: &StorageBackendConfig,
        memory_node_confg: &MemoryNodeConfig,
    ) -> Result<Vec<(i32, Arc<CellStore>)>> {
        if let Some(rtstore_schema) = &table_desc.schema {
            let schema = arrow_parquet_utils::table_desc_to_arrow_schema(&rtstore_schema)?;
            let region = MemoryNodeState::build_region(&storage_config.region)?;
            let mut cells: Vec<(i32, Arc<CellStore>)> = Vec::new();
            for id in partition_ids {
                //TODO table id is not safe
                //
                let safe_table_id = table_id.replace(".", "-");
                let object_path = format!("{}/{}", &safe_table_id, id);
                let auth = MemoryNodeState::build_storage_auth();
                let cell_log_path = format!(
                    "{}/{}/{}/log/",
                    memory_node_confg.binlog_root_dir, &safe_table_id, id
                );
                let cell_tmp_path = format!(
                    "{}/{}/{}/tmp/",
                    memory_node_confg.tmp_store_root_dir, &safe_table_id, id
                );
                let mut cell_config = CellStoreConfig::new(
                    &storage_config.bucket,
                    region.clone(),
                    &schema,
                    &cell_log_path,
                    auth,
                    &cell_tmp_path,
                    &object_path,
                )?;
                cell_config.set_l1_rows_limit(storage_config.l1_rows_limit);
                cell_config.set_l2_rows_limit(storage_config.l2_rows_limit);
                let cell_store = Arc::new(CellStore::new(cell_config)?);
                cell_store.create_bucket().await?;
                cells.push((*id, cell_store));
            }
            Ok(cells)
        } else {
            Err(RTStoreError::CellStoreInvalidConfigError {
                name: "table schema".to_string(),
                err: "is null".to_string(),
            })
        }
    }

    pub fn get_cell(&self, table_id: &str, pid: i32) -> Option<Arc<CellStore>> {
        if let Some(internal_map) = self.cells.get(table_id) {
            if let Some(cell) = internal_map.get(&pid) {
                Some(cell.clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn add_cell(&mut self, table_id: &str, pid: i32, cell_store: Arc<CellStore>) -> Result<()> {
        if let Some(internal_map) = self.cells.get_mut(table_id) {
            match internal_map.get(&pid) {
                Some(_) => Err(RTStoreError::CellStoreExistError {
                    tid: table_id.to_string(),
                    pid,
                }),
                _ => {
                    internal_map.insert(pid, cell_store);
                    Ok(())
                }
            }
        } else {
            let mut table: HashMap<i32, Arc<CellStore>> = HashMap::new();
            table.insert(pid, cell_store);
            self.cells.insert(table_id.to_string(), table);
            Ok(())
        }
    }
}

impl Default for MemoryNodeState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MemoryNodeImpl {
    state: Arc<Mutex<MemoryNodeState>>,
    config: MemoryNodeConfig,
    meta_node_client: ArcSwap<Option<MetaNodeSDK>>,
}

impl MemoryNodeImpl {
    pub fn new(config: MemoryNodeConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryNodeState::new())),
            config,
            meta_node_client: ArcSwap::from(Arc::new(None)),
        }
    }

    pub async fn connect_meta_node(&self) -> Result<()> {
        if self.config.meta_node_endpoint.is_empty() {
            return Err(RTStoreError::NodeRPCInvalidEndpointError {
                name: "meta node ".to_string(),
            });
        }
        if let Ok(meta_node_sdk) = MetaNodeSDK::connect(&self.config.meta_node_endpoint).await {
            if meta_node_sdk
                .register_node(&self.config.my_endpoint, RtStoreNodeType::KMemoryNode)
                .await
                .is_ok()
            {
                self.meta_node_client.store(Arc::new(Some(meta_node_sdk)));
            } else {
                return Err(RTStoreError::NodeRPCError(
                    self.config.meta_node_endpoint.to_string(),
                ));
            }
            return Ok(());
        } else {
            return Err(RTStoreError::NodeRPCError(
                self.config.meta_node_endpoint.to_string(),
            ));
        }
    }

    pub fn start_l2_compaction(&self, table_id: &str, pid: i32) {
        let local_table_id = table_id.to_string();
        let local_state = self.state.clone();
        // TODO avoid start compaction repeated
        tokio::task::spawn(async move {
            loop {
                sleep(Duration::from_millis(1000)).await;
                let cell_opt = match local_state.lock() {
                    Ok(node_state) => node_state.get_cell(&local_table_id, pid),
                    Err(_) => None,
                };
                if let Some(cell) = cell_opt {
                    if cell.do_l2_compaction().await.is_ok() {
                        info!(
                            "do l2 compaction done for table {}, pid {}",
                            &local_table_id, pid
                        )
                    }
                } else {
                    break;
                }
            }
        });
    }

    pub fn get_cell(&self, table_id: &str, pid: i32) -> Option<Arc<CellStore>> {
        match self.state.lock() {
            Ok(node_state) => node_state.get_cell(table_id, pid),
            Err(_) => None,
        }
    }
}

unsafe impl Send for MemoryNodeImpl {}

unsafe impl Sync for MemoryNodeImpl {}

#[tonic::async_trait]
impl MemoryNode for MemoryNodeImpl {
    async fn append_records(
        &self,
        request: Request<AppendRecordsRequest>,
    ) -> std::result::Result<Response<AppendRecordsResponse>, Status> {
        let append_request = request.into_inner();
        if let Some(cell_store) =
            self.get_cell(&append_request.table_id, append_request.partition_id)
        {
            let row_batch = decode(&append_request.records)?;
            cell_store.put_records(row_batch).await?;
            Ok(Response::new(AppendRecordsResponse {}))
        } else {
            Err(Status::from(RTStoreError::CellStoreNotFoundError {
                tid: append_request.table_id.to_string(),
                pid: append_request.partition_id,
            }))
        }
    }

    async fn assign_partition(
        &self,
        request: Request<AssignPartitionRequest>,
    ) -> std::result::Result<Response<AssignPartitionResponse>, Status> {
        let assign_request = request.into_inner();
        if let (Some(table_desc), Some(config)) =
            (&assign_request.table_desc, &assign_request.config)
        {
            let cells = MemoryNodeState::build_cell_store(
                &assign_request.table_id,
                &assign_request.partition_ids,
                table_desc,
                config,
                &self.config,
            )
            .await?;
            let mut cell_ids: Vec<i32> = Vec::new();
            let result = match self.state.lock() {
                Ok(mut node_state) => {
                    for (id, cell) in cells {
                        node_state.add_cell(&assign_request.table_id, id, cell)?;
                        cell_ids.push(id);
                    }
                    Ok(Response::new(AssignPartitionResponse {}))
                }
                Err(_) => Err(Status::internal(RTStoreError::BaseBusyError(
                    "fail to get lock".to_string(),
                ))),
            };
            if result.is_ok() {
                for id in cell_ids {
                    self.start_l2_compaction(&assign_request.table_id, id);
                }
            }
            result
        } else {
            Err(Status::invalid_argument(
                "table desc or config is null".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::row_codec::{encode, Data, RowRecordBatch};
    use crate::proto::rtstore_base_proto::{RtStoreColumnDesc, RtStoreSchemaDesc, RtStoreType};
    use std::thread;
    use tempdir::TempDir;

    #[tokio::test]
    async fn test_assign_partitions() {
        let tmp_dir_path = TempDir::new("assign_partition").expect("create temp dir");
        if let Some(tmp_dir_path_str) = tmp_dir_path.path().to_str() {
            let config = MemoryNodeConfig {
                binlog_root_dir: format!("{}/binlog_root_dir", tmp_dir_path_str).to_string(),
                tmp_store_root_dir: format!("{}/tmp_store_root_dir", tmp_dir_path_str).to_string(),
                meta_node_endpoint: "".to_string(),
                my_endpoint: "".to_string(),
            };
            let memory_node = MemoryNodeImpl::new(config);
            let assign_req = create_assign_partition_request("test.eth");
            let req = Request::new(assign_req);
            assert!(memory_node.assign_partition(req).await.is_ok());
            assert!(memory_node.get_cell("test.eth", 3).is_none());
            assert!(memory_node.get_cell("test.eth", 0).is_some());
        } else {
            panic!("should not be here");
        }
    }

    #[tokio::test]
    async fn test_append_records_compaction() -> Result<()> {
        let tmp_dir_path = TempDir::new("append_compaction_records").expect("create temp dir");
        if let Some(tmp_dir_path_str) = tmp_dir_path.path().to_str() {
            let config = MemoryNodeConfig {
                binlog_root_dir: format!("{}/binlog_root_dir", tmp_dir_path_str).to_string(),
                tmp_store_root_dir: format!("{}/tmp_store_root_dir", tmp_dir_path_str).to_string(),
                meta_node_endpoint: "".to_string(),
                my_endpoint: "".to_string(),
            };
            let memory_node = MemoryNodeImpl::new(config);
            let assign_req = create_assign_partition_request("test.sol");
            let req = Request::new(assign_req);
            assert!(memory_node.assign_partition(req).await.is_ok());
            assert!(memory_node.get_cell("test.sol", 3).is_none());
            assert!(memory_node.get_cell("test.sol", 0).is_some());
            for _ in 0..102400 {
                let batch = gen_sample_row_batch();
                let data = encode(&batch)?;
                let req = Request::new(AppendRecordsRequest {
                    table_id: "test.sol".to_string(),
                    partition_id: 0,
                    records: data,
                });
                assert!(memory_node.append_records(req).await.is_ok());
            }
        } else {
            panic!("should not be here");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_append_records() -> Result<()> {
        let tmp_dir_path = TempDir::new("append_records").expect("create temp dir");
        if let Some(tmp_dir_path_str) = tmp_dir_path.path().to_str() {
            let config = MemoryNodeConfig {
                binlog_root_dir: format!("{}/binlog_root_dir", tmp_dir_path_str).to_string(),
                tmp_store_root_dir: format!("{}/tmp_store_root_dir", tmp_dir_path_str).to_string(),
                meta_node_endpoint: "".to_string(),
                my_endpoint: "".to_string(),
            };
            let memory_node = MemoryNodeImpl::new(config);
            let assign_req = create_assign_partition_request("test.btc");
            let req = Request::new(assign_req);
            assert!(memory_node.assign_partition(req).await.is_ok());
            assert!(memory_node.get_cell("test.btc", 3).is_none());
            let batch = gen_sample_row_batch();
            let data = encode(&batch)?;
            let req = Request::new(AppendRecordsRequest {
                table_id: "test.btc".to_string(),
                partition_id: 0,
                records: data,
            });
            assert!(memory_node.get_cell("test.btc", 0).is_some());
            assert!(memory_node.append_records(req).await.is_ok());
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

    fn create_assign_partition_request(tname: &str) -> AssignPartitionRequest {
        let region = StorageRegion {
            region: "".to_string(),
            endpoint: "http://127.0.0.1:9090".to_string(),
        };

        let storage_config = StorageBackendConfig {
            bucket: "test_bk_1".to_string(),
            region: Some(region),
            l1_rows_limit: 10 * 1024,
            l2_rows_limit: 5 * 10 * 1024,
        };
        let table_desc = create_simple_table_desc(tname);
        let pids: Vec<i32> = vec![0, 1, 2];
        AssignPartitionRequest {
            partition_ids: pids,
            table_desc: Some(table_desc),
            table_id: tname.to_string(),
            config: Some(storage_config),
        }
    }

    fn create_simple_table_desc(tname: &str) -> RtStoreTableDesc {
        let col1 = RtStoreColumnDesc {
            name: "col1".to_string(),
            ctype: RtStoreType::KBigInt as i32,
            null_allowed: true,
        };
        let schema = RtStoreSchemaDesc {
            columns: vec![col1],
            version: 1,
        };
        RtStoreTableDesc {
            names: vec![tname.to_string()],
            schema: Some(schema),
            partition_desc: None,
        }
    }
}
