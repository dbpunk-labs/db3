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
use crate::codec::flight_codec::{flight_data_from_arrow_batch, SchemaAsIpc};
use crate::codec::row_codec::decode;
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::{
    FlightData, RtStoreNode, RtStoreTableDesc, StorageBackendConfig, StorageRegion,
};
use crate::proto::rtstore_memory_proto::memory_node_server::MemoryNode;
use crate::proto::rtstore_memory_proto::{
    AppendRecordsRequest, AppendRecordsResponse, AssignPartitionRequest, AssignPartitionResponse,
    FetchPartitionRequest,
};
use crate::store::cell_store::{CellStore, CellStoreConfig};
use crate::store::meta_store::MetaStore;
use crate::store::object_store::build_credentials;
use futures::Stream;
use s3::creds::Credentials;
use s3::region::Region;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;
use tonic::{Request, Response, Status};
uselog!(info, warn, debug);
pub struct MemoryNodeConfig {
    pub binlog_root_dir: String,
    pub tmp_store_root_dir: String,
    pub etcd_cluster: String,
    pub etcd_root_path: String,
    pub node: RtStoreNode,
}

pub struct MemoryNodeState {
    // db->table->partition->cell
    cells: HashMap<String, HashMap<String, HashMap<i32, Arc<CellStore>>>>,
    // cells_compaction_state: HashMap<String, HashMap<i32, bool>>,
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

    fn build_storage_auth() -> Result<Credentials> {
        build_credentials(None, None)
    }

    pub async fn build_cell_store(
        partition_ids: &[i32],
        table_desc: &RtStoreTableDesc,
        storage_config: &StorageBackendConfig,
        memory_node_confg: &MemoryNodeConfig,
    ) -> Result<Vec<(i32, Arc<CellStore>)>> {
        if let Some(rtstore_schema) = &table_desc.schema {
            let schema = arrow_parquet_utils::table_desc_to_arrow_schema(rtstore_schema)?;
            let region = MemoryNodeState::build_region(&storage_config.region)?;
            let name = &table_desc.name;
            let db = &table_desc.db;
            let mut cells: Vec<(i32, Arc<CellStore>)> = Vec::new();
            for id in partition_ids {
                //TODO table id is not safe
                let object_path = format!("{}/{}", name, id);
                let auth = MemoryNodeState::build_storage_auth()?;
                let cell_log_path = format!(
                    "{}/{}/{}/{}/log/",
                    memory_node_confg.binlog_root_dir, db, name, id
                );
                let cell_tmp_path = format!(
                    "{}/{}/{}/{}/tmp/",
                    memory_node_confg.tmp_store_root_dir, db, name, id
                );
                let mut cell_config = CellStoreConfig::new(
                    db,
                    region.clone(),
                    &schema,
                    &cell_log_path,
                    auth,
                    &cell_tmp_path,
                    &object_path,
                    false,
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

    pub fn get_cell(&self, db: &str, table_id: &str, pid: i32) -> Option<Arc<CellStore>> {
        if let Some(db_map) = self.cells.get(db) {
            if let Some(table_map) = db_map.get(table_id) {
                table_map.get(&pid).cloned()
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn add_cell(
        &mut self,
        db: &str,
        table_id: &str,
        pid: i32,
        cell_store: Arc<CellStore>,
    ) -> Result<()> {
        if let Some(db_map) = self.cells.get_mut(db) {
            match db_map.get_mut(table_id) {
                Some(table_map) => match table_map.get(&pid) {
                    Some(_) => Err(RTStoreError::CellStoreExistError {
                        tid: table_id.to_string(),
                        pid,
                    }),
                    _ => {
                        table_map.insert(pid, cell_store);
                        Ok(())
                    }
                },
                _ => {
                    let mut table: HashMap<i32, Arc<CellStore>> = HashMap::new();
                    table.insert(pid, cell_store);
                    db_map.insert(table_id.to_string(), table);
                    Ok(())
                }
            }
        } else {
            let mut db_map: HashMap<String, HashMap<i32, Arc<CellStore>>> = HashMap::new();
            let mut table_map: HashMap<i32, Arc<CellStore>> = HashMap::new();
            table_map.insert(pid, cell_store);
            db_map.insert(table_id.to_string(), table_map);
            self.cells.insert(db.to_string(), db_map);
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
    meta_store: Arc<MetaStore>,
}

impl MemoryNodeImpl {
    pub fn new(config: MemoryNodeConfig, meta_store: Arc<MetaStore>) -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryNodeState::new())),
            config,
            meta_store,
        }
    }

    pub async fn init(&self) -> Result<()> {
        self.meta_store.add_node(&self.config.node).await?;
        Ok(())
    }

    pub fn start_l2_compaction(&self, db: &str, table_id: &str, pid: i32) {
        let local_table_id = table_id.to_string();
        let local_db = db.to_string();
        let local_state = self.state.clone();
        // TODO avoid start compaction repeated
        tokio::task::spawn(async move {
            loop {
                sleep(Duration::from_millis(1000 * 10)).await;
                let cell_opt = match local_state.lock() {
                    Ok(node_state) => node_state.get_cell(&local_db, &local_table_id, pid),
                    Err(_) => None,
                };
                if let Some(cell) = cell_opt {
                    if cell.do_l2_compaction().await.is_ok() {
                        debug!(
                            "do l2 compaction done for table {}, pid {}",
                            &local_table_id, pid
                        )
                    }
                } else {
                    warn!(
                        "partition {} of table {} exist from compaction",
                        &local_table_id, pid
                    );
                    break;
                }
            }
        });
    }

    pub fn get_cell(&self, db: &str, table_id: &str, pid: i32) -> Option<Arc<CellStore>> {
        match self.state.lock() {
            Ok(node_state) => node_state.get_cell(db, table_id, pid),
            Err(_) => None,
        }
    }
}

unsafe impl Send for MemoryNodeImpl {}

unsafe impl Sync for MemoryNodeImpl {}

#[tonic::async_trait]
impl MemoryNode for MemoryNodeImpl {
    type FetchPartitionStream = Pin<
        Box<dyn Stream<Item = std::result::Result<FlightData, Status>> + Send + Sync + 'static>,
    >;
    async fn fetch_partition(
        &self,
        request: Request<FetchPartitionRequest>,
    ) -> std::result::Result<Response<Self::FetchPartitionStream>, Status> {
        let fetch_request = request.into_inner();
        if let Some(cell_store) = self.get_cell(
            &fetch_request.db,
            &fetch_request.table_id,
            fetch_request.partition_id,
        ) {
            let batches = cell_store.get_memory_batch_snapshot()?;
            info!("batch size {}", batches.len());
            let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
            //TODO  empty check
            let schema_flight_data =
                SchemaAsIpc::new(batches[0].schema().as_ref(), &options).into();
            let mut flights: Vec<std::result::Result<FlightData, Status>> =
                vec![Ok(schema_flight_data)];
            let mut batches: Vec<std::result::Result<FlightData, Status>> = batches
                .iter()
                .flat_map(|batch| {
                    let (flight_dictionaries, flight_batch) =
                        flight_data_from_arrow_batch(batch, &options);
                    flight_dictionaries
                        .into_iter()
                        .chain(std::iter::once(flight_batch))
                        .map(Ok)
                })
                .collect();
            // append batch vector to schema vector, so that the first message sent is the schema
            flights.append(&mut batches);
            let output = futures::stream::iter(flights);
            Ok(Response::new(Box::pin(output) as Self::FetchPartitionStream))
        } else {
            Err(Status::from(RTStoreError::CellStoreNotFoundError {
                tid: fetch_request.table_id.to_string(),
                pid: fetch_request.partition_id,
            }))
        }
    }

    async fn append_records(
        &self,
        request: Request<AppendRecordsRequest>,
    ) -> std::result::Result<Response<AppendRecordsResponse>, Status> {
        let append_request = request.into_inner();
        if let Some(cell_store) = self.get_cell(
            &append_request.db,
            &append_request.table_id,
            append_request.partition_id,
        ) {
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
                        node_state.add_cell(&table_desc.db, &table_desc.name, id, cell)?;
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
                    self.start_l2_compaction(&table_desc.db, &table_desc.name, id);
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
    use crate::proto::rtstore_base_proto::{
        RtStoreColumnDesc, RtStoreNodeType, RtStoreSchemaDesc, RtStoreType,
    };
    use crate::store::build_readonly_meta_store;
    use std::thread;
    use tempdir::TempDir;

    fn build_config(tmp_dir_path: &str) -> MemoryNodeConfig {
        let node = RtStoreNode {
            endpoint: "http://127.0.0.1:9191".to_string(),
            node_type: RtStoreNodeType::KMemoryNode as i32,
            ns: "127.0.0.1".to_string(),
            port: 9191,
        };
        MemoryNodeConfig {
            binlog_root_dir: format!("{}/binlog_root_dir", tmp_dir_path).to_string(),
            tmp_store_root_dir: format!("{}/tmp_store_root_dir", tmp_dir_path).to_string(),
            etcd_cluster: "127.0.0.1:9191".to_string(),
            etcd_root_path: "/rtstore".to_string(),
            node,
        }
    }

    async fn build_memory_node() -> MemoryNodeImpl {
        let tmp_dir_path = TempDir::new("assign_partition").expect("create temp dir");
        let tmp_dir_path_str = tmp_dir_path.path().to_str().unwrap();
        let config = build_config(tmp_dir_path_str);
        let meta_store = build_readonly_meta_store(&config.etcd_cluster, &config.etcd_root_path)
            .await
            .unwrap();
        MemoryNodeImpl::new(config, Arc::new(meta_store))
    }

    #[tokio::test]
    async fn test_assign_partitions() {
        let db = "db22";
        let table = "tttt22";
        let memory_node = build_memory_node().await;
        let assign_req = create_assign_partition_request(table, db);
        let req = Request::new(assign_req);
        assert!(memory_node.assign_partition(req).await.is_ok());
        assert!(memory_node.get_cell(db, table, 3).is_none());
        assert!(memory_node.get_cell(db, table, 0).is_some());
    }

    #[tokio::test]
    async fn test_append_records_compaction() -> Result<()> {
        let db = "db333";
        let table = "ttt33";
        let memory_node = build_memory_node().await;
        let assign_req = create_assign_partition_request(table, db);
        let req = Request::new(assign_req);
        assert!(memory_node.assign_partition(req).await.is_ok());
        assert!(memory_node.get_cell(db, table, 3).is_none());
        assert!(memory_node.get_cell(db, table, 0).is_some());
        for _ in 0..102400 {
            let batch = gen_sample_row_batch();
            let data = encode(&batch)?;
            let req = Request::new(AppendRecordsRequest {
                table_id: table.to_string(),
                partition_id: 0,
                records: data,
                db: db.to_string(),
            });
            assert!(memory_node.append_records(req).await.is_ok());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_append_records() -> Result<()> {
        let db = "db11";
        let table = "btc_test";
        let memory_node = build_memory_node().await;
        let assign_req = create_assign_partition_request(table, db);
        let req = Request::new(assign_req);
        assert!(memory_node.assign_partition(req).await.is_ok());
        assert!(memory_node.get_cell(db, table, 3).is_none());
        let batch = gen_sample_row_batch();
        let data = encode(&batch)?;
        let req = Request::new(AppendRecordsRequest {
            table_id: table.to_string(),
            partition_id: 0,
            records: data,
            db: db.to_string(),
        });
        assert!(memory_node.get_cell(db, table, 0).is_some());
        assert!(memory_node.append_records(req).await.is_ok());
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
        }
    }

    fn create_assign_partition_request(tname: &str, db: &str) -> AssignPartitionRequest {
        let region = StorageRegion {
            region: "".to_string(),
            endpoint: "http://127.0.0.1:9000".to_string(),
        };

        let storage_config = StorageBackendConfig {
            bucket: "test_bk_1".to_string(),
            region: Some(region),
            l1_rows_limit: 1 * 1024,
            l2_rows_limit: 5 * 1024,
        };

        let table_desc = create_simple_table_desc(tname, db);
        let pids: Vec<i32> = vec![0, 1, 2];
        AssignPartitionRequest {
            partition_ids: pids,
            table_desc: Some(table_desc),
            config: Some(storage_config),
        }
    }

    fn create_simple_table_desc(tname: &str, db: &str) -> RtStoreTableDesc {
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
            name: tname.to_string(),
            schema: Some(schema),
            partition_desc: None,
            db: db.to_string(),
            ctime: 0,
            mappings: Vec::new(),
        }
    }
}
