//
//
// catalog.rs
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

use super::table::Table;
use crate::base::arrow_parquet_utils::*;
use crate::base::time_utils;
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::{RtStoreDatabase, RtStoreTableDesc, StorageRegion};
use crate::sdk::memory_node_sdk::MemoryNodeSDK;
use crate::store::meta_store::MetaStore;
use crate::store::object_store::build_region;
use bytes::Bytes;
use crossbeam_skiplist_piedb::SkipMap;
use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use etcd_client::EventType;
use prost::Message;
use s3::region::Region;
use std::collections::HashMap;

uselog!(info, warn);
use std::any::Any;
use std::sync::Arc;

#[derive(Clone)]
pub struct Database {
    // name of database and s3 bucket name
    db: String,
    // tables in database
    tables: Arc<SkipMap<String, Arc<Table>>>,
    // s3 region config
    region: Region,
    meta_store: Arc<MetaStore>,
    ctime: i64,
}

impl Database {
    pub fn from_db_desc(db_desc: &RtStoreDatabase, meta_store: Arc<MetaStore>) -> Result<Self> {
        match &db_desc.region {
            Some(r) => {
                let region = build_region(&r.region);
                Ok(Self {
                    db: db_desc.db.to_string(),
                    tables: Arc::new(SkipMap::new()),
                    region,
                    meta_store,
                    ctime: db_desc.ctime,
                })
            }
            _ => {
                warn!(
                    "region is required but db with name {} does not have it",
                    db_desc.db
                );
                Err(RTStoreError::DBInvalidInput)
            }
        }
    }

    pub fn new(db: &str, region: Region, meta_store: Arc<MetaStore>) -> Database {
        Self {
            db: db.to_string(),
            tables: Arc::new(SkipMap::new()),
            region,
            meta_store,
            ctime: time_utils::now_in_second(),
        }
    }

    pub async fn recover(&self) -> Result<()> {
        let tables = self.meta_store.get_tables(&self.db).await?;
        for table in tables {
            self.create_table(&table, true).await?;
        }
        Ok(())
    }

    pub fn get_table_names(&self) -> Vec<String> {
        let mut table_names: Vec<String> = Vec::new();
        for e in self.tables.iter() {
            table_names.push(e.key().clone());
        }
        table_names
    }

    pub async fn create_table(&self, table_desc: &RtStoreTableDesc, recover: bool) -> Result<()> {
        let schema = match &table_desc.schema {
            Some(s) => table_desc_to_arrow_schema(s),
            _ => {
                warn!("table {} schema is invalid", &table_desc.name);
                Err(RTStoreError::TableSchemaInvalidError {
                    name: table_desc.name.to_string(),
                })
            }
        }?;
        let table = Arc::new(Table::new(&table_desc.clone(), schema));
        let table = self
            .tables
            .insert(table_desc.name.clone(), table)
            .value()
            .clone();
        let mut nodes: HashMap<String, MemoryNodeSDK> = HashMap::new();
        for partition_node in table_desc.mappings.iter() {
            if partition_node.node_list.is_empty() {
                continue;
            }
            if !nodes.contains_key(&partition_node.node_list[0]) {
                let node = MemoryNodeSDK::connect(&partition_node.node_list[0])
                    .await
                    .map_err(|e| {
                        warn!("fail to connect to memory node for error {}", e);
                        RTStoreError::RPCConnectError(e)
                    })?;
                nodes.insert(partition_node.node_list[0].to_string(), node);
            }
            table.assign_partition_to_node(
                partition_node.partition_id,
                nodes.get(&partition_node.node_list[0]).unwrap().clone(),
            )?;
        }

        if !recover {
            self.meta_store.add_table(table_desc).await?;
        }
        Ok(())
    }

    pub fn get_table(&self, table_name: &str) -> Result<Arc<Table>> {
        let table_entry = self.tables.get(table_name);
        match table_entry {
            Some(entry) => Ok(entry.value().clone()),
            _ => Err(RTStoreError::TableNotFoundError {
                tname: table_name.to_string(),
            }),
        }
    }

    pub fn to_db_desc(&self) -> RtStoreDatabase {
        if let Region::Custom { region, endpoint } = &self.region {
            let sregion = StorageRegion {
                region: region.to_string(),
                endpoint: endpoint.to_string(),
            };
            RtStoreDatabase {
                db: self.db.to_string(),
                ctime: self.ctime,
                region: Some(sregion),
            }
        } else {
            let sregion = StorageRegion {
                region: format!("{}", self.region),
                endpoint: "".to_string(),
            };
            RtStoreDatabase {
                db: self.db.to_string(),
                ctime: self.ctime,
                region: Some(sregion),
            }
        }
    }
}

impl SchemaProvider for Database {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.get_table_names()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        match self.get_table(name) {
            Ok(t) => Some(t.clone()),
            _ => None,
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[derive(Clone)]
pub struct Catalog {
    dbs: Arc<SkipMap<String, Arc<Database>>>,
    meta_store: Arc<MetaStore>,
}

impl Catalog {
    pub fn new(meta_store: Arc<MetaStore>) -> Self {
        Self {
            dbs: Arc::new(SkipMap::new()),
            meta_store,
        }
    }

    pub async fn recover(&self) -> Result<()> {
        let dbs = self.meta_store.get_dbs().await?;
        for db_desc in dbs {
            let name = db_desc.db.to_string();
            let database = Arc::new(Database::from_db_desc(&db_desc, self.meta_store.clone())?);
            database.recover().await?;
            info!("recover database {} ok", name);
            self.dbs.get_or_insert_with(name, || database);
        }
        Ok(())
    }

    pub async fn subscribe_changes(catalog: &Arc<Self>) {
        let local_self = catalog.clone();
        tokio::task::spawn(async move {
            // just subscribe table events
            if let Ok(mut stream) = local_self.meta_store.subscribe_table_events().await {
                while let Ok(Some(resp)) = stream.message().await {
                    if resp.canceled() {
                        warn!("canceled watch table event");
                        break;
                    }
                    let mut new_add_tables: Vec<RtStoreTableDesc> = Vec::new();
                    for event in resp.events() {
                        match (event.event_type(), event.kv()) {
                            (EventType::Put, Some(kv)) => {
                                let buf = Bytes::from(kv.value().to_vec());
                                match RtStoreTableDesc::decode(buf) {
                                    Ok(table) => new_add_tables.push(table),
                                    Err(e) => {
                                        warn!("fail to decode table for error {}", e);
                                    }
                                }
                            }
                            (_, _) => {
                                //TODO handle delete
                            }
                        }
                    }
                    for table_desc in new_add_tables {
                        info!("new table {} to be added", table_desc.name);
                        if let Ok(database) = local_self.get_db(&table_desc.db) {
                            if let Err(e) = database.create_table(&table_desc, true).await {
                                warn!("fail  to create table for error {}", e);
                            }
                        } else {
                            let db_desc =
                                local_self.meta_store.get_db(&table_desc.db).await.unwrap();
                            let name = db_desc.db.to_string();
                            let database = Arc::new(
                                Database::from_db_desc(&db_desc, local_self.meta_store.clone())
                                    .unwrap(),
                            );
                            database.create_table(&table_desc, true).await.unwrap();
                            local_self.dbs.get_or_insert_with(name, || database);
                        }
                    }
                }
            }
        });
    }

    pub async fn create_db(&self, name: &str, region: Region) -> Result<()> {
        if self.dbs.contains_key(name) {
            warn!("new database with name {} exist", name);
            return Err(RTStoreError::DBNameExistError(name.to_string()));
        }
        let db = Database::new(name, region, self.meta_store.clone());
        let db_desc = db.to_db_desc();
        self.dbs
            .get_or_insert_with(name.to_string(), || Arc::new(db));
        self.meta_store.add_db(&db_desc).await?;
        info!("create database {} ok ", name);
        Ok(())
    }

    pub fn get_db(&self, name: &str) -> Result<Arc<Database>> {
        let db_entry = self.dbs.get(name);
        match db_entry {
            Some(entry) => Ok(entry.value().clone()),
            _ => Err(RTStoreError::DBNotFoundError(name.to_string())),
        }
    }

    pub fn get_db_names(&self) -> Vec<String> {
        let mut db_names: Vec<String> = Vec::new();
        for e in self.dbs.iter() {
            db_names.push(e.key().clone());
        }
        db_names
    }
}

impl CatalogProvider for Catalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.get_db_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match self.get_db(name) {
            Ok(db) => Some(db),
            _ => None,
        }
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {}
