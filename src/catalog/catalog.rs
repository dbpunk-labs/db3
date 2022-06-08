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

use crate::base::arrow_parquet_utils::*;
use crate::base::time_utils;
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::{
    RtStoreDatabase, RtStoreNode, RtStoreTableDesc, StorageRegion,
};
use crate::store::meta_store::MetaStore;
use crate::store::object_store::build_region;
use arrow::datatypes::{Schema, SchemaRef};
use chrono::offset::Utc;
use crossbeam_skiplist_piedb::SkipMap;
use s3::region::Region;

uselog!(info, warn);
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
                let mut endpoint: Option<String> = None;
                // not custom region
                if !r.endpoint.is_empty() {
                    endpoint = Some(r.endpoint.to_string());
                }
                let region = build_region(&r.region, endpoint);
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
        // add mutex
        if self.tables.contains_key(&table_desc.name) {
            warn!("new table with name {} exist", &table_desc.name);
            return Err(RTStoreError::TableNamesExistError {
                name: table_desc.name.to_string(),
            });
        }

        let schema = match &table_desc.schema {
            Some(s) => table_desc_to_arrow_schema(s),
            _ => {
                warn!("table {} schema is invalid", &table_desc.name);
                Err(RTStoreError::TableSchemaInvalidError {
                    name: table_desc.name.to_string(),
                })
            }
        }?;
        let table = Arc::new(Table {
            desc: table_desc.clone(),
            parquet_schema: schema,
            partition_to_nodes: Arc::new(SkipMap::new()),
        });
        self.tables
            .get_or_insert_with(table_desc.name.clone(), || table);
        if !recover {
            self.meta_store
                .add_table(&self.db, &table_desc.name, table_desc)
                .await?;
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

#[derive(Clone)]
pub struct Table {
    desc: RtStoreTableDesc,
    parquet_schema: SchemaRef,
    partition_to_nodes: Arc<SkipMap<i32, RtStoreNode>>,
}

impl Table {
    pub fn assign_partition_to_node(&self, pid: i32, node: RtStoreNode) -> Result<()> {
        self.partition_to_nodes.remove(&pid);
        self.partition_to_nodes.get_or_insert_with(pid, || node);
        Ok(())
    }
    #[inline]
    pub fn get_table_desc(&self) -> &RtStoreTableDesc {
        &self.desc
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
    pub fn get_node_by_partition(&self, pid: i32) -> Option<RtStoreNode> {
        let node_entry = self.partition_to_nodes.get(&pid);
        match node_entry {
            Some(entry) => {
                let node = entry.value();
                Some(node.clone())
            }
            _ => None,
        }
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
