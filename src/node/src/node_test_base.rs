//
// node_test_base.rs
// Copyright (C) 2023 db3.network Author imotai <codego.me@gmail.com>
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

#[cfg(test)]
pub mod tests {
    use crate::recover::{Recover, RecoverConfig, RecoverType};
    use crate::rollup_executor::{RollupExecutor, RollupExecutorConfig};
    use db3_crypto::db3_address::DB3Address;
    use db3_error::Result;
    use db3_proto::db3_base_proto::SystemConfig;
    use db3_proto::db3_mutation_v2_proto::MutationAction;
    use db3_storage::db_store_v2::{DBStoreV2, DBStoreV2Config};
    use db3_storage::doc_store::DocStoreConfig;
    use db3_storage::mutation_store::{MutationStore, MutationStoreConfig};
    use db3_storage::state_store::StateStore;
    use db3_storage::state_store::StateStoreConfig;
    use db3_storage::system_store::{SystemRole, SystemStore, SystemStoreConfig};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempdir::TempDir;
    pub struct NodeTestBase {}
    impl NodeTestBase {
        pub fn generate_config(
            tmp_dir_path: &TempDir,
        ) -> (
            StateStoreConfig,
            SystemStoreConfig,
            MutationStoreConfig,
            RollupExecutorConfig,
            DBStoreV2Config,
            RecoverConfig,
            RecoverConfig,
        ) {
            let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
            let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let key_root_path = path
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("tools/keys")
                .to_str()
                .unwrap()
                .to_string();
            let rollup_config = RollupExecutorConfig {
                temp_data_path: format!("{real_path}/rollup_data_path"),
                key_root_path: key_root_path.to_string(),
                use_legacy_tx: false,
            };
            if let Err(_e) = std::fs::create_dir_all(rollup_config.temp_data_path.as_str()) {
                println!("create dir error");
            } else {
                println!(
                    "create dir {} success",
                    rollup_config.temp_data_path.as_str()
                );
            }
            let system_store_config = SystemStoreConfig {
                key_root_path: key_root_path.to_string(),
                evm_wallet_key: "evm".to_string(),
                ar_wallet_key: "ar".to_string(),
            };

            let store_config = MutationStoreConfig {
                db_path: format!("{real_path}/mutation_path"),
                block_store_cf_name: "block_store_cf".to_string(),
                tx_store_cf_name: "tx_store_cf".to_string(),
                rollup_store_cf_name: "rollup_store_cf".to_string(),
                gc_cf_name: "gc_store_cf".to_string(),
                message_max_buffer: 4 * 1024,
                scan_max_limit: 50,
                block_state_cf_name: "block_state_cf".to_string(),
            };
            let state_config = StateStoreConfig {
                db_path: format!("{real_path}/state_store"),
            };

            let db_store_config = DBStoreV2Config {
                db_path: format!("{real_path}/db_store"),
                db_store_cf_name: "db".to_string(),
                doc_store_cf_name: "doc".to_string(),
                collection_store_cf_name: "cf2".to_string(),
                index_store_cf_name: "index".to_string(),
                doc_owner_store_cf_name: "doc_owner".to_string(),
                db_owner_store_cf_name: "db_owner".to_string(),
                scan_max_limit: 50,
                enable_doc_store: false,
                doc_store_conf: DocStoreConfig::default(),
                doc_start_id: 1000,
            };

            let recover_index_config = RecoverConfig {
                key_root_path: key_root_path.to_string(),
                temp_data_path: format!("{real_path}/recover_index_temp_data"),
                recover_type: RecoverType::Index,
            };
            if let Err(_e) = std::fs::create_dir_all(recover_index_config.temp_data_path.as_str()) {
            }
            let recover_rollup_config = RecoverConfig {
                key_root_path: key_root_path.to_string(),
                temp_data_path: format!("{real_path}/recover_rollup_temp_data"),
                recover_type: RecoverType::Rollup,
            };
            if let Err(_e) = std::fs::create_dir_all(recover_rollup_config.temp_data_path.as_str())
            {
            }

            (
                state_config,
                system_store_config,
                store_config,
                rollup_config,
                db_store_config,
                recover_rollup_config,
                recover_index_config,
            )
        }

        pub fn mock_system_config() -> SystemConfig {
            SystemConfig {
                min_rollup_size: 1024,
                rollup_interval: 1000,
                network_id: 1,
                evm_node_url: "ws://127.0.0.1:8545".to_string(),
                ar_node_url: "http://127.0.0.1:1984".to_string(),
                chain_id: 31337_u32,
                rollup_max_interval: 2000,
                contract_addr: "0x5FbDB2315678afecb367f032d93F642f64180aa3".to_string(),
                min_gc_offset: 100,
            }
        }

        pub fn add_mutations(storage: &MutationStore, rows: u64) -> u64 {
            let payload: Vec<u8> = vec![1];
            let signature: &str = "0xasdasdsad";
            let (_id, block, _order) = storage
                .generate_mutation_block_and_order(payload.as_ref(), signature)
                .unwrap();
            for _i in 0..rows {
                let (_id, block, order) = storage
                    .generate_mutation_block_and_order(payload.as_ref(), signature)
                    .unwrap();
                let result = storage.add_mutation(
                    payload.as_ref(),
                    signature,
                    "",
                    &DB3Address::ZERO,
                    1,
                    block,
                    order,
                    1,
                    MutationAction::CreateDocumentDb,
                );
                assert_eq!(true, result.is_ok());
            }
            block
        }

        pub async fn setup_for_smoke_test(
            tmp_dir_path: &TempDir,
        ) -> Result<(RollupExecutor, Recover, MutationStore)> {
            let (
                state_config,
                system_store_config,
                store_config,
                rollup_config,
                db_config,
                recover_rollup_config,
                _,
            ) = NodeTestBase::generate_config(tmp_dir_path);
            let state_store = Arc::new(StateStore::new(state_config).unwrap());
            let system_store = Arc::new(SystemStore::new(system_store_config, state_store.clone()));
            let storage = MutationStore::new(store_config).unwrap();
            storage.recover().unwrap();
            let system_config = NodeTestBase::mock_system_config();
            let result = system_store.update_config(&SystemRole::DataRollupNode, &system_config);
            let db_store = DBStoreV2::new(db_config)?;
            assert_eq!(true, result.is_ok());
            Self::add_mutations(&storage, 3);
            let (_, _) = storage.increase_block_return_last_state()?;
            let rollup_executor =
                RollupExecutor::new(rollup_config, storage.clone(), system_store.clone()).await?;
            let rollup_recover = Recover::new(
                recover_rollup_config,
                db_store.clone(),
                system_store.clone(),
                None,
            )
            .await?;
            Ok((rollup_executor, rollup_recover, storage))
        }
    }
}
