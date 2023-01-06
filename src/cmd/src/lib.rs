//
// lib.rs
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

use db3_base::{get_address_from_pk, strings};
use db3_proto::db3_account_proto::Account;
use db3_proto::db3_base_proto::{ChainId, ChainRole, UnitType, Units};
use db3_proto::db3_mutation_proto::{KvPair, Mutation, MutationAction};
use db3_proto::db3_node_proto::OpenSessionResponse;
use db3_proto::db3_session_proto::SessionStatus;
use db3_sdk::mutation_sdk::MutationSDK;
use db3_sdk::store_sdk::StoreSDK;
use dirs;
use ed25519_dalek::Keypair;
use rand::rngs::OsRng;
use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
#[macro_use]
extern crate prettytable;
use prettytable::{format, Table};
use std::process::exit;

const HELP: &str = r#"the help of db3 command
help    show all command
put     write pairs of key and value to db3 e.g. put ns1 key1 value1 key2 values
del     delete key from db3                 e.g. del ns1 key1 key2
get     get value from db3                  e.g. get ns1 key1 key2
range   get a range from db3                e.g. range ns1 start_key end_key
account get balance of current account
blocks  get latest blocks
session info        get session info    e.g session info
quit    quit command line console
"#;

fn current_seconds() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => 0,
    }
}

pub fn get_key_pair(warning: bool) -> std::io::Result<Keypair> {
    let mut home_dir = dirs::home_dir().unwrap();
    home_dir.push(".db3");
    let user_dir = home_dir.as_path();
    std::fs::create_dir_all(user_dir)?;
    home_dir.push("user.key");
    let key_path = home_dir.as_path();
    if warning {
        println!(
            "WARNING, db3 will generate private key and save it to {}",
            key_path.to_string_lossy()
        );
    }
    if key_path.exists() {
        let kp_bytes = std::fs::read(key_path)?;
        let key_pair = Keypair::from_bytes(kp_bytes.as_ref()).unwrap();
        let addr = get_address_from_pk(&key_pair.public);
        if warning {
            println!("restore the key with addr {:?}", addr);
        }
        Ok(key_pair)
    } else {
        let mut rng = OsRng {};
        let kp: Keypair = Keypair::generate(&mut rng);
        let addr = get_address_from_pk(&kp.public);
        let kp_bytes = kp.to_bytes();
        let mut f = File::create(key_path)?;
        f.write_all(kp_bytes.as_ref())?;
        f.sync_all()?;
        if warning {
            println!("create new key with addr {:?}", addr);
        }
        Ok(kp)
    }
}

fn show_account(account: &Account) {
    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    table.add_row(row![
        "total bills",
        "storage used",
        "mutation",
        "querys",
        "credits"
    ]);
    let inner_account = account.clone();
    let bills = inner_account.total_bills;
    let credits = inner_account.credits;
    table.add_row(row![
        strings::units_to_readable_num_str(&bills.unwrap()),
        strings::bytes_to_readable_num_str(account.total_storage_in_bytes),
        account.total_mutation_count,
        account.total_query_session_count,
        strings::units_to_readable_num_str(&credits.unwrap())
    ]);
    table.printstd();
}

/// open new session
async fn open_session(store_sdk: &mut StoreSDK, session: &mut Option<OpenSessionResponse>) -> bool {
    match store_sdk.open_session().await {
        Ok(open_session_info) => {
            *session = Some(open_session_info);
            println!("Open Session Successfully!\n{:?}", session.as_ref());
            return true;
        }
        Err(e) => {
            println!("Open Session Error: {}", e);
            return false;
        }
    }
}

/// close current session
async fn close_session(
    store_sdk: &mut StoreSDK,
    session: &mut Option<OpenSessionResponse>,
) -> bool {
    if session.is_none() {
        return true;
    }
    match store_sdk
        .close_session(&session.as_ref().unwrap().session_token)
        .await
    {
        Ok((sess_info_node, sess_info_client, hash)) => {
            println!(
                "Close Session Successfully!\nNode session {:?}\nClient session: {:?}\nSubmit query session tx: {}",
                sess_info_node, sess_info_client, hash
            );
            // set session_id to 0
            *session = None;
            return true;
        }
        Err(e) => {
            println!("Close Session Error: {}", e);
            return false;
        }
    }
}
/// restart session when current session is invalid/closed/blocked
async fn refresh_session(
    store_sdk: &mut StoreSDK,
    session: &mut Option<OpenSessionResponse>,
) -> bool {
    if session.is_none() {
        return open_session(store_sdk, session).await;
    }
    if store_sdk
        .get_session_info(&session.as_ref().unwrap().session_token)
        .await
        .map_err(|e| {
            println!("{:?}", e);
            return false;
        })
        .unwrap()
        .status
        != SessionStatus::Running as i32
    {
        println!("Refresh session...");
        return close_session(store_sdk, session).await && open_session(store_sdk, session).await;
    }
    return true;
}
pub async fn process_cmd(
    sdk: &MutationSDK,
    store_sdk: &mut StoreSDK,
    cmd: &str,
    session: &mut Option<OpenSessionResponse>,
) -> bool {
    let parts: Vec<&str> = cmd.split(" ").collect();
    if parts.len() < 1 {
        println!("{}", HELP);
        return false;
    }
    let cmd = parts[0];
    // session info: {session_id, max_query_limit,
    match cmd {
        "help" => {
            println!("{}", HELP);
            return true;
        }
        "quit" => {
            close_session(store_sdk, session).await;
            println!("Good bye!");
            exit(1);
        }
        "account" => {
            let kp = get_key_pair(false).unwrap();
            let addr = get_address_from_pk(&kp.public);
            let account = store_sdk.get_account(&addr).await.unwrap();
            show_account(&account);
            return true;
        }
        "session" => {
            if parts.len() < 2 {
                println!("no enough command, e.g. session info | session restart");
                return false;
            }
            let op = parts[1];
            match op {
                "info" => {
                    // TODO(chenjing): show history session list
                    if session.is_none() {
                        println!("start a session before query session info");
                        return true;
                    }
                    if let Ok(session_info) = store_sdk
                        .get_session_info(&session.as_ref().unwrap().session_token)
                        .await
                    {
                        println!("{:?}", session_info);
                        return true;
                    } else {
                        println!("empty set");
                        return false;
                    }
                }
                _ => {}
            }
        }
        "range" | "blocks" => {
            println!("to be provided");
            return false;
        }
        _ => {}
    }
    if parts.len() < 3 {
        println!("no enough command, e.g. put n1 k1 v1 k2 v2 k3 v3");
        return false;
    }

    let ns = parts[1];
    let mut pairs: Vec<KvPair> = Vec::new();
    match cmd {
        "get" => {
            if !refresh_session(store_sdk, session).await {
                return false;
            }

            let mut keys: Vec<Vec<u8>> = Vec::new();
            for i in 2..parts.len() {
                keys.push(parts[i].as_bytes().to_vec());
            }
            if let Ok(Some(values)) = store_sdk
                .batch_get(
                    ns.as_bytes(),
                    keys,
                    &session.as_ref().unwrap().session_token,
                )
                .await
                .map_err(|e| {
                    println!("{:?}", e);
                    return false;
                })
            {
                for kv in values.values {
                    println!(
                        "{} -> {}",
                        std::str::from_utf8(kv.key.as_ref()).unwrap(),
                        std::str::from_utf8(kv.value.as_ref()).unwrap()
                    );
                }
                return true;
            }
        }
        "put" => {
            if parts.len() < 4 {
                println!("no enough command, e.g. put n1 k1 v1 k2 v2 k3 v3");
                return false;
            }
            for i in 1..parts.len() / 2 {
                pairs.push(KvPair {
                    key: parts[i * 2].as_bytes().to_vec(),
                    value: parts[i * 2 + 1].as_bytes().to_vec(),
                    action: MutationAction::InsertKv.into(),
                });
            }
        }
        "del" => {
            for i in 2..parts.len() {
                pairs.push(KvPair {
                    key: parts[i].as_bytes().to_vec(),
                    value: vec![],
                    action: MutationAction::DeleteKv.into(),
                });
            }
        }
        _ => todo!(),
    }
    let mutation = Mutation {
        ns: ns.as_bytes().to_vec(),
        kv_pairs: pairs.to_owned(),
        nonce: current_seconds(),
        gas_price: Some(Units {
            utype: UnitType::Tai.into(),
            amount: 100,
        }),
        gas: 100,
        chain_id: ChainId::DevNet.into(),
        chain_role: ChainRole::StorageShardChain.into(),
    };

    if let Ok(_) = sdk.submit_mutation(&mutation).await {
        println!("submit mutation to mempool done!");
        return true;
    } else {
        println!("fail to submit mutation to mempool");
        return false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_base::get_a_static_keypair;
    use db3_crypto::signer::Db3Signer;
    use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
    use db3_session::session_manager::DEFAULT_SESSION_QUERY_LIMIT;
    use std::sync::Arc;
    use std::{thread, time};
    use tonic::transport::Endpoint;
    #[tokio::test]
    async fn cmd_smoke_test() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let mclient = client.clone();

        let kp = get_a_static_keypair();
        let signer = Db3Signer::new(kp);
        let msdk = MutationSDK::new(mclient, signer);
        let kp = get_a_static_keypair();
        let signer = Db3Signer::new(kp);
        let mut sdk = StoreSDK::new(client, signer);
        let mut session: Option<OpenSessionResponse> = None;

        // Put kv store
        assert!(
            process_cmd(
                &msdk,
                &mut sdk,
                "put cmd_smoke_test k1 v1 k2 v2 k3 v3",
                &mut session
            )
            .await
        );
        thread::sleep(time::Duration::from_millis(2000));

        // Get kv store
        assert!(process_cmd(&msdk, &mut sdk, "get cmd_smoke_test k1 k2 k3", &mut session).await);

        // Refresh session
        let session_token1 = session.as_ref().unwrap().session_token.clone();
        assert!(!session_token1.is_empty());
        for _ in 0..(DEFAULT_SESSION_QUERY_LIMIT + 10) {
            assert!(
                process_cmd(&msdk, &mut sdk, "get cmd_smoke_test k1 k2 k3", &mut session).await
            );
        }
        let session_token2 = session.as_ref().unwrap().session_token.clone();
        assert_ne!(session_token2, session_token1);

        // Del kv store
        assert!(process_cmd(&msdk, &mut sdk, "del cmd_smoke_test k1", &mut session).await);
        thread::sleep(time::Duration::from_millis(2000));
    }
    #[tokio::test]
    async fn open_session_test() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));

        let kp = get_a_static_keypair();
        let signer = Db3Signer::new(kp);
        let mut sdk = StoreSDK::new(client, signer);
        let mut session: Option<OpenSessionResponse> = None;
        assert!(open_session(&mut sdk, &mut session).await);
        assert!(!session.as_ref().unwrap().session_token.is_empty());
    }
}
