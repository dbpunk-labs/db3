//
// mutation_sdk.rs
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

use chrono::Utc;
use db3_proto::db3_node_proto::{QuerySessionInfo, SessionStatus};
use ethereum_types::Address;
use std::collections::HashMap;
// default session timeout 1hrs
pub const DEFAULT_SESSION_PERIOD: i64 = 3600;
// default session limit
pub const DEFAULT_SESSION_QUERY_LIMIT: i32 = 1000;
// default session limit
pub const DEFAULT_SESSION_POOL_SIZE_LIMIT: usize = 1000;

pub struct SessionPool {
    session_pool: HashMap<i32, SessionManager>,
}
impl SessionPool {
    pub fn new() -> Self {
        SessionPool {
            session_pool: HashMap::new(),
        }
    }

    pub fn create_new_session(&mut self, id: i32) -> Result<i32, String> {
        if self.session_pool.len() >= DEFAULT_SESSION_POOL_SIZE_LIMIT {
            Err(format!(
                "Fail to create new session since session pool size exceed limit {}",
                DEFAULT_SESSION_POOL_SIZE_LIMIT
            ))
        } else if self.session_pool.contains_key(&id) {
            Err(format!(
                "Fail to create new session since session id {} already exist",
                id
            ))
        } else {
            self.session_pool
                .insert(id, SessionManager::create_session(id));
            Ok(id)
        }
    }

    pub fn remove_session(&mut self, session_id: i32) -> Result<i32, String> {
        if self.session_pool.contains_key(&session_id) {
            self.session_pool.remove(&session_id);
            Ok(session_id)
        } else {
            Err(format!("session {} not exist in session pool", session_id))
        }
    }

    pub fn get_session(&self, session_id: i32) -> Option<&SessionManager> {
        self.session_pool.get(&session_id)
    }
    pub fn get_session_mut(&mut self, session_id: i32) -> Option<&mut SessionManager> {
        self.session_pool.get_mut(&session_id)
    }
}
pub struct SessionStore {
    session_pools: HashMap<Address, SessionPool>,
    uuid: i32,
}

impl SessionStore {
    pub fn new() -> Self {
        SessionStore {
            session_pools: HashMap::new(),
            uuid: 0,
        }
    }

    /// Add session into pool
    pub fn add_new_session(&mut self, addr: Address) -> Result<i32, String> {
        self.uuid += 1;
        match self.session_pools.get_mut(&addr) {
            Some(sess_pool) => sess_pool.create_new_session(self.uuid),
            None => {
                let mut sess_pool = SessionPool::new();
                let res = sess_pool.create_new_session(self.uuid);
                if res.is_ok() {
                    self.session_pools.insert(addr, sess_pool);
                }
                res
            }
        }
    }
    pub fn remove_session(&mut self, addr: Address, session_id: i32) -> Result<i32, String> {
        match self.session_pools.get_mut(&addr) {
            Some(sess_pool) => sess_pool.remove_session(session_id),
            None => Err(format!(
                "Fail to remove session since  {}",
                DEFAULT_SESSION_POOL_SIZE_LIMIT
            )),
        }
    }
    pub fn is_session_exist(self, addr: Address, session_id: i32) -> bool {
        match self.session_pools.get(&addr) {
            Some(sess_pool) => sess_pool.session_pool.contains_key(&session_id),
            None => false,
        }
    }
    pub fn get_session_mut(
        &mut self,
        addr: Address,
        session_id: i32,
    ) -> Option<&mut SessionManager> {
        match self.session_pools.get_mut(&addr) {
            Some(sess_pool) => sess_pool.session_pool.get_mut(&session_id),
            None => None,
        }
    }
}

#[derive(Debug)]
pub struct SessionManager {
    session_info: QuerySessionInfo,
}

impl SessionManager {
    pub fn new() -> Self {
        Self::create_session(0)
    }
    pub fn create_session(id: i32) -> Self {
        SessionManager {
            session_info: QuerySessionInfo {
                id,
                start_time: Utc::now().timestamp(),
                query_count: 0,
                status: SessionStatus::Running.into(),
            },
        }
    }
    pub fn get_session_info(&self) -> QuerySessionInfo {
        self.session_info.clone()
    }
    pub fn get_session_id(&self) -> i32 {
        self.session_info.id
    }
    pub fn get_start_time(&self) -> i64 {
        self.session_info.start_time
    }
    pub fn get_session_query_count(&self) -> i32 {
        self.session_info.query_count
    }
    pub fn check_session_running(&mut self) -> bool {
        self.check_session_status() == SessionStatus::Running.into()
    }
    pub fn check_session_status(&mut self) -> SessionStatus {
        match SessionStatus::from_i32(self.session_info.status) {
            Some(SessionStatus::Running) => {
                if Utc::now().timestamp() - self.session_info.start_time > DEFAULT_SESSION_PERIOD {
                    self.session_info.status = SessionStatus::Blocked.into();
                } else if self.session_info.query_count >= DEFAULT_SESSION_QUERY_LIMIT {
                    self.session_info.status = SessionStatus::Blocked.into();
                }
            }
            _ => {}
        }
        SessionStatus::from_i32(self.session_info.status).unwrap()
    }
    pub fn close_session(&mut self) {
        self.session_info.status = SessionStatus::Stop.into();
    }
    pub fn reset_session(&mut self) {
        self.session_info.query_count = 0;
        self.session_info.status = SessionStatus::Running.into();
        self.session_info.start_time = Utc::now().timestamp();
        self.session_info.id += 1;
    }
    pub fn increase_query(&mut self, count: i32) {
        self.session_info.query_count += count;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use db3_base::get_address_from_pk;
    use db3_proto::db3_node_proto::SessionStatus;
    use fastcrypto::secp256k1::Secp256k1PublicKey;
    use fastcrypto::traits::ToFromBytes;
    use hex;
    use std::str::FromStr;
    #[test]
    fn test_new_session() {
        let mut session = SessionManager::new();
        assert_eq!(SessionStatus::Running, (session.check_session_status()));
    }

    #[test]
    fn update_session_status_happy_path() {
        let mut session = SessionManager::new();
        assert_eq!(SessionStatus::Running, session.check_session_status());
    }

    #[test]
    fn query_exceed_limit_session_blocked() {
        let mut session = SessionManager::new();
        session.check_session_status();
        assert_eq!(SessionStatus::Running, session.check_session_status());
        session.increase_query(DEFAULT_SESSION_QUERY_LIMIT + 1);
        session.check_session_status();
        assert_eq!(SessionStatus::Blocked, session.check_session_status());
    }

    #[test]
    fn close_session_test() {
        let mut session = SessionManager::new();
        assert_eq!(SessionStatus::Running, session.check_session_status());
        session.close_session();
        assert_eq!(SessionStatus::Stop, session.check_session_status());
    }

    #[test]
    fn add_session_test() {
        let mut sess_store = SessionStore::new();
        let pk = Secp256k1PublicKey::from_bytes(
            &hex::decode("03ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138")
                .unwrap(),
        );
        let addr = get_address_from_pk(&pk.unwrap().pubkey);

        // add session and create new session pool
        {
            let res = sess_store.add_new_session(addr);
            assert!(res.is_ok());
            assert_eq!(1, res.unwrap());
        }

        // add session into existing session pool
        {
            let res = sess_store.add_new_session(addr);
            assert!(res.is_ok());
            assert_eq!(2, res.unwrap());
        }
    }

    #[test]
    fn add_session_exceed_limit() {
        let mut sess_store = SessionStore::new();
        let pk = Secp256k1PublicKey::from_bytes(
            &hex::decode("03ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138")
                .unwrap(),
        );
        let addr = get_address_from_pk(&pk.unwrap().pubkey);

        for i in 0..DEFAULT_SESSION_POOL_SIZE_LIMIT {
            assert!(sess_store.add_new_session(addr).is_ok())
        }

        let res = sess_store.add_new_session(addr);
        assert!(res.is_err());
        assert_eq!(
            "Fail to create new session since session pool size exceed limit 1000",
            res.err().unwrap()
        );
    }
    #[test]
    fn get_session() {
        let mut sess_store = SessionStore::new();
        let pk = Secp256k1PublicKey::from_bytes(
            &hex::decode("03ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138")
                .unwrap(),
        );
        let addr = get_address_from_pk(&pk.unwrap().pubkey);
        // add session and create new session pool
        {
            let res = sess_store.add_new_session(addr);
            assert!(res.is_ok());
            assert_eq!(1, res.unwrap());
        }
        // add session into existing session pool
        {
            let res = sess_store.add_new_session(addr);
            assert!(res.is_ok());
            assert_eq!(2, res.unwrap());
        }
        {
            let res = sess_store.get_session_mut(addr, 1);
            assert!(res.is_some());
            assert_eq!(res.unwrap().get_session_id(), 1);
        }
        {
            let res = sess_store.get_session_mut(addr, 3);
            assert!(res.is_none());
        }
    }

    #[test]
    fn remove_session_test() {
        let mut sess_store = SessionStore::new();
        let pk = Secp256k1PublicKey::from_bytes(
            &hex::decode("03ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138")
                .unwrap(),
        );
        let addr = get_address_from_pk(&pk.unwrap().pubkey);

        // add session and create new session pool
        {
            let res = sess_store.add_new_session(addr);
            assert!(res.is_ok());
            assert_eq!(1, res.unwrap());
        }

        // add session into existing session pool
        {
            let res = sess_store.add_new_session(addr);
            assert!(res.is_ok());
            assert_eq!(2, res.unwrap());
        }
        {
            let res = sess_store.remove_session(addr, 2);
            assert!(res.is_ok());
            assert_eq!(2, res.unwrap());
        }
        {
            let res = sess_store.remove_session(addr, 2);
            assert!(res.is_err());
            assert_eq!("session 2 not exist in session pool", res.err().unwrap());
        }
    }
}
