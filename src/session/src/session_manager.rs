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
use uuid::Uuid;

// retry generate token
pub const GEN_TOKEN_RETRY: i32 = 10;
// default session timeout 1hrs
pub const DEFAULT_SESSION_PERIOD: i64 = 3600;
// default session limit
pub const DEFAULT_SESSION_QUERY_LIMIT: i32 = 1000;
// default session limit
pub const DEFAULT_SESSION_POOL_SIZE_LIMIT: usize = 1000;

// default session clean period 1 min
pub const DEFAULT_CLEANUP_SESSION_PERIOD: i64 = 60;

pub struct SessionPool {
    session_pool: HashMap<String, SessionManager>,
    last_cleanup_time: i64,
}

impl SessionPool {
    pub fn new() -> Self {
        SessionPool {
            session_pool: HashMap::new(),
            last_cleanup_time: Utc::now().timestamp(),
        }
    }

    /// clean up blocked/stop sessions
    pub fn cleanup_session(&mut self) -> bool {
        self.session_pool.retain(|_, v| !v.check_session_running());
        self.last_cleanup_time = Utc::now().timestamp();
        return true;
    }

    pub fn need_cleanup(&self) -> bool {
        (Utc::now().timestamp() - self.last_cleanup_time) >= DEFAULT_CLEANUP_SESSION_PERIOD
    }

    /// add brand new session into the pool
    /// clean up the pool when the pool size exceed half
    pub fn create_new_session(
        &mut self,
        sid: i32,
        token: &String,
    ) -> Result<(String, QuerySessionInfo), String> {
        if self.need_cleanup() {
            self.cleanup_session();
        }

        if self.session_pool.len() >= DEFAULT_SESSION_POOL_SIZE_LIMIT {
            return Err(format!(
                "Fail to create new session since session pool size exceed limit {}",
                DEFAULT_SESSION_POOL_SIZE_LIMIT
            ));
        }

        let sess = SessionManager::create_session(sid);
        self.session_pool.insert(token.clone(), sess.clone());
        return Ok((token.clone(), sess.session_info));
    }
    pub fn insert_session_with_token(
        &mut self,
        session_info: &QuerySessionInfo,
        token: &String,
    ) -> Result<String, String> {
        if self.session_pool.contains_key(token) {
            Err(format!("Fail to create session. Token already exist."))
        } else {
            self.session_pool.insert(
                token.clone(),
                SessionManager {
                    session_info: session_info.clone(),
                },
            );
            Ok(token.clone())
        }
    }
    pub fn remove_session(&mut self, token: &String) -> Result<SessionManager, String> {
        match self.session_pool.remove(token) {
            Some(session) => Ok(session),
            None => Err(format!("session {} not exist in session pool", token)),
        }
    }

    pub fn get_session(&self, token: &String) -> Option<&SessionManager> {
        self.session_pool.get(token)
    }
    pub fn get_session_mut(&mut self, token: &String) -> Option<&mut SessionManager> {
        self.session_pool.get_mut(token)
    }

    pub fn get_pool_size(&self) -> usize {
        self.session_pool.len()
    }
}

pub struct SessionStore {
    session_pools: HashMap<Address, SessionPool>,
    token_account_map: HashMap<String, Address>,
    sid: i32,
}

impl SessionStore {
    pub fn new() -> Self {
        SessionStore {
            session_pools: HashMap::new(),
            token_account_map: HashMap::new(),
            sid: 0,
        }
    }
    fn gen_token(&self) -> String {
        Uuid::new_v4().to_string()
    }

    fn generate_unique_token(&self) -> Result<String, String> {
        for _ in 0..GEN_TOKEN_RETRY {
            let token = self.gen_token();
            if !self.token_account_map.contains_key(&token) {
                return Ok(token.clone());
            }
        }
        Err(format!("Fail to generate unique token after retry"))
    }

    /// Add session into pool
    pub fn add_new_session(&mut self, addr: Address) -> Result<(String, QuerySessionInfo), String> {
        self.sid += 1;
        let token = self.generate_unique_token().map_err(|e| e)?;
        match self.session_pools.get_mut(&addr) {
            Some(sess_pool) => {
                self.token_account_map.insert(token.clone(), addr);
                sess_pool.create_new_session(self.sid, &token)
            }
            None => {
                let mut sess_pool = SessionPool::new();
                let res = sess_pool.create_new_session(self.sid, &token);
                if res.is_ok() {
                    self.token_account_map.insert(token.clone(), addr);
                    self.session_pools.insert(addr, sess_pool);
                }
                res
            }
        }
    }

    /// remove session with given token
    /// 1. verify token exsit
    /// 2. verify session exist with given (token, addr)
    pub fn remove_session(&mut self, token: &String) -> Result<SessionManager, String> {
        match self.token_account_map.remove(token) {
            Some(addr) => match self.session_pools.get_mut(&addr) {
                Some(sess_pool) => sess_pool.remove_session(token),
                None => Err(format!("Fail to remove session. Address not exist")),
            },
            None => Err(format!("Fail to remove session, token not exist {}", token)),
        }
    }
    pub fn is_session_exist(&self, token: &String) -> bool {
        match self.token_account_map.get(token) {
            Some(addr) => match self.session_pools.get(&addr) {
                Some(sess_pool) => sess_pool.session_pool.contains_key(token),
                None => false,
            },
            None => false,
        }
    }
    pub fn get_address(&self, token: &String) -> Option<Address> {
        match self.token_account_map.get(token).clone() {
            Some(addr) => Some(addr.clone()),
            None => None,
        }
    }
    pub fn get_session_mut(&mut self, token: &String) -> Option<&mut SessionManager> {
        match self.token_account_map.get(token) {
            Some(addr) => match self.session_pools.get_mut(&addr) {
                Some(sess_pool) => sess_pool.session_pool.get_mut(token),
                None => None,
            },
            None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SessionManager {
    session_info: QuerySessionInfo,
}

impl SessionManager {
    pub fn new() -> Self {
        Self::create_session(0)
    }
    pub fn create_session(id: i32) -> Self {
        let start_time = Utc::now().timestamp();
        SessionManager {
            session_info: QuerySessionInfo {
                id,
                start_time,
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
    pub fn increase_query(&mut self, count: i32) {
        self.session_info.query_count += count;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_base::get_address_from_pk;
    use db3_base::get_a_static_keypair;
    use db3_proto::db3_node_proto::SessionStatus;
    use fastcrypto::secp256k1::Secp256k1PublicKey;
    use fastcrypto::traits::ToFromBytes;
    use hex;

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
    fn add_session_exceed_limit() {
        let mut sess_store = SessionStore::new();
        let kp = get_a_static_keypair();
        let addr = get_address_from_pk(&kp.public);
        for _ in 0..DEFAULT_SESSION_POOL_SIZE_LIMIT {
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
        let kp = get_a_static_keypair();
        let addr = get_address_from_pk(&kp.public);
        let mut token1 = String::new();
        // add session and create new session pool
        {
            let res = sess_store.add_new_session(addr);
            assert!(res.is_ok());
            token1 = res.unwrap().0;
            assert_eq!(token1.len(), 36);
        }
        // add session into existing session pool
        let mut token2 = String::new();
        {
            let res = sess_store.add_new_session(addr);
            assert!(res.is_ok());
            token2 = res.unwrap().0;
            assert_ne!(token1, token2);
        }
        {
            let res = sess_store.get_session_mut(&token1);
            assert!(res.is_some());
            assert_eq!(res.unwrap().get_session_id(), 1);
        }
        {
            let res = sess_store.get_session_mut(&"token_unknow".to_string());
            assert!(res.is_none());
        }
    }

    #[test]
    fn remove_session_test() {
        let mut sess_store = SessionStore::new();
        let kp = get_a_static_keypair();
        let addr = get_address_from_pk(&kp.public);

        let mut token1 = String::new();
        // add session and create new session pool
        {
            let res = sess_store.add_new_session(addr);
            assert!(res.is_ok());
            token1 = res.unwrap().0;
            assert_eq!(token1.len(), 36);
        }
        // add session into existing session pool
        let mut token2 = String::new();
        {
            let res = sess_store.add_new_session(addr);
            assert!(res.is_ok());
            token2 = res.unwrap().0;
            assert_ne!(token1, token2);
        }
        {
            let res = sess_store.remove_session(&token2);
            assert!(res.is_ok());
            assert_eq!(2, res.unwrap().get_session_id());
        }
        {
            let res = sess_store.remove_session(&token2);
            assert!(res.is_err());
        }
    }

    #[test]
    fn cleanup_session_test() {
        let mut sess_store = SessionStore::new();
        let kp = get_a_static_keypair();
        let addr = get_address_from_pk(&kp.public);
        for i in 0..100 {
            let (token, _) = sess_store.add_new_session(addr).unwrap();

            // convert session with even id into blocked status
            if i % 2 == 0 {
                let session = sess_store.get_session_mut(&token).unwrap();
                session.increase_query(DEFAULT_SESSION_QUERY_LIMIT + 1);
                session.check_session_status();
                assert_eq!(SessionStatus::Blocked, session.check_session_status());
            }
        }
        // expect session pool len 100. 50 running, 50 blocked
        assert_eq!(
            sess_store.session_pools.get(&addr).unwrap().get_pool_size(),
            100
        );

        // Act: clean up session
        sess_store
            .session_pools
            .get_mut(&addr)
            .unwrap()
            .cleanup_session();

        assert_eq!(
            sess_store.session_pools.get(&addr).unwrap().get_pool_size(),
            50
        );
    }
}
