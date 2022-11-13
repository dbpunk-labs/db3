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
#[derive(Debug)]
pub struct SessionManager {
    session_info: QuerySessionInfo
}
// default session timeout 1hrs
pub const DEFAULT_SESSION_PERIOD: i64 = 3600;
// default session limit
pub const DEFAULT_SESSION_QUERY_LIMIT: i32 = 1000;

impl SessionManager {
    pub fn new() -> Self {
        Self::create_session(0)
    }
    pub fn create_session(id: i32) -> Self {
        SessionManager {
            session_info : QuerySessionInfo {
                id,
                start_time: Utc::now().timestamp(),
                query_count: 0,
                status: SessionStatus::Running.into(),
            }
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
            Some(SessionStatus::Blocked) => {}
            None => {}
        }
        SessionStatus::from_i32(self.session_info.status).unwrap()
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
    use db3_proto::db3_node_proto::SessionStatus;
    use std::{thread, time};
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
}
