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

#[derive(Debug, PartialEq)]
pub enum SessionStatus {
    READY,
    RUNNING,
    BLOCKED,
}
#[derive(Debug)]
pub struct SessionManager {
    id: i32,
    start_time: i64,
    query_count: i32,
    status: SessionStatus
}
// default session timeout 60s
pub const DEFAULT_SESSION_PERIOD : i64 = 60000;
// default session limit
pub const DEFAULT_SESSION_QUERY_LIMIT: i32 = 100;

impl SessionManager {
    pub fn new() -> Self {
        Self::create_session(0)
    }
    pub fn create_session(id: i32) -> Self {
        SessionManager {
            id: id,
            start_time: Utc::now().timestamp(),
            query_count: 0,
            status: SessionStatus::READY,
        }
    }
    pub fn get_session_id(&self) -> i32 {
        self.id
    }
    pub fn get_session_query_count(&self) -> i32 {
        self.query_count
    }
    pub fn get_session_status(&self) -> &SessionStatus {
        &self.status
    }
    pub fn check_session_status(&mut self) -> &SessionStatus {
        match self.status {
            SessionStatus::READY => {
                self.status = SessionStatus::RUNNING;
                self.start_time = Utc::now().timestamp();
            }
            SessionStatus::RUNNING => {
                if Utc::now().timestamp() - self.start_time > DEFAULT_SESSION_PERIOD {
                    self.status = SessionStatus::BLOCKED;
                } else if self.query_count >= DEFAULT_SESSION_QUERY_LIMIT {
                    self.status = SessionStatus::BLOCKED;
                }
            }
            SessionStatus::BLOCKED => {
            }
        }
        &self.status
    }
    pub fn reset_session(&mut self) {
        self.query_count = 0;
        self.status = SessionStatus::READY;
        self.id += 1;
    }
    pub fn increate_query(&mut self, count: i32) {
        self.query_count += count;
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_session() {
        let session = SessionManager::new();
        assert_eq!(SessionStatus::READY, session.status);
    }

    #[test]
    fn update_session_status_happy_path() {
        let mut session = SessionManager::new();
        session.check_session_status();
        assert_eq!(SessionStatus::RUNNING, session.status);
    }
}