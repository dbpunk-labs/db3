use crate::auth_storage::{AuthStorage, NetworkState};
use db3_session::session_manager::SessionStore;
use std::sync::Arc;

pub struct NodeStorage {
    auth_store: AuthStorage,
    session_store: SessionStore,
}

impl NodeStorage {
    pub fn new(auth_store: AuthStorage) -> Self {
        Self {
            auth_store,
            session_store: SessionStore::new(),
        }
    }
    pub fn get_auth_store(&mut self) -> &mut AuthStorage {
        &mut self.auth_store
    }
    pub fn get_session_store(&mut self) -> &mut SessionStore {
        &mut self.session_store
    }

    pub fn get_state(&self) -> Arc<NetworkState> {
        self.auth_store.get_state()
    }
}
