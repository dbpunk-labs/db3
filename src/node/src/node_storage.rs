use crate::auth_storage::AuthStorage;
use db3_session::session_manager::SessionStore;

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
}
