//
// collection_key.rs
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
use db3_crypto::db3_address::DB3Address;
use db3_error::Result;
use std::fmt;
const DOC_PREFIX: &str = "/doc/";
/// DBDocKey with db address, doc id
pub struct DbDocKeyV2<'a>(pub &'a DB3Address, pub i64);
impl<'a> DbDocKeyV2<'a> {
    ///
    /// encode the database key
    ///
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded_key = DOC_PREFIX.as_bytes().to_vec();
        encoded_key.extend_from_slice(self.0.as_ref());
        encoded_key.extend_from_slice(self.1.to_be_bytes().as_ref());
        Ok(encoded_key)
    }
}

impl fmt::Display for DbDocKeyV2<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}/{}", DOC_PREFIX, self.0.to_hex(), self.1,)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use db3_crypto::db3_address::DB3_ADDRESS_LENGTH;
    #[test]
    fn it_works() {}

    #[test]
    fn test_db_doc_key_v2() {
        let db_addr = DB3Address::ZERO;
        let doc_id = 1;
        let db_doc_key = DbDocKeyV2(&db_addr, doc_id);
        let encoded_key = db_doc_key.encode().unwrap();
        assert_eq!(encoded_key.len(), DOC_PREFIX.len() + DB3_ADDRESS_LENGTH + 8);
        assert_eq!(
            "/doc//0x0000000000000000000000000000000000000000/1",
            db_doc_key.to_string()
        );
    }
}
