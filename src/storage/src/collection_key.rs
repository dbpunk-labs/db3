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
use db3_error::{DB3Error, Result};

const MAX_COLLECTION_NAME: usize = 20;

pub fn build_collection_key(db_addr: &DB3Address, name: &str) -> Result<Vec<u8>> {
    if name.len() > MAX_COLLECTION_NAME {
        return Err(DB3Error::InvalidCollectionNameError(
            "name exceeds the max lens limit".to_string(),
        ));
    }
    let mut buf = db_addr.as_ref().to_vec();
    buf.extend_from_slice(name.as_bytes());
    Ok(buf)
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {}
}
