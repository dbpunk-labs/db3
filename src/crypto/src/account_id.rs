//
// account_id.rs
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

use super::address;
use ethereum_types::Address;
use rust_secp256k1::PublicKey;

// it's ethereum compatiable account id
pub struct AccountId {
    pub addr: Address,
    pub pk: PublicKey,
}

impl AccountId {
    pub fn new(pk: PublicKey) -> Self {
        let addr = address::get_address_from_pk(&pk);
        Self { addr, pk }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {}
}
