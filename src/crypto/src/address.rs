//
//
// address.rs
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

use ethereum_types::Address;
use fastcrypto::secp256k1::Secp256k1PublicKey;

pub fn get_address_from_pk(pk:&Secp256k1PublicKey)->Address {
    let public_key = pk.public_key.to_encoded_point(/* compress = */ false);
    let public_key = public_key.as_bytes();
    let hash = Keccak256::digest(&public_key[1..]);
    Address::from_slice(&hash[12..])
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn it_works() {
	}
}
