//
// id.rs
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

use crate::db3_address::{DB3Address, DB3_ADDRESS_LENGTH};
use byteorder::{BigEndian, WriteBytesExt};
use db3_error::DB3Error;
use fastcrypto::hash::{HashFunction, Sha3_256};
use rust_secp256k1::hashes::{sha256, Hash};
use rust_secp256k1::ThirtyTwoByteHash;
// it's ethereum compatiable account id
#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub struct AccountId {
    pub addr: DB3Address,
}

impl AccountId {
    pub fn new(addr: DB3Address) -> Self {
        Self { addr }
    }
}

#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub struct TxId {
    data: [u8; 32],
}

impl From<&[u8]> for TxId {
    fn from(message: &[u8]) -> Self {
        let id = sha256::Hash::hash(message);
        Self { data: id.into_32() }
    }
}

impl AsRef<[u8]> for TxId {
    fn as_ref(&self) -> &[u8] {
        &self.data[..]
    }
}

pub const DBID_LENGTH: usize = DB3_ADDRESS_LENGTH;

#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub struct DbId {
    addr: DB3Address,
}

impl DbId {
    #[inline]
    pub fn length() -> usize {
        DBID_LENGTH
    }

    #[inline]
    pub fn min_id() -> DbId {
        DbId::from(&[std::u8::MIN; DB3_ADDRESS_LENGTH])
    }

    #[inline]
    pub fn max_id() -> DbId {
        DbId::from(&[std::u8::MAX; DB3_ADDRESS_LENGTH])
    }
}

impl AsRef<[u8]> for DbId {
    fn as_ref(&self) -> &[u8] {
        self.addr.as_ref()
    }
}

impl From<&[u8; DB3_ADDRESS_LENGTH]> for DbId {
    fn from(data: &[u8; DB3_ADDRESS_LENGTH]) -> Self {
        Self {
            addr: DB3Address::from(data),
        }
    }
}

impl From<DB3Address> for DbId {
    fn from(addr: DB3Address) -> Self {
        Self { addr }
    }
}

impl TryFrom<(&DB3Address, u64)> for DbId {
    type Error = DB3Error;
    fn try_from(input: (&DB3Address, u64)) -> std::result::Result<Self, DB3Error> {
        let mut bs = [0u8; std::mem::size_of::<u64>()];
        bs.as_mut()
            .write_u64::<BigEndian>(input.1)
            .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
        let mut hasher = Sha3_256::default();
        hasher.update(bs.as_ref());
        hasher.update(input.0);
        let g_arr = hasher.finalize();
        let mut res = [0u8; DB3_ADDRESS_LENGTH];
        res.copy_from_slice(&AsRef::<[u8]>::as_ref(&g_arr)[..DB3_ADDRESS_LENGTH]);
        Ok(Self {
            addr: DB3Address::from(&res),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
