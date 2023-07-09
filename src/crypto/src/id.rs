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
use base64ct::Encoding as _;
use db3_error::DB3Error;
use fastcrypto::hash::{HashFunction, Sha3_256};

pub const TX_ID_LENGTH: usize = 32;
#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub struct TxId {
    data: [u8; TX_ID_LENGTH],
}

impl TxId {
    #[inline]
    pub fn zero() -> Self {
        Self {
            data: [0; TX_ID_LENGTH],
        }
    }

    pub fn to_base64(&self) -> String {
        base64ct::Base64::encode_string(self.as_ref())
    }

    #[inline]
    pub fn to_hex(&self) -> String {
        format!("0x{}", hex::encode(self.data.as_ref()))
    }

    pub fn try_from_base64(input: &str) -> std::result::Result<Self, DB3Error> {
        Self::try_from_bytes(base64ct::Base64::decode_vec(input).unwrap().as_slice())
    }

    pub fn try_from_hex(input: &str) -> std::result::Result<Self, DB3Error> {
        if input.starts_with("0x") {
            let new_input = &input[2..];
            let data = hex::decode(new_input)
                .map_err(|e| DB3Error::KeyCodecError(format!("fail to decode tx id for {e}")))?;
            Self::try_from_bytes(data.as_slice())
        } else {
            let data = hex::decode(input)
                .map_err(|e| DB3Error::KeyCodecError(format!("fail to decode tx id for {e}")))?;
            Self::try_from_bytes(data.as_slice())
        }
    }

    pub fn try_from_bytes(data: &[u8]) -> std::result::Result<Self, DB3Error> {
        let arr: [u8; TX_ID_LENGTH] = data.try_into().map_err(|_| DB3Error::InvalidAddress)?;
        Ok(Self { data: arr })
    }
}

impl From<&[u8]> for TxId {
    fn from(message: &[u8]) -> Self {
        let mut hasher = Sha3_256::default();
        hasher.update(message);
        let id = hasher.finalize();
        Self { data: id.into() }
    }
}

impl From<(&[u8], &[u8])> for TxId {
    fn from(pair: (&[u8], &[u8])) -> Self {
        let mut hasher = Sha3_256::default();
        hasher.update(pair.0);
        hasher.update(pair.1);
        let hash = hasher.finalize();
        Self { data: hash.into() }
    }
}

impl From<[u8; TX_ID_LENGTH]> for TxId {
    fn from(data: [u8; TX_ID_LENGTH]) -> Self {
        Self { data }
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

    #[inline]
    pub fn to_hex(&self) -> String {
        format!("0x{}", hex::encode(self.addr.as_ref()))
    }

    #[inline]
    pub fn address(&self) -> &DB3Address {
        &self.addr
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

impl TryFrom<&str> for DbId {
    type Error = DB3Error;
    fn try_from(addr: &str) -> std::result::Result<Self, DB3Error> {
        Ok(Self {
            addr: DB3Address::try_from(addr)?,
        })
    }
}

impl TryFrom<&[u8]> for DbId {
    type Error = DB3Error;
    fn try_from(data: &[u8]) -> std::result::Result<Self, DB3Error> {
        Ok(Self {
            addr: DB3Address::try_from(data)?,
        })
    }
}

impl From<DB3Address> for DbId {
    fn from(addr: DB3Address) -> Self {
        Self { addr }
    }
}

impl From<(&DB3Address, u64, u64)> for DbId {
    fn from(input: (&DB3Address, u64, u64)) -> Self {
        let mut hasher = Sha3_256::default();
        hasher.update(input.1.to_be_bytes());
        hasher.update(input.2.to_be_bytes());
        hasher.update(input.0);
        let g_arr = hasher.finalize();
        let mut res = [0u8; DB3_ADDRESS_LENGTH];
        res.copy_from_slice(&AsRef::<[u8]>::as_ref(&g_arr)[..DB3_ADDRESS_LENGTH]);
        Self {
            addr: DB3Address::from(&res),
        }
    }
}
