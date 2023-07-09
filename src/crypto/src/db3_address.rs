//
// db3_address.rs
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

use db3_error::DB3Error;
use fastcrypto::encoding::{decode_bytes_hex, Encoding, Hex};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

pub const DB3_ADDRESS_LENGTH: usize = 20;
#[serde_as]
#[derive(
    Eq, Default, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct DB3Address([u8; DB3_ADDRESS_LENGTH]);

impl DB3Address {
    pub const ZERO: Self = Self([0u8; DB3_ADDRESS_LENGTH]);
    pub const ONE: Self = Self([1u8; DB3_ADDRESS_LENGTH]);

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    pub fn optional_address_as_hex<S>(
        key: &Option<DB3Address>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(&key.map(Hex::encode).unwrap_or_default())
    }

    pub fn optional_address_from_hex<'de, D>(
        deserializer: D,
    ) -> Result<Option<DB3Address>, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = decode_bytes_hex(&s).map_err(serde::de::Error::custom)?;
        Ok(Some(value))
    }

    pub fn to_inner(self) -> [u8; DB3_ADDRESS_LENGTH] {
        self.0
    }

    #[inline]
    pub fn to_hex(&self) -> String {
        format!("0x{}", hex::encode(self.0.as_ref()))
    }
    #[inline]
    pub fn from_hex(input: &str) -> Result<Self, DB3Error> {
        if input.starts_with("0x") {
            let new_input = &input[2..];
            let data = hex::decode(new_input)
                .map_err(|e| DB3Error::KeyCodecError(format!("fail to decode tx id for {e}")))?;
            Self::try_from(data.as_slice())
        } else {
            let data = hex::decode(input)
                .map_err(|e| DB3Error::KeyCodecError(format!("fail to decode tx id for {e}")))?;
            Self::try_from(data.as_slice())
        }
    }
}

impl TryFrom<Vec<u8>> for DB3Address {
    type Error = DB3Error;
    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let arr: [u8; DB3_ADDRESS_LENGTH] =
            bytes.try_into().map_err(|_| DB3Error::InvalidAddress)?;
        Ok(Self(arr))
    }
}

impl From<&[u8; DB3_ADDRESS_LENGTH]> for DB3Address {
    fn from(data: &[u8; DB3_ADDRESS_LENGTH]) -> Self {
        Self(*data)
    }
}

impl TryFrom<&[u8]> for DB3Address {
    type Error = DB3Error;

    fn try_from(bytes: &[u8]) -> std::result::Result<Self, DB3Error> {
        let arr: [u8; DB3_ADDRESS_LENGTH] =
            bytes.try_into().map_err(|_| DB3Error::InvalidAddress)?;
        Ok(Self(arr))
    }
}

impl TryFrom<&str> for DB3Address {
    type Error = DB3Error;
    fn try_from(addr: &str) -> std::result::Result<Self, DB3Error> {
        let value = decode_bytes_hex(addr).map_err(|_| DB3Error::InvalidAddress)?;
        Ok(Self(value))
    }
}

impl AsRef<[u8]> for DB3Address {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
