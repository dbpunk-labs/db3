//
// id_v2.rs
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

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use db3_error::{DB3Error, Result};
use std::fmt;

pub const OP_ENTRY_ID_LENGTH: usize = 14;

#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Copy, Clone, Debug)]
pub struct OpEntryId {
    data: [u8; OP_ENTRY_ID_LENGTH],
}

impl OpEntryId {
    pub fn create(block_id: u64, order_id: u32, idx: u16) -> Result<Self> {
        let mut bytes: Vec<u8> = Vec::with_capacity(OP_ENTRY_ID_LENGTH);
        bytes
            .write_u64::<BigEndian>(block_id)
            .map_err(|_| DB3Error::InvalidOpEntryIdBytes)?;
        bytes
            .write_u32::<BigEndian>(order_id)
            .map_err(|_| DB3Error::InvalidOpEntryIdBytes)?;
        bytes
            .write_u16::<BigEndian>(idx)
            .map_err(|_| DB3Error::InvalidOpEntryIdBytes)?;
        Self::try_from_bytes(bytes.as_slice())
    }

    #[inline]
    pub fn zero() -> Self {
        Self {
            data: [0; OP_ENTRY_ID_LENGTH],
        }
    }
    #[inline]
    pub fn one() -> Self {
        Self {
            data: [1; OP_ENTRY_ID_LENGTH],
        }
    }

    fn get_block_id(&self) -> u64 {
        let block_id = (&self.data[0..]).read_u64::<BigEndian>().unwrap();
        block_id
    }

    fn get_order_id(&self) -> u32 {
        let order_id = (&self.data[8..]).read_u32::<BigEndian>().unwrap();
        order_id
    }

    fn get_idx(&self) -> u16 {
        let id = (&self.data[12..]).read_u16::<BigEndian>().unwrap();
        id
    }
    #[inline]
    pub fn to_hex(&self) -> String {
        format!("0x{}", hex::encode(self.as_ref()))
    }

    pub fn try_from_bytes(data: &[u8]) -> Result<Self> {
        let buf: [u8; OP_ENTRY_ID_LENGTH] = data
            .try_into()
            .map_err(|_| DB3Error::InvalidOpEntryIdBytes)?;
        Ok(Self { data: buf })
    }
}

/// Diplay OpEntryId = BlockId-MutationId-OpEntryId
impl fmt::Display for OpEntryId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Customize so only `x` and `y` are denoted.
        write!(
            f,
            "{}-{}-{}",
            self.get_block_id(),
            self.get_order_id(),
            self.get_idx()
        )
    }
}

impl AsRef<[u8]> for OpEntryId {
    fn as_ref(&self) -> &[u8] {
        &self.data[..]
    }
}
