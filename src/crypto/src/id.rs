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
use byteorder::{BigEndian, WriteBytesExt};
use db3_error::DB3Error;
use fastcrypto::hash::{HashFunction, Sha3_256};
use rust_secp256k1::hashes::{sha256, Hash};
use rust_secp256k1::ThirtyTwoByteHash;
use std::{fmt, mem};

// it's ethereum compatiable account id
#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Copy, Clone)]
pub struct AccountId {
    pub addr: DB3Address,
}

impl AccountId {
    pub fn new(addr: DB3Address) -> Self {
        Self { addr }
    }
    #[inline]
    pub fn to_hex(&self) -> String {
        format!("0x{}", hex::encode(self.addr.as_ref()))
    }
}

impl TryFrom<&[u8]> for AccountId {
    type Error = DB3Error;
    fn try_from(data: &[u8]) -> std::result::Result<Self, DB3Error> {
        Ok(Self {
            addr: DB3Address::try_from(data)?,
        })
    }
}

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
    pub fn try_from_base64(input: &str) -> std::result::Result<Self, DB3Error> {
        Self::try_from_bytes(base64ct::Base64::decode_vec(input).unwrap().as_slice())
    }

    pub fn try_from_bytes(data: &[u8]) -> std::result::Result<Self, DB3Error> {
        let arr: [u8; TX_ID_LENGTH] = data.try_into().map_err(|_| DB3Error::InvalidAddress)?;
        Ok(Self { data: arr })
    }
}

impl From<&[u8]> for TxId {
    fn from(message: &[u8]) -> Self {
        let id = sha256::Hash::hash(message);
        Self { data: id.into_32() }
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
pub const TYPE_ID_LENGTH: usize = 1;
pub const BLOCK_ID_LENGTH: usize = 8;
pub const MUTATION_ID_LENGTH: usize = 4;
pub const OP_ENTRY_INDEX_LENGTH: usize = 4;
/// OpEntryId := BlockId + MutationId + OpEntryIdx
pub const OP_ENTRY_ID_LENGTH: usize = 16;

pub const DocumentIdTypeId: i8 = 1;
pub const IndexIdTypeId: i8 = 2;

#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Copy, Clone, Debug)]
pub struct OpEntryId {
    data: [u8; OP_ENTRY_ID_LENGTH],
}
impl OpEntryId {
    pub fn create(
        block_id: u64,
        mutation_id: u32,
        op_entry_idx: u32,
    ) -> std::result::Result<Self, DB3Error> {
        let mut bytes: Vec<u8> = Vec::with_capacity(OP_ENTRY_ID_LENGTH);
        bytes.extend(block_id.to_be_bytes());
        bytes.extend(mutation_id.to_be_bytes());
        bytes.extend(op_entry_idx.to_be_bytes());
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
    fn get_as_int(&self) -> u128 {
        unsafe { mem::transmute::<[u8; 16], u128>(self.data) }
    }

    pub fn try_from_bytes(data: &[u8]) -> std::result::Result<Self, DB3Error> {
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
        write!(f, "{}", self.get_as_int())
    }
}
impl AsRef<[u8]> for OpEntryId {
    fn as_ref(&self) -> &[u8] {
        &self.data[..]
    }
}

pub type DocumentEntryId = OpEntryId;

pub type CollectionId = OpEntryId;

/// DocumentId := CollectionId + DocumentId
pub const DOCUMENT_ID_LENGTH: usize = TYPE_ID_LENGTH + OP_ENTRY_ID_LENGTH + OP_ENTRY_ID_LENGTH;
#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Debug)]
pub struct DocumentId {
    data: [u8; DOCUMENT_ID_LENGTH],
}

impl DocumentId {
    pub fn create(
        collection_id: &CollectionId,
        document_entry_id: &DocumentEntryId,
    ) -> std::result::Result<Self, DB3Error> {
        let mut bytes: Vec<u8> = Vec::with_capacity(DOCUMENT_ID_LENGTH);
        bytes.extend(DocumentIdTypeId.to_be_bytes());
        bytes.extend(collection_id.as_ref());
        bytes.extend(document_entry_id.as_ref());
        Self::try_from_bytes(bytes.as_slice())
    }

    /// collection id = document_id[OP_ENTRY_ID_LENGTH..]
    pub fn get_collection_id(&self) -> std::result::Result<DocumentEntryId, DB3Error> {
        CollectionId::try_from_bytes(
            self.data[DOCUMENT_ID_LENGTH..DOCUMENT_ID_LENGTH + OP_ENTRY_ID_LENGTH].as_ref(),
        )
    }

    /// document entry id = document_id[OP_ENTRY_ID_LENGTH..]
    pub fn get_document_entry_id(&self) -> std::result::Result<DocumentEntryId, DB3Error> {
        DocumentEntryId::try_from_bytes(
            self.data[DOCUMENT_ID_LENGTH + OP_ENTRY_ID_LENGTH..].as_ref(),
        )
    }

    pub fn try_from_bytes(data: &[u8]) -> std::result::Result<Self, DB3Error> {
        let buf: [u8; DOCUMENT_ID_LENGTH] = data
            .try_into()
            .map_err(|_| DB3Error::InvalidDocumentIdBytes)?;
        Ok(Self { data: buf })
    }
}

impl AsRef<[u8]> for DocumentId {
    fn as_ref(&self) -> &[u8] {
        &self.data[..]
    }
}

impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Customize so only `x` and `y` are denoted.
        let collection_id = self.get_collection_id().map_err(|e| e).unwrap();
        let document_entry_id = self.get_document_entry_id().map_err(|e| e).unwrap();
        write!(f, "{}|{}", collection_id, document_entry_id)
    }
}
/// DocumentId := CollectionId + IndexFieldId + KeyBytes + DocumentEntryId
#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Clone, Debug)]
pub struct IndexId {
    data: Vec<u8>,
}

impl IndexId {
    pub fn create(
        collection_id: &CollectionId,
        index_field_id: u32,
        key: &str,
        document_id: &DocumentId,
    ) -> std::result::Result<Self, DB3Error> {
        let mut data: Vec<u8> = Vec::new();
        data.extend(IndexIdTypeId.to_be_bytes());
        data.extend(collection_id.as_ref());
        data.extend(index_field_id.to_be_bytes());
        data.extend(key.as_bytes());
        data.extend(document_id.as_ref());
        Ok(Self { data })
    }

    pub fn get_document_id(&self) -> std::result::Result<DocumentEntryId, DB3Error> {
        DocumentEntryId::try_from_bytes(self.data[self.data.len() - DOCUMENT_ID_LENGTH..].as_ref())
    }
    pub fn get_collection_id(&self) -> std::result::Result<CollectionId, DB3Error> {
        CollectionId::try_from_bytes(
            self.data[TYPE_ID_LENGTH..TYPE_ID_LENGTH + OP_ENTRY_ID_LENGTH].as_ref(),
        )
    }
}
impl AsRef<Vec<u8>> for IndexId {
    fn as_ref(&self) -> &Vec<u8> {
        &self.data
    }
}
impl fmt::Display for IndexId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Customize so only `x` and `y` are denoted.
        let collection_id = self.get_collection_id().map_err(|e| e).unwrap();
        let document_id = self.get_document_id().map_err(|e| e).unwrap();
        write!(f, "{}|.todo..|{}", collection_id, document_id)
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

    #[test]
    fn tx_base64_encode_decode() {
        let tx_id = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=");
        assert!(tx_id.is_ok());
        assert_eq!(
            "iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=",
            tx_id.unwrap().to_base64()
        )
    }
    #[test]
    fn op_entry_create_ut() {
        let doc_id = OpEntryId::create(1000000, 1000, 100);
        assert_eq!(
            vec![0, 0, 0, 0, 0, 15, 66, 64, 0, 0, 3, 232, 0, 0, 0, 100],
            doc_id.unwrap().data.to_vec()
        );
    }
    #[test]
    fn test_ts_db_id_smoke() {
        let sender = DB3Address::try_from("0xed17b3f435c03ff69c2cdc6d394932e68375f20f").unwrap();
        let nonce: u64 = 10;
        let db_id = DbId::try_from((&sender, nonce)).unwrap();
        assert_eq!(
            db_id.to_hex().as_str(),
            "0xd74360cca976522a8b66c7cbd4f674fef9eeef97"
        );
    }
}