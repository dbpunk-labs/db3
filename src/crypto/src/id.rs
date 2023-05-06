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
use bson::Bson;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Buf;
use db3_error::{DB3Error, Result};
use fastcrypto::hash::{HashFunction, Sha3_256};
use rust_secp256k1::hashes::{sha256, Hash};
use rust_secp256k1::ThirtyTwoByteHash;
use std::fmt;
use std::io::Cursor;

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

pub const BILL_ID_LENGTH: usize = 10;
#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Copy, Clone, Debug)]
pub struct BillId {
    data: [u8; BILL_ID_LENGTH],
}

impl BillId {
    pub fn new(block_id: u64, mutation_id: u16) -> Result<Self> {
        let mut data: Vec<u8> = Vec::with_capacity(BILL_ID_LENGTH);
        data.write_u64::<BigEndian>(block_id)
            .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
        data.write_u16::<BigEndian>(mutation_id)
            .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
        //TODO avoid to copy data
        let data_array: [u8; BILL_ID_LENGTH] = data
            .try_into()
            .map_err(|_| DB3Error::KeyCodecError("invalid array length".to_string()))?;
        Ok(BillId { data: data_array })
    }

    pub fn to_base64(&self) -> String {
        base64ct::Base64::encode_string(self.data.as_ref())
    }

    pub fn get_block_range(block_id: u64) -> Result<(BillId, BillId)> {
        let start = BillId::new(block_id, 0)?;
        let end = BillId::new(block_id, std::u16::MAX)?;
        Ok((start, end))
    }
}

impl AsRef<[u8]> for BillId {
    fn as_ref(&self) -> &[u8] {
        &self.data[..]
    }
}

impl TryFrom<&[u8]> for BillId {
    type Error = DB3Error;
    fn try_from(data: &[u8]) -> Result<Self> {
        let data_array: [u8; BILL_ID_LENGTH] = data
            .try_into()
            .map_err(|_| DB3Error::KeyCodecError("array length is invalid".to_string()))?;
        Ok(BillId { data: data_array })
    }
}

pub const TYPE_ID_LENGTH: usize = 1;
pub const BLOCK_ID_LENGTH: usize = 8;
pub const MUTATION_ID_LENGTH: usize = 2;
pub const OP_ENTRY_INDEX_LENGTH: usize = 2;
pub const INDEX_FIELD_ID_LENGTH: usize = 4;
/// OpEntryId := BlockId + MutationId + OpEntryIdx
pub const OP_ENTRY_ID_LENGTH: usize = 12;
pub const DOCUMENT_ID_TYPE_ID: i8 = 1;
pub const INDEX_ID_TYPE_ID: i8 = 2;

/// FieldTypeId := 1 bytes
pub const FIELD_TYPE_ID_LENGTH: usize = 1;

#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Copy, Clone, Debug)]
pub struct OpEntryId {
    data: [u8; OP_ENTRY_ID_LENGTH],
}

impl OpEntryId {
    pub fn create(
        block_id: u64,
        mutation_id: u16,
        op_entry_idx: u16,
    ) -> std::result::Result<Self, DB3Error> {
        let mut bytes: Vec<u8> = Vec::with_capacity(OP_ENTRY_ID_LENGTH);
        bytes
            .write_u64::<BigEndian>(block_id)
            .map_err(|_| DB3Error::InvalidOpEntryIdBytes)?;
        bytes
            .write_u16::<BigEndian>(mutation_id)
            .map_err(|_| DB3Error::InvalidOpEntryIdBytes)?;
        bytes
            .write_u16::<BigEndian>(op_entry_idx)
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

    fn get_mutation_id(&self) -> u16 {
        let mutation_id = (&self.data[8..]).read_u16::<BigEndian>().unwrap();
        mutation_id
    }

    fn get_op_entry_ixd(&self) -> u16 {
        let id = (&self.data[10..]).read_u16::<BigEndian>().unwrap();
        id
    }

    pub fn to_base64(&self) -> String {
        base64ct::Base64::encode_string(self.as_ref())
    }
    pub fn try_from_base64(input: &str) -> std::result::Result<Self, DB3Error> {
        Self::try_from_bytes(base64ct::Base64::decode_vec(input).unwrap().as_slice())
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
        write!(
            f,
            "{}-{}-{}",
            self.get_block_id(),
            self.get_mutation_id(),
            self.get_op_entry_ixd()
        )
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
        bytes.extend(DOCUMENT_ID_TYPE_ID.to_be_bytes());
        bytes.extend(collection_id.as_ref());
        bytes.extend(document_entry_id.as_ref());
        Self::try_from_bytes(bytes.as_slice())
    }
    #[inline]
    pub fn zero() -> Self {
        Self {
            data: [0; DOCUMENT_ID_LENGTH],
        }
    }
    #[inline]
    pub fn one() -> Self {
        Self {
            data: [1; DOCUMENT_ID_LENGTH],
        }
    }

    /// collection id = document_id[OP_ENTRY_ID_LENGTH..]
    pub fn get_collection_id(&self) -> std::result::Result<CollectionId, DB3Error> {
        CollectionId::try_from_bytes(
            self.data[TYPE_ID_LENGTH..TYPE_ID_LENGTH + OP_ENTRY_ID_LENGTH].as_ref(),
        )
    }

    /// document entry id = document_id[OP_ENTRY_ID_LENGTH..]
    pub fn get_document_entry_id(&self) -> std::result::Result<DocumentEntryId, DB3Error> {
        DocumentEntryId::try_from_bytes(self.data[TYPE_ID_LENGTH + OP_ENTRY_ID_LENGTH..].as_ref())
    }

    pub fn try_from_bytes(data: &[u8]) -> std::result::Result<Self, DB3Error> {
        let buf: [u8; DOCUMENT_ID_LENGTH] = data
            .try_into()
            .map_err(|_| DB3Error::InvalidDocumentIdBytes)?;
        Ok(Self { data: buf })
    }

    pub fn to_base64(&self) -> String {
        base64ct::Base64::encode_string(self.as_ref())
    }

    pub fn try_from_base64(input: &str) -> std::result::Result<Self, DB3Error> {
        let b64_vec =
            base64ct::Base64::decode_vec(input).map_err(|_| DB3Error::InvalidDocumentIdBytes)?;
        Self::try_from_bytes(b64_vec.as_slice())
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
        write!(f, "DOC|{}|{}", collection_id, document_entry_id)
    }
}
pub enum FieldTypeId {
    Null = 0x01,
    Bool = 0x02,
    I32 = 0x03,
    I64 = 0x04,
    F32 = 0x05,
    F64 = 0x06,
    DateTime = 0x07,
    String = 0x0a,
}
#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Clone, Debug)]
pub struct FieldKey {
    data: Vec<u8>,
}
impl FieldKey {
    fn encode_i32(input: i32) -> u32 {
        match input < 0 {
            true => {
                if input == i32::MIN {
                    0
                } else {
                    let new_input = input as u32;
                    (new_input & 0x7fffffff) as u32
                }
            }
            false => {
                let new_input = input as u32;
                (new_input | 0x80000000) as u32
            }
        }
    }
    fn decode_u32(input: u32) -> i32 {
        if input == 0 {
            i32::MIN
        } else {
            if input as i32 > 0 {
                (input | 0x80000000) as i32
            } else {
                (input & 0x7fffffff) as i32
            }
        }
    }

    fn encode_i64(input: i64) -> u64 {
        match input < 0 {
            true => {
                if input == i64::MIN {
                    0
                } else {
                    let new_input = input as u64;
                    (new_input & 0x7fffffffffffffff) as u64
                }
            }
            false => {
                let new_input = input as u64;
                (new_input | 0x8000000000000000) as u64
            }
        }
    }
    fn decode_u64(input: u64) -> i64 {
        if input == 0 {
            i64::MIN
        } else {
            if input as i64 > 0 {
                (input | 0x8000000000000000) as i64
            } else {
                (input & 0x7fffffffffffffff) as i64
            }
        }
    }

    fn add(&mut self, field: &Option<Bson>) -> std::result::Result<(), DB3Error> {
        match field {
            None => {
                self.data
                    .write_u8(FieldTypeId::Null as u8)
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
            }

            Some(Bson::Boolean(b)) => {
                self.data
                    .write_u8(FieldTypeId::Bool as u8)
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
                self.data
                    .write_u8(*b as u8)
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
            }
            Some(Bson::Int64(n)) => {
                self.data
                    .write_u8(FieldTypeId::I64 as u8)
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
                self.data
                    .write_u64::<BigEndian>(Self::encode_i64(*n))
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
            }
            Some(Bson::Int32(n)) => {
                self.data
                    .write_u8(FieldTypeId::I32 as u8)
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
                self.data
                    .write_u32::<BigEndian>(Self::encode_i32(*n))
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
            }
            // TODO: add \0 as the end of string.
            Some(Bson::String(s)) => {
                self.data
                    .write_u8(FieldTypeId::String as u8)
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
                self.data.extend_from_slice(s.as_bytes());
                self.data.extend_from_slice(&[0]);
            }
            Some(Bson::DateTime(dt)) => {
                self.data
                    .write_u8(FieldTypeId::DateTime as u8)
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
                let value: u64 = Self::encode_i64(dt.timestamp_millis());
                self.data
                    .write_u64::<BigEndian>(value)
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
            }
            _ => {
                return Err(DB3Error::DocumentDecodeError(
                    "value type is not supported".to_string(),
                ))
            }
        }
        Ok(())
    }

    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn create(fields: &Vec<Option<Bson>>) -> std::result::Result<Self, DB3Error> {
        let mut key = Self::new();
        if fields.len() > 16 {
            return Err(DB3Error::DocumentDecodeError(format!(
                "field length is over 16"
            )));
        }
        for field in fields {
            key.add(&field)?;
        }
        Ok(key)
    }
    pub fn create_single_key(field: Option<Bson>) -> std::result::Result<Self, DB3Error> {
        Self::create(&vec![field])
    }
    fn read_next_field(
        cursor: &mut Cursor<Vec<u8>>,
    ) -> std::result::Result<Option<Bson>, DB3Error> {
        let field_type = cursor
            .read_u8()
            .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
        match field_type {
            0x01 => Ok(None),
            0x02 => {
                let value = cursor
                    .read_u8()
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
                Ok(Some(Bson::Boolean(value != 0)))
            }
            0x03 => {
                let value = cursor
                    .read_u32::<BigEndian>()
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
                Ok(Some(Bson::Int32(Self::decode_u32(value))))
            }
            0x04 => {
                let value = cursor
                    .read_u64::<BigEndian>()
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
                Ok(Some(Bson::Int64(Self::decode_u64(value))))
            }
            0x07 => {
                let value = cursor
                    .read_u64::<BigEndian>()
                    .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
                Ok(Some(Bson::DateTime(bson::DateTime::from_millis(
                    Self::decode_u64(value),
                ))))
            }
            0x0a => {
                let mut buf = vec![];
                while cursor.has_remaining() {
                    let c = cursor
                        .read_u8()
                        .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
                    if c == 0 {
                        let value = String::from_utf8(buf)
                            .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
                        return Ok(Some(Bson::String(value)));
                    }
                    buf.push(c);
                }

                Err(DB3Error::DocumentDecodeError(
                    "string field is not terminated by \0".to_string(),
                ))
            }
            _ => Err(DB3Error::DocumentDecodeError(
                "field type is not supported".to_string(),
            )),
        }
    }
    /// extract_fields extract fields from a key.
    pub fn extract_fields(&self) -> std::result::Result<Vec<Option<Bson>>, DB3Error> {
        if self.data.len() == 0 {
            return Ok(Vec::new());
        }
        let mut fields = Vec::new();
        let mut cursor = Cursor::new(self.data.clone());
        while cursor.has_remaining() {
            match Self::read_next_field(&mut cursor) {
                Ok(field) => {
                    fields.push(field);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(fields)
    }
    pub fn try_from_bytes(data: &[u8]) -> std::result::Result<Self, DB3Error> {
        Ok(Self {
            data: data.to_vec(),
        })
    }
}

impl AsRef<Vec<u8>> for FieldKey {
    fn as_ref(&self) -> &Vec<u8> {
        &self.data
    }
}
impl fmt::Display for FieldKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Customize so only `x` and `y` are denoted.
        match self.extract_fields() {
            Ok(fields) => {
                let key_string = fields
                    .iter()
                    .map(|f| match f {
                        Some(Bson::String(s)) => format!("{}", s).to_string(),
                        Some(Bson::Int32(i)) => format!("{}", i).to_string(),
                        Some(Bson::Int64(i)) => format!("{}", i).to_string(),
                        Some(Bson::Boolean(b)) => format!("{}", b).to_string(),
                        Some(Bson::DateTime(d)) => format!("{}", d).to_string(),
                        None => "null".to_string(),
                        _ => "NA".to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join("#")
                    .to_string();
                write!(f, "{}", key_string)
            }
            Err(_) => write!(f, "NA"),
        }
    }
}
/// DocumentId := CollectionId + IndexFieldId + KeyBytes + DocumentEntryId
#[derive(Eq, Default, PartialEq, Ord, PartialOrd, Clone, Debug)]
pub struct IndexId {
    data: Vec<u8>,
}

impl IndexId {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
    pub fn create(
        collection_id: &CollectionId,
        index_field_id: u32,
        key: &[u8],
        document_id: &DocumentId,
    ) -> std::result::Result<Self, DB3Error> {
        let mut data: Vec<u8> = Vec::new();
        data.extend(INDEX_ID_TYPE_ID.to_be_bytes());
        data.extend(collection_id.as_ref());
        data.extend(index_field_id.to_be_bytes());
        data.extend(key);
        data.extend(document_id.as_ref());
        Ok(Self { data })
    }

    pub fn get_document_id(&self) -> std::result::Result<DocumentId, DB3Error> {
        DocumentId::try_from_bytes(self.data[self.data.len() - DOCUMENT_ID_LENGTH..].as_ref())
    }
    pub fn get_collection_id(&self) -> std::result::Result<CollectionId, DB3Error> {
        CollectionId::try_from_bytes(
            self.data[TYPE_ID_LENGTH..TYPE_ID_LENGTH + OP_ENTRY_ID_LENGTH].as_ref(),
        )
    }
    pub fn get_index_field_id(&self) -> u32 {
        let mut x: [u8; 4] = [0, 0, 0, 0];
        x.copy_from_slice(
            &self.data[TYPE_ID_LENGTH + OP_ENTRY_ID_LENGTH
                ..TYPE_ID_LENGTH + OP_ENTRY_ID_LENGTH + INDEX_FIELD_ID_LENGTH],
        );
        u32::from_be_bytes(x)
    }

    pub fn get_key(&self) -> std::result::Result<FieldKey, DB3Error> {
        FieldKey::try_from_bytes(
            self.data[TYPE_ID_LENGTH + OP_ENTRY_ID_LENGTH + INDEX_FIELD_ID_LENGTH
                ..self.data.len() - DOCUMENT_ID_LENGTH]
                .as_ref(),
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
        let field_key = self.get_key().map_err(|e| e).unwrap();
        write!(
            f,
            "INDEX|{}|{}|{}|{}",
            collection_id,
            self.get_index_field_id(),
            field_key,
            document_id
        )
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
    use chrono::Utc;
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
        let op_entry_id = OpEntryId::create(1000000, 1000, 100).unwrap();
        assert_eq!(1000000, op_entry_id.get_block_id());
        assert_eq!(1000, op_entry_id.get_mutation_id());
        assert_eq!(100, op_entry_id.get_op_entry_ixd());
        assert_eq!("1000000-1000-100", op_entry_id.to_string())
    }

    #[test]
    fn document_id_ut() {
        let collection_id = CollectionId::create(1000, 100, 10).unwrap();
        let document_entry_id = DocumentEntryId::create(999, 99, 9).unwrap();
        let document_id = DocumentId::create(&collection_id, &document_entry_id).unwrap();
        assert_eq!(collection_id, document_id.get_collection_id().unwrap());
        assert_eq!(
            document_entry_id,
            document_id.get_document_entry_id().unwrap()
        );
        assert_eq!("DOC|1000-100-10|999-99-9", document_id.to_string());

        assert_eq!(
            "AQAAAAAAAAPoAGQACgAAAAAAAAPnAGMACQ==",
            document_id.to_base64()
        );
        assert_eq!(
            DocumentId::try_from_base64("AQAAAAAAAAPoAGQACgAAAAAAAAPnAGMACQ==").unwrap(),
            document_id
        )
    }

    #[test]
    fn index_id_ut() {
        let collection_id = CollectionId::create(1000, 100, 10).unwrap();
        let document_entry_id = DocumentEntryId::create(999, 99, 9).unwrap();
        let document_id = DocumentId::create(&collection_id, &document_entry_id).unwrap();
        let field_key =
            FieldKey::create_single_key(Some(Bson::String("key_content".to_string()))).unwrap();
        let index_id =
            IndexId::create(&collection_id, 3, &field_key.as_ref(), &document_id).unwrap();
        assert_eq!(collection_id, index_id.get_collection_id().unwrap());
        assert_eq!(document_id, index_id.get_document_id().unwrap());
        assert_eq!(3, index_id.get_index_field_id());
        assert_eq!(field_key, index_id.get_key().unwrap());
        assert_eq!(
            "INDEX|1000-100-10|3|key_content|DOC|1000-100-10|999-99-9",
            index_id.to_string()
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

    #[test]
    fn bill_id_smoke_test() {
        let block_id: u64 = 1;
        let mutation_id: u16 = 2;
        let bill_id = BillId::new(block_id, mutation_id).unwrap();
        let b64_str = bill_id.to_base64();
        assert_eq!(b64_str.as_str(), "AAAAAAAAAAEAAg==");
        let bill_id2 = BillId::try_from(bill_id.as_ref()).unwrap();
        let b64_str = bill_id2.to_base64();
        assert_eq!(b64_str.as_str(), "AAAAAAAAAAEAAg==");
        let (start, end) = BillId::get_block_range(1).unwrap();
        assert!(start.as_ref().cmp(end.as_ref()) == std::cmp::Ordering::Less);
    }

    /// helper method to adapter unit test
    fn bson_into_comparison_bytes(field: &Bson) -> std::result::Result<FieldKey, DB3Error> {
        let key = FieldKey::create(&vec![Some(field.clone())])?;
        let extract_field = key.extract_fields()?[0].clone().unwrap();
        assert_eq!(extract_field, field.clone());
        Ok(key)
    }

    #[test]
    fn i64_bson_into_comparison_bytes_ut() {
        let i64_neg_2 = bson_into_comparison_bytes(&Bson::Int64(-2)).unwrap();
        let i64_neg_1 = bson_into_comparison_bytes(&Bson::Int64(-1)).unwrap();
        let i64_small_value1 = -(0x7F00000000000000 as i64);
        let i64_small_1 = bson_into_comparison_bytes(&Bson::Int64(i64_small_value1)).unwrap();
        let i64_small_value2 = -(0x7000000000000000 as i64);
        let i64_small_2 = bson_into_comparison_bytes(&Bson::Int64(i64_small_value2)).unwrap();
        let i64_0 = bson_into_comparison_bytes(&Bson::Int64(0)).unwrap();
        let i64_1 = bson_into_comparison_bytes(&Bson::Int64(1)).unwrap();
        let i64_big_value1 =
            bson_into_comparison_bytes(&Bson::Int64(0x7000000000000000 as i64)).unwrap();
        let i64_big_value2 =
            bson_into_comparison_bytes(&Bson::Int64(0x7F00000000000000 as i64)).unwrap();
        let i64_max = bson_into_comparison_bytes(&Bson::Int64(i64::MAX)).unwrap();
        let i64_min = bson_into_comparison_bytes(&Bson::Int64(i64::MIN)).unwrap();
        println!("i64_min: {:?}", i64_min);
        println!("{} i64_small_value1: {:?}", i64_small_value1, i64_small_1);
        println!("{} i64_small_value2: {:?}", i64_small_value2, i64_small_2);
        println!("i64_-2: {:?}", i64_neg_2);
        println!("i64_-1: {:?}", i64_neg_1);
        println!("i64_0: {:?}", i64_0);
        println!("i64_1: {:?}", i64_1);
        println!(
            "{} i64_big_value1: {:?}",
            0x7000000000000000 as i64, i64_big_value1
        );
        println!(
            "{} i64_big_value2: {:?}",
            0x7F00000000000000 as i64, i64_big_value2
        );
        println!("i64_max: {:?}", i64_max);

        assert!(i64_min < i64_small_1);
        assert!(i64_small_1 < i64_small_2);
        assert!(i64_small_2 < i64_neg_1);
        assert!(i64_neg_2 < i64_1);
        assert!(i64_neg_1 < i64_0);
        assert!(i64_0 < i64_1);
        assert!(i64_1 < i64_big_value1);
        assert!(i64_big_value1 < i64_big_value2);
        assert!(i64_big_value2 < i64_max);
    }

    #[test]
    fn i32_bson_into_comparison_bytes_ut() {
        let i32_small_value1 = -(0x7F000000 as i32);
        let i32_small_1 = bson_into_comparison_bytes(&Bson::Int32(i32_small_value1)).unwrap();
        let i32_small_value2 = -(0x70000000 as i32);
        let i32_small_2 = bson_into_comparison_bytes(&Bson::Int32(i32_small_value2)).unwrap();
        let i32_neg_2 = bson_into_comparison_bytes(&Bson::Int32(-2)).unwrap();
        let i32_neg_1 = bson_into_comparison_bytes(&Bson::Int32(-1)).unwrap();
        let i32_0 = bson_into_comparison_bytes(&Bson::Int32(0)).unwrap();
        let i32_1 = bson_into_comparison_bytes(&Bson::Int32(1)).unwrap();
        let i32_big_value1 = bson_into_comparison_bytes(&Bson::Int32(0x70000000 as i32)).unwrap();
        let i32_big_value2 = bson_into_comparison_bytes(&Bson::Int32(0x7F000000 as i32)).unwrap();
        let i32_max = bson_into_comparison_bytes(&Bson::Int32(i32::MAX)).unwrap();
        let i32_min = bson_into_comparison_bytes(&Bson::Int32(i32::MIN)).unwrap();

        println!("i32_min: {:?}", i32_min);
        println!("{} i32_small_1: {:?}", i32_small_value1, i32_small_1);
        println!("{} i32_small_2: {:?}", i32_small_value2, i32_small_2);
        println!("i32_-2: {:?}", i32_neg_2);
        println!("i32_-1: {:?}", i32_neg_1);
        println!("i32_0: {:?}", i32_0);
        println!("i32_1: {:?}", i32_1);
        println!("{} i32_big_value1: {:?}", 0x70000000 as i32, i32_big_value1);
        println!("{} i32_big_value2: {:?}", 0x7F000000 as i32, i32_big_value2);
        println!("i32_max: {:?}", i32_max);

        assert!(i32_min < i32_small_1);
        assert!(i32_small_1 < i32_small_2);
        assert!(i32_small_2 < i32_neg_2);
        assert!(i32_neg_2 < i32_neg_1);
        assert!(i32_neg_1 < i32_0);
        assert!(i32_0 < i32_1);
        assert!(i32_1 < i32_big_value1);
        assert!(i32_big_value1 < i32_big_value2);
        assert!(i32_big_value2 < i32_max);
    }

    #[test]
    fn string_bson_into_comparison_bytes_ut() {
        let empty_str = bson_into_comparison_bytes(&Bson::String("".to_string())).unwrap();
        let a_str = bson_into_comparison_bytes(&Bson::String("a".to_string())).unwrap();
        let z_str = bson_into_comparison_bytes(&Bson::String("z".to_string())).unwrap();
        let a_long_str = bson_into_comparison_bytes(&Bson::String("abcdefg".to_string())).unwrap();
        println!("empty_str: {:?}", empty_str);
        println!("a_str: {:?}", a_str);
        println!("z_str: {:?}", z_str);
        println!("a_long_str: {:?}", a_long_str);
        assert!(empty_str < a_str);
        assert!(a_str < z_str);
        assert!(a_long_str < z_str);
    }

    #[test]
    fn datetime_bson_into_comparison_bytes_ut() {
        let now_ts = Utc::now().timestamp_millis();
        let now = bson_into_comparison_bytes(&Bson::DateTime(bson::DateTime::from_millis(now_ts)))
            .unwrap();
        let now_minus_one =
            bson_into_comparison_bytes(&Bson::DateTime(bson::DateTime::from_millis(now_ts - 1)))
                .unwrap();
        let now_plus_one =
            bson_into_comparison_bytes(&Bson::DateTime(bson::DateTime::from_millis(now_ts + 1)))
                .unwrap();
        let zero_ts =
            bson_into_comparison_bytes(&Bson::DateTime(bson::DateTime::from_millis(0))).unwrap();
        let min_ts = bson_into_comparison_bytes(&Bson::DateTime(bson::DateTime::MIN)).unwrap();
        let max_ts = bson_into_comparison_bytes(&Bson::DateTime(bson::DateTime::MAX)).unwrap();

        assert!(min_ts < zero_ts);
        assert!(zero_ts < now_minus_one);
        assert!(now_minus_one < now);
        assert!(now < now_plus_one);
        assert!(now_plus_one < max_ts);
    }

    #[test]
    fn multiple_fields_key_ut() {
        let fields = vec![
            Some(Bson::String("a".to_string())),
            Some(Bson::Int64(1 as i64)),
        ];
        let key = FieldKey::create(&fields).unwrap();
        let extract_fields = key.extract_fields().unwrap();
        assert_eq!(extract_fields[0], Some(Bson::String("a".to_string())));
        assert_eq!(extract_fields[1], Some(Bson::Int64(1 as i64)));
        assert_eq!("a#1", format!("{}", key));
        let fields = vec![
            Some(Bson::String("".to_string())),
            Some(Bson::Int64(1 as i64)),
        ];
        let key = FieldKey::create(&fields).unwrap();
        let extract_fields = key.extract_fields().unwrap();
        assert_eq!(extract_fields[0], Some(Bson::String("".to_string())));
        assert_eq!(extract_fields[1], Some(Bson::Int64(1 as i64)));
        assert_eq!("#1", format!("{}", key));
        let fields = vec![
            Some(Bson::Int64(10 as i64)),
            Some(Bson::String("".to_string())),
        ];
        let key = FieldKey::create(&fields).unwrap();
        let extract_fields = key.extract_fields().unwrap();
        assert_eq!(extract_fields[0], Some(Bson::Int64(10 as i64)));
        assert_eq!(extract_fields[1], Some(Bson::String("".to_string())));
        assert_eq!("10#", format!("{}", key));
    }
}
