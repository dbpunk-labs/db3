//
// db3_document.rs
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
//

use bson::spec::BinarySubtype;
use bson::Document;
use bson::{Binary, Bson};
use db3_base::bson_util;
use db3_crypto::db3_address::DB3Address;
use db3_crypto::id::{DocumentId, FieldKey, TxId};
use db3_error::DB3Error;
#[derive(Debug)]
pub struct DB3Document {
    root: Document,
}
impl DB3Document {
    pub fn new(
        document: Document,
        document_id: &DocumentId,
        tx: &TxId,
        owner: &DB3Address,
    ) -> Self {
        let mut db3_document = DB3Document {
            root: Document::new(),
        };
        db3_document.add_document(document);
        db3_document.add_document_id(document_id);
        db3_document.add_owner(owner);
        db3_document.set_tx_id(tx);
        db3_document
    }
    pub fn create_from_document_bytes(
        document_bytes: Vec<u8>,
        document_id: &DocumentId,
        tx: &TxId,
        owner: &DB3Address,
    ) -> std::result::Result<Self, DB3Error> {
        match bson_util::bytes_to_bson_document(document_bytes) {
            Ok(document) => Ok(DB3Document::new(document, document_id, tx, owner)),
            Err(err) => Err(DB3Error::DocumentDecodeError(format!("{:?}", err))),
        }
    }

    #[cfg(test)]
    pub fn create_from_json_str(
        document_json: &str,
        document_id: &DocumentId,
        tx: &TxId,
        owner: &DB3Address,
    ) -> std::result::Result<Self, DB3Error> {
        match bson_util::json_str_to_bson_document(document_json) {
            Ok(document) => Ok(DB3Document::new(document, document_id, tx, owner)),
            Err(err) => Err(DB3Error::DocumentDecodeError(format!("{:?}", err))),
        }
    }

    pub fn into_bytes(&self) -> Vec<u8> {
        bson_util::bson_document_into_bytes(&self.root)
    }
    fn add_document(&mut self, doc: Document) {
        self.root.insert("_doc", doc);
    }

    pub fn get_document(&self) -> std::result::Result<&Document, DB3Error> {
        let doc = self
            .root
            .get_document("_doc")
            .map_err(|e| DB3Error::DocumentDecodeError(format!("{:?}", e)))?;
        Ok(doc)
    }

    fn add_document_id(&mut self, doc_id: &DocumentId) {
        self.root.insert(
            "_doc_id",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: doc_id.as_ref().to_vec(),
            }),
        );
    }

    #[cfg(test)]
    pub fn get_document_id(&self) -> std::result::Result<DocumentId, DB3Error> {
        match self.root.get_binary_generic("_doc_id") {
            Ok(doc_id) => DocumentId::try_from_bytes(doc_id.as_slice()),
            Err(err) => Err(DB3Error::DocumentDecodeError(format!("{:?}", err))),
        }
    }
    pub fn set_tx_id(&mut self, tx_id: &TxId) {
        self.root.insert(
            "_tx_id",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: tx_id.as_ref().to_vec(),
            }),
        );
    }
    pub fn get_tx_id(&self) -> std::result::Result<TxId, DB3Error> {
        match self.root.get_binary_generic("_tx_id") {
            Ok(tx_id) => TxId::try_from_bytes(tx_id.as_slice()),
            Err(err) => Err(DB3Error::DocumentDecodeError(format!("{:?}", err))),
        }
    }

    fn add_owner(&mut self, addr: &DB3Address) {
        self.root.insert(
            "_ownerAddr",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: addr.as_ref().to_vec(),
            }),
        );
    }
    pub fn get_owner(&self) -> std::result::Result<DB3Address, DB3Error> {
        match self.root.get_binary_generic("_ownerAddr") {
            Ok(addr) => DB3Address::try_from(addr.as_slice()),
            Err(err) => Err(DB3Error::DocumentDecodeError(format!("{:?}", err))),
        }
    }

    pub fn get_keys(
        &self,
        index: &db3_proto::db3_database_proto::Index,
    ) -> std::result::Result<Option<FieldKey>, DB3Error> {
        let keys: Vec<_> = index.fields.iter().map(|f| f.field_path.as_str()).collect();
        match keys.len() {
            0 => Err(DB3Error::DocumentDecodeError(format!(
                "fail to get empty keys"
            ))),
            _ => self.get_multiple_keys(keys),
        }
    }

    fn get_multiple_keys(
        &self,
        keys: Vec<&str>,
    ) -> std::result::Result<Option<FieldKey>, DB3Error> {
        let mut fields = vec![];
        let mut has_field = false;
        for key in keys.iter() {
            match self.get_document()?.get(key) {
                Some(value) => {
                    fields.push(Some(value.clone()));
                    has_field = true;
                }
                None => fields.push(None),
            }
        }
        if has_field {
            Ok(Some(FieldKey::create(&fields)?))
        } else {
            // if no fields exist, return None
            Ok(None)
        }
    }
}
impl AsRef<Document> for DB3Document {
    fn as_ref(&self) -> &Document {
        &self.root
    }
}
impl TryFrom<Vec<u8>> for DB3Document {
    type Error = DB3Error;
    fn try_from(buf: Vec<u8>) -> std::result::Result<Self, DB3Error> {
        Ok(Self {
            root: bson_util::bytes_to_bson_document(buf)
                .map_err(|e| DB3Error::DocumentDecodeError(format!("{}", e)))
                .unwrap(),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use db3_crypto::id::{AccountId, CollectionId, DocumentEntryId};
    use db3_proto::db3_database_proto::{
        index::index_field::{Order, ValueMode},
        index::IndexField,
        Index,
    };

    fn mock_document_id() -> DocumentId {
        let collection_id = CollectionId::create(99999, 999, 99).unwrap();
        let document_entry_id = DocumentEntryId::create(100000, 1000, 100).unwrap();
        DocumentId::create(&collection_id, &document_entry_id).unwrap()
    }

    #[test]
    fn add_doc_id() {
        let document_id = mock_document_id();
        let mut document = DB3Document {
            root: Document::new(),
        };
        document.add_document_id(&document_id);
        assert_eq!(document_id, document.get_document_id().unwrap());
        assert!(document.get_tx_id().is_err());
    }
    #[test]
    fn add_tx_id() {
        let mut document = DB3Document {
            root: Document::new(),
        };
        let tx_id = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();
        document.set_tx_id(&tx_id);
        assert!(document.get_document_id().is_err());
        assert_eq!(
            "iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=",
            document.get_tx_id().unwrap().to_base64()
        );
    }
    #[test]
    fn add_addr() {
        let mut document = DB3Document {
            root: Document::new(),
        };
        assert!(document.get_owner().is_err());
        let addr = DB3Address::try_from("0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b").unwrap();
        document.add_owner(&addr);
        assert!(document.get_document_id().is_err());
        assert!(document.get_tx_id().is_err());
        assert_eq!(
            "0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b",
            AccountId::try_from(document.get_owner().unwrap().as_ref())
                .unwrap()
                .to_hex()
        );
    }

    #[test]
    fn get_document_wrong_path() {
        let document = DB3Document {
            root: Document::new(),
        };

        let res = document.get_document();
        assert!(res.is_err());
    }

    #[test]
    fn create_from_json_str_ut() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let addr = DB3Address::try_from("0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b").unwrap();
        let tx_id = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();
        let document_id = mock_document_id();
        let document =
            DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr).unwrap();
        assert_eq!(
            "0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b",
            AccountId::try_from(document.get_owner().unwrap().as_ref())
                .unwrap()
                .to_hex()
        );
        assert_eq!(
            "iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=",
            document.get_tx_id().unwrap().to_base64()
        );
        assert_eq!(document_id, document.get_document_id().unwrap());
    }
    #[test]
    fn new_ut() {
        let document = b"\x13\x00\x00\x00\x02hi\x00\x06\x00\x00\x00y'all\x00\x00".to_vec();

        let addr = DB3Address::try_from("0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b").unwrap();
        let tx_id = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();
        let document_id = mock_document_id();
        let document =
            DB3Document::create_from_document_bytes(document, &document_id, &tx_id, &addr).unwrap();
        assert_eq!(
            "0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b",
            AccountId::try_from(document.get_owner().unwrap().as_ref())
                .unwrap()
                .to_hex()
        );
        assert_eq!(
            "iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=",
            document.get_tx_id().unwrap().to_base64()
        );
        assert_eq!(document_id, document.get_document_id().unwrap());
    }

    #[test]
    fn get_single_keys_ut_happy_path() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let addr = DB3Address::try_from("0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b").unwrap();
        let tx_id = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();
        let document_id = mock_document_id();
        let document =
            DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr).unwrap();
        let index_field = IndexField {
            field_path: "name".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };

        let index = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![index_field],
        };
        if let Ok(Some(keys)) = document.get_keys(&index) {
            let fields = keys.extract_fields().unwrap();
            assert_eq!(Some(Bson::String("John Doe".to_string())), fields[0]);
        } else {
            assert!(false);
        }
    }
    #[test]
    fn get_single_string_key_ut_happy_path() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let addr = DB3Address::try_from("0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b").unwrap();
        let tx_id = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();
        let document_id = mock_document_id();
        let document =
            DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr).unwrap();
        let index_field = IndexField {
            field_path: "name".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };

        let index = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![index_field],
        };
        if let Ok(Some(keys)) = document.get_keys(&index) {
            assert_eq!(
                Some(Bson::String("John Doe".to_string())),
                keys.extract_fields().unwrap()[0]
            );
        } else {
            assert!(false);
        }
    }
    #[test]
    fn get_single_i64_key_ut_happy_path() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let addr = DB3Address::try_from("0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b").unwrap();
        let tx_id = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();
        let document_id = mock_document_id();
        let document =
            DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr).unwrap();
        let index_field = IndexField {
            field_path: "age".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };

        let index = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![index_field],
        };
        if let Ok(Some(keys)) = document.get_keys(&index) {
            assert_eq!(vec![21, 128, 0, 0, 0, 0, 0, 0, 43], *keys.as_ref());
        } else {
            assert!(false);
        }
    }

    #[test]
    fn get_single_keys_ut_wrong_path() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let addr = DB3Address::try_from("0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b").unwrap();
        let tx_id = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();

        let document_id = mock_document_id();
        let document =
            DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr).unwrap();
        let index_field = IndexField {
            field_path: "key_not_exist".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };

        let index = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![index_field],
        };
        let res = document.get_keys(&index);
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
    }

    #[test]
    fn get_multi_keys_ut_happy_path() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let addr = DB3Address::try_from("0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b").unwrap();
        let tx_id = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();
        let document_id = mock_document_id();
        let document =
            DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr).unwrap();
        let index = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![
                IndexField {
                    field_path: "name".to_string(),
                    value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
                },
                IndexField {
                    field_path: "age".to_string(),
                    value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
                },
            ],
        };
        if let Ok(Some(keys)) = document.get_keys(&index) {
            assert_eq!(
                Some(Bson::String("John Doe".to_string())),
                keys.extract_fields().unwrap()[0]
            );
            assert_eq!(Some(Bson::Int64(43)), keys.extract_fields().unwrap()[1]);
        } else {
            assert!(false);
        }
    }
    #[test]
    fn get_multi_keys_ut_wrong_path() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let addr = DB3Address::try_from("0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b").unwrap();
        let tx_id = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();
        let document_id = mock_document_id();
        let document =
            DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr).unwrap();
        let index_has_key_not_exist = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![
                IndexField {
                    field_path: "name_not_exit".to_string(),
                    value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
                },
                IndexField {
                    field_path: "age".to_string(),
                    value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
                },
            ],
        };
        assert!(document.get_keys(&index_has_key_not_exist).is_ok());
        let index_empty_keys = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![],
        };
        assert!(document.get_keys(&index_empty_keys).is_err());
    }
}
