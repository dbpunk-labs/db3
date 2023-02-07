use crate::db3_address::DB3Address;
use crate::id::{AccountId, DocumentId, TxId};
use bson::spec::BinarySubtype;
use bson::Document;
use bson::{Array, Binary, Bson, RawDocumentBuf};
use db3_base::bson_util;
use db3_error::DB3Error;
use serde_json::Value;
#[derive(Debug)]
pub struct DB3Document {
    doc: Document,
}
impl DB3Document {
    pub fn new(
        document: Vec<u8>,
        document_id: &DocumentId,
        tx: &TxId,
        owner: &DB3Address,
    ) -> std::result::Result<Self, DB3Error> {
        match DB3Document::try_from(document) {
            Ok(mut db3_document) => {
                db3_document.add_document_id(document_id);
                db3_document.set_tx_id(tx);
                db3_document.add_owner(owner);
                Ok(db3_document)
            }
            Err(err) => Err(DB3Error::DocumentDecodeError(format!("{:?}", err))),
        }
    }
    pub fn create_from_json_str(
        document: &str,
        document_id: &DocumentId,
        tx: &TxId,
        owner: &DB3Address,
    ) -> Self {
        let mut db3_document = DB3Document::try_from(document).unwrap();
        db3_document.add_document_id(document_id);
        db3_document.set_tx_id(tx);
        db3_document.add_owner(owner);
        db3_document
    }
    pub fn into_bytes(&self) -> Vec<u8> {
        bson_util::bson_document_into_bytes(&self.doc)
    }
    fn add_document_id(&mut self, doc_id: &DocumentId) {
        self.doc.insert(
            "_doc_id",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: doc_id.as_ref().to_vec(),
            }),
        );
    }

    pub fn get_document_id(&self) -> std::result::Result<DocumentId, DB3Error> {
        match self.doc.get_binary_generic("_doc_id") {
            Ok(doc_id) => DocumentId::try_from_bytes(doc_id.as_slice()),
            Err(err) => Err(DB3Error::DocumentDecodeError(format!("{:?}", err))),
        }
    }
    pub fn set_tx_id(&mut self, tx_id: &TxId) {
        self.doc.insert(
            "_tx_id",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: tx_id.as_ref().to_vec(),
            }),
        );
    }
    pub fn get_tx_id(&self) -> std::result::Result<TxId, DB3Error> {
        match self.doc.get_binary_generic("_tx_id") {
            Ok(tx_id) => TxId::try_from_bytes(tx_id.as_slice()),
            Err(err) => Err(DB3Error::DocumentDecodeError(format!("{:?}", err))),
        }
    }

    fn add_owner(&mut self, addr: &DB3Address) {
        self.doc.insert(
            "_ownerAddr",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: addr.as_ref().to_vec(),
            }),
        );
    }
    pub fn get_owner(&self) -> std::result::Result<DB3Address, DB3Error> {
        match self.doc.get_binary_generic("_ownerAddr") {
            Ok(addr) => DB3Address::try_from(addr.as_slice()),
            Err(err) => Err(DB3Error::DocumentDecodeError(format!("{:?}", err))),
        }
    }

    pub fn get_keys(
        &self,
        index: &db3_proto::db3_database_proto::Index,
    ) -> std::result::Result<Bson, DB3Error> {
        let keys: Vec<_> = index.fields.iter().map(|f| f.field_path.as_str()).collect();
        match keys.len() {
            0 => Err(DB3Error::DocumentDecodeError(format!(
                "fail to get empty keys"
            ))),
            1 => self.get_single_key(keys[0]),
            _ => self.get_multiple_keys(keys),
        }
    }
    fn get_single_key(&self, key: &str) -> std::result::Result<Bson, DB3Error> {
        match self.doc.get(key) {
            Some(value) => Ok(value.clone()),
            None => Err(DB3Error::DocumentDecodeError(format!(
                "key {} not exist in document",
                key
            ))),
        }
    }
    fn get_multiple_keys(&self, keys: Vec<&str>) -> std::result::Result<Bson, DB3Error> {
        let mut array_bson: Vec<_> = Vec::new();
        for key in keys.iter() {
            match self.get_single_key(key) {
                Ok(v) => {
                    array_bson.push(v);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(Bson::Array(array_bson))
    }
}
impl AsRef<Document> for DB3Document {
    fn as_ref(&self) -> &Document {
        &self.doc
    }
}
impl TryFrom<&str> for DB3Document {
    type Error = DB3Error;
    fn try_from(json_str: &str) -> std::result::Result<Self, DB3Error> {
        Ok(Self {
            doc: bson_util::json_str_to_bson_document(json_str)
                .map_err(|e| DB3Error::DocumentDecodeError(format!("{}", e)))
                .unwrap(),
        })
    }
}

impl TryFrom<Vec<u8>> for DB3Document {
    type Error = DB3Error;
    fn try_from(buf: Vec<u8>) -> std::result::Result<Self, DB3Error> {
        Ok(Self {
            doc: bson_util::bytes_to_bson_document(buf)
                .map_err(|e| DB3Error::DocumentDecodeError(format!("{}", e)))
                .unwrap(),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{CollectionId, DocumentEntryId};
    use bson::spec::ElementType;
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
    fn try_from_json_str_ut() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let document = DB3Document::try_from(data).unwrap();

        assert_eq!("John Doe", document.as_ref().get_str("name").unwrap());
        assert_eq!(43, document.as_ref().get_i64("age").unwrap());
        let array = document.as_ref().get_array("phones").unwrap();
        let mut phones = vec![];
        for item in array.iter() {
            phones.push(item.as_str().unwrap());
        }
        assert_eq!(vec!["+44 1234567", "+44 2345678"], phones);
    }
    #[test]
    fn try_from_and_into_bytes_ut() {
        let db3_document = DB3Document::try_from(
            b"\x13\x00\x00\x00\x02hi\x00\x06\x00\x00\x00y'all\x00\x00".to_vec(),
        )
        .unwrap();
        assert_eq!("y'all", db3_document.as_ref().get_str("hi").unwrap());
        assert_eq!(
            db3_document.into_bytes(),
            b"\x13\x00\x00\x00\x02hi\x00\x06\x00\x00\x00y'all\x00\x00"
        )
    }

    #[test]
    fn add_doc_id() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;

        let document_id = mock_document_id();
        let mut document = DB3Document::try_from(data).unwrap();
        document.add_document_id(&document_id);
        assert_eq!(document_id, document.get_document_id().unwrap());
        assert!(document.get_tx_id().is_err());
        assert_eq!("John Doe", document.as_ref().get_str("name").unwrap());
    }
    #[test]
    fn add_tx_id() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let mut document = DB3Document::try_from(data).unwrap();
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
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;

        let mut document = DB3Document::try_from(data).unwrap();
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
        assert_eq!("John Doe", document.as_ref().get_str("name").unwrap());
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
        let document = DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr);
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
        let document = DB3Document::new(document, &document_id, &tx_id, &addr).unwrap();
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
        let document = DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr);
        let index_field = IndexField {
            field_path: "name".to_string(),
            value_mode: Some(ValueMode::Order(Order::Ascending as i32)),
        };

        let index = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![index_field],
        };
        if let Ok(keys) = document.get_keys(&index) {
            assert_eq!(keys.element_type(), ElementType::String);
            assert_eq!("John Doe", keys.as_str().unwrap())
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
        let document = DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr);
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
        assert!(res.is_err());
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
        let document = DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr);
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
        if let Ok(keys) = document.get_keys(&index) {
            assert_eq!(keys.element_type(), ElementType::Array);
            println!("keys {}", keys);
            // println!("keys {}", );
            assert_eq!("John Doe", keys.as_array().unwrap()[0].as_str().unwrap());
            assert_eq!(43, keys.as_array().unwrap()[1].as_i64().unwrap());
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
        let document = DB3Document::create_from_json_str(data, &document_id, &tx_id, &addr);
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
        assert!(document.get_keys(&index_has_key_not_exist).is_err());

        let index_empty_keys = Index {
            id: 0,
            name: "idx1".to_string(),
            fields: vec![],
        };
        assert!(document.get_keys(&index_empty_keys).is_err());
    }
}
