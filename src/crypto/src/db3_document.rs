use crate::db3_address::DB3Address;
use crate::id::{AccountId, DocumentId, TxId};
use bson::spec::BinarySubtype;
use bson::Document;
use bson::{Binary, Bson, RawDocumentBuf};
use db3_error::{DB3Error, Result};
use fastcrypto::traits::ToFromBytes;
use serde_json::{Map, Value};

#[derive(Debug)]
pub struct DB3Document {
    doc: Document,
}
impl DB3Document {
    pub fn new(document: Vec<u8>, document_id: &DocumentId, tx: &TxId, owner: &DB3Address) -> Self {
        let mut db3_document = DB3Document::try_from(document).unwrap();
        db3_document.add_document_id(document_id);
        db3_document.add_tx_id(tx);
        db3_document.add_owner(owner);
        db3_document
    }
    pub fn create_from_json_str(
        document: &str,
        document_id: &DocumentId,
        tx: &TxId,
        owner: &DB3Address,
    ) -> Self {
        let mut db3_document = DB3Document::try_from(document).unwrap();
        db3_document.add_document_id(document_id);
        db3_document.add_tx_id(tx);
        db3_document.add_owner(owner);
        db3_document
    }
    pub fn into_bytes(&self) -> Vec<u8> {
        let row_doc = RawDocumentBuf::from_document(&self.doc).unwrap();
        row_doc.into_bytes()
    }
    pub fn add_document_id(&mut self, docId: &DocumentId) {
        self.doc.insert(
            "_docId",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: docId.as_ref().to_vec(),
            }),
        );
    }

    pub fn get_document_id(&self) -> std::result::Result<DocumentId, DB3Error> {
        match self.doc.get_binary_generic("_docId") {
            Ok(docId) => DocumentId::try_from_bytes(docId.as_slice()),
            Err(err) => Err(DB3Error::DocumentDecodeError(format!("{:?}", err))),
        }
    }
    pub fn add_tx_id(&mut self, tx_id: &TxId) {
        self.doc.insert(
            "_txId",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: tx_id.as_ref().to_vec(),
            }),
        );
    }
    pub fn get_tx_id(&self) -> std::result::Result<TxId, DB3Error> {
        match self.doc.get_binary_generic("_txId") {
            Ok(txId) => TxId::try_from_bytes(txId.as_slice()),
            Err(err) => Err(DB3Error::DocumentDecodeError(format!("{:?}", err))),
        }
    }

    pub fn add_owner(&mut self, addr: &DB3Address) {
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
}
impl AsRef<Document> for DB3Document {
    fn as_ref(&self) -> &Document {
        &self.doc
    }
}
impl TryFrom<&str> for DB3Document {
    type Error = DB3Error;
    fn try_from(json_str: &str) -> std::result::Result<Self, DB3Error> {
        // Parse the string of data into serde_json::Value.
        let value: Value = serde_json::from_str(json_str)
            .map_err(|e| DB3Error::DocumentDecodeError(format!("{}", e)))
            .unwrap();
        let bson_value = bson::to_document(&value)
            .map_err(|e| DB3Error::DocumentDecodeError(format!("{}", e)))
            .unwrap();
        Ok(Self { doc: bson_value })
    }
}

impl TryFrom<Vec<u8>> for DB3Document {
    type Error = DB3Error;
    fn try_from(buf: Vec<u8>) -> std::result::Result<Self, DB3Error> {
        // Parse the string of data into serde_json::Value.
        let doc = RawDocumentBuf::from_bytes(buf)
            .map_err(|e| DB3Error::DocumentDecodeError(format!("{}", e)))
            .unwrap();
        let bson_value = doc
            .to_document()
            .map_err(|e| DB3Error::DocumentDecodeError(format!("{}", e)))
            .unwrap();
        Ok(Self { doc: bson_value })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
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
    fn add_docId() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let document_id = DocumentId::create(100000, 1000, 100).unwrap();
        let mut document = DB3Document::try_from(data).unwrap();
        document.add_document_id(&document_id);
        assert_eq!(document_id, document.get_document_id().unwrap());
        assert!(document.get_tx_id().is_err());
        assert_eq!("John Doe", document.as_ref().get_str("name").unwrap());
    }
    #[test]
    fn add_txId() {
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
        let txId = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();
        document.add_tx_id(&txId);
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
        let txId = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();
        let document_id = DocumentId::create(100000, 1000, 100).unwrap();
        let document = DB3Document::create_from_json_str(data, &document_id, &txId, &addr);
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
        let txId = TxId::try_from_base64("iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=").unwrap();
        let document_id = DocumentId::create(100000, 1000, 100).unwrap();
        let document = DB3Document::new(document, &document_id, &txId, &addr);
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
}
