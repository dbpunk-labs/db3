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
    pub fn into_bytes(&self) -> Vec<u8> {
        let row_doc = RawDocumentBuf::from_document(&self.doc).unwrap();
        row_doc.into_bytes()
    }
    pub fn add_documentId(&mut self, docId: &str) {
        self.doc.insert("_docId", docId);
    }

    pub fn get_documentId(&self) -> Option<&str> {
        match self.doc.get_str("_docId") {
            Ok(docId) => Some(docId),
            Err(err) => None,
        }
    }
    pub fn add_txId(&mut self, txId: &[u8]) {
        self.doc.insert(
            "_txId",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: txId.to_vec(),
            }),
        );
    }
    pub fn get_txId(&mut self) -> Option<&Vec<u8>> {
        match self.doc.get_binary_generic("_txId") {
            Ok(txId) => Some(txId),
            Err(err) => None,
        }
    }

    pub fn add_owner(&mut self, addr: &[u8]) {
        self.doc.insert(
            "_ownerAddr",
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: addr.to_vec(),
            }),
        );
    }
    pub fn get_owner(&self) -> Option<&Vec<u8>> {
        match self.doc.get_binary_generic("_ownerAddr") {
            Ok(addr) => Some(addr),
            Err(err) => None,
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
    use db3_crypto::db3_address::DB3Address;
    use db3_crypto::id::{AccountId, TxId};
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

        println!("document: {:?}", document);
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
        let document = DB3Document::try_from(
            b"\x13\x00\x00\x00\x02hi\x00\x06\x00\x00\x00y'all\x00\x00".to_vec(),
        )
        .unwrap();
        assert_eq!("y'all", document.as_ref().get_str("hi").unwrap());
        assert_eq!(
            document.into_bytes(),
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
        let mut document = DB3Document::try_from(data).unwrap();
        document.add_documentId("123456781234123");
        assert_eq!("123456781234123", document.get_documentId().unwrap());
        assert!(document.get_txId().is_none());
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
        document.add_txId(&txId.as_ref());
        assert!(document.get_documentId().is_none());
        assert_eq!(
            "iLO992XuyfmsgWq7Ob81E86dfzIKeK6MvzFmNDk99R8=",
            TxId::try_from_bytes(document.get_txId().unwrap().as_slice())
                .unwrap()
                .to_base64()
        );
        assert_eq!("John Doe", document.as_ref().get_str("name").unwrap());
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
        document.add_owner(&addr.as_ref());
        assert!(document.get_documentId().is_none());
        assert!(document.get_txId().is_none());
        assert_eq!(
            "0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b",
            AccountId::try_from(document.get_owner().unwrap().as_slice())
                .unwrap()
                .to_hex()
        );
        assert_eq!("John Doe", document.as_ref().get_str("name").unwrap());
    }
}
