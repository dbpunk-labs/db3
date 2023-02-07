use bson::Document;
use bson::{Array, Binary, Bson, RawDocumentBuf};
use serde_json::Value;

/// convert json string to Bson::Document
pub fn json_str_to_bson_document(json_str: &str) -> std::result::Result<Document, String> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| format!("{}", e))
        .unwrap();
    let bson_document = bson::to_document(&value)
        .map_err(|e| format!("{}", e))
        .unwrap();
    Ok(bson_document)
}

pub fn json_str_to_bson_bytes(json_str: &str) -> std::result::Result<Vec<u8>, String> {
    match json_str_to_bson_document(json_str) {
        Ok(doc) => Ok(bson_document_into_bytes(&doc)),
        Err(err) => Err(err),
    }
}

/// convert bytes to Bson::Document
pub fn bytes_to_bson_document(buf: Vec<u8>) -> std::result::Result<Document, String> {
    let doc = RawDocumentBuf::from_bytes(buf)
        .map_err(|e| format!("{}", e))
        .unwrap();
    let bson_document = doc.to_document().map_err(|e| format!("{}", e)).unwrap();
    Ok(bson_document)
}

/// convert Bson::Document into bytes
pub fn bson_document_into_bytes(doc: &Document) -> Vec<u8> {
    let row_doc = RawDocumentBuf::from_document(doc).unwrap();
    row_doc.into_bytes()
}

#[cfg(test)]
mod tests {
    use crate::bson_util::{
        bson_document_into_bytes, bytes_to_bson_document, json_str_to_bson_bytes,
        json_str_to_bson_document,
    };

    #[test]
    fn json_str_to_bson_document_ut() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;
        let document = json_str_to_bson_document(data).unwrap();

        assert_eq!("John Doe", document.get_str("name").unwrap());
        assert_eq!(43, document.get_i64("age").unwrap());
        let array = document.get_array("phones").unwrap();
        let mut phones = vec![];
        for item in array.iter() {
            phones.push(item.as_str().unwrap());
        }
        assert_eq!(vec!["+44 1234567", "+44 2345678"], phones);
    }

    #[test]
    fn bytes_to_bson_document_ut() {
        let document = bytes_to_bson_document(
            b"\x13\x00\x00\x00\x02hi\x00\x06\x00\x00\x00y'all\x00\x00".to_vec(),
        )
        .unwrap();
        assert_eq!("y'all", document.get_str("hi").unwrap());
        assert_eq!(
            bson_document_into_bytes(&document),
            b"\x13\x00\x00\x00\x02hi\x00\x06\x00\x00\x00y'all\x00\x00"
        )
    }
}
