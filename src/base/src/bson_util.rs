use bson::Bson;
use bson::Document;
use bson::RawDocumentBuf;
use byteorder::{BigEndian, WriteBytesExt};
use db3_error::DB3Error;
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

fn keep_order_i32(input: i32) -> u32 {
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

fn keep_order_i64(input: i64) -> u64 {
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

/// convert bson value to bytes for key comparation
pub fn bson_into_comparison_bytes(value: &Bson) -> std::result::Result<Option<Vec<u8>>, DB3Error> {
    let mut data: Vec<u8> = Vec::new();
    match value {
        Bson::Null => Ok(None),
        Bson::Boolean(b) => {
            data.write_u8(*b as u8)
                .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
            Ok(Some(data))
        }
        Bson::Int64(n) => {
            data.write_u64::<BigEndian>(keep_order_i64(*n))
                .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
            Ok(Some(data))
        }
        Bson::Int32(n) => {
            data.write_u32::<BigEndian>(keep_order_i32(*n))
                .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
            Ok(Some(data))
        }
        // TODO: add \0 as the end of string.
        Bson::String(s) => {
            data.extend_from_slice(s.as_bytes());
            Ok(Some(data))
        }
        Bson::DateTime(dt) => {
            let value: u64 = keep_order_i64(dt.timestamp_millis());
            data.write_u64::<BigEndian>(value)
                .map_err(|e| DB3Error::DocumentDecodeError(format!("{e}")))?;
            Ok(Some(data))
        }
        _ => Err(DB3Error::DocumentDecodeError(
            "value type is not supported".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::bson_util::{
        bson_document_into_bytes, bson_into_comparison_bytes, bytes_to_bson_document,
        json_str_to_bson_document,
    };
    use bson::raw::RawBson;
    use bson::Bson;
    use bson::Document;
    use chrono::Utc;
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
}
