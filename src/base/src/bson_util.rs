use bson::Document;
use bson::RawDocumentBuf;
use bson::{Array, Bson};
use db3_error::DB3Error;
use db3_proto::db3_database_proto::structured_query::composite_filter::Operator as CompositeOp;
use db3_proto::db3_database_proto::structured_query::field_filter::Operator;
use db3_proto::db3_database_proto::structured_query::filter::FilterType;
use db3_proto::db3_database_proto::structured_query::value::ValueType;
use db3_proto::db3_database_proto::structured_query::Filter;
use db3_proto::db3_database_proto::structured_query::Value;
use db3_proto::db3_database_proto::structured_query::{CompositeFilter, FieldFilter};
use db3_proto::db3_database_proto::{index::IndexField, Index};
use serde_json::Value as JsonValue;

/// convert json string to Bson::Document
pub fn json_str_to_bson_document(json_str: &str) -> std::result::Result<Document, DB3Error> {
    let value: JsonValue =
        serde_json::from_str(json_str).map_err(|e| DB3Error::InvalidJson(format!("{}", e)))?;
    let bson_document =
        bson::to_document(&value).map_err(|e| DB3Error::InvalidDocumentBytes(format!("{}", e)))?;
    Ok(bson_document)
}

pub fn json_str_to_index(json_str: &str, idx: u32) -> std::result::Result<Index, DB3Error> {
    let value: JsonValue =
        serde_json::from_str(json_str).map_err(|e| DB3Error::InvalidJson(format!("{}", e)))?;

    if let Some(name) = value.get("name") {
        if let Some(fields) = value.get("fields") {
            return Ok(Index {
                id: idx,
                name: name.as_str().unwrap().to_string(),
                fields: fields
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|field| serde_json::from_value::<IndexField>(field.clone()).unwrap())
                    .collect(),
            });
        }
    }
    Err(DB3Error::InvalidJson(format!("")))
}

pub fn json_str_to_bson_bytes(json_str: &str) -> std::result::Result<Vec<u8>, DB3Error> {
    match json_str_to_bson_document(json_str) {
        Ok(doc) => Ok(bson_document_into_bytes(&doc)),
        Err(err) => Err(err),
    }
}

/// convert bytes to Bson::Document
pub fn bytes_to_bson_document(buf: Vec<u8>) -> std::result::Result<Document, DB3Error> {
    let doc = RawDocumentBuf::from_bytes(buf)
        .map_err(|e| DB3Error::InvalidDocumentBytes(format!("{}", e)))?;
    let bson_document = doc
        .to_document()
        .map_err(|e| DB3Error::InvalidDocumentBytes(format!("{}", e)))
        .unwrap();
    Ok(bson_document)
}

/// convert Bson::Document into bytes
pub fn bson_document_into_bytes(doc: &Document) -> Vec<u8> {
    let row_doc = RawDocumentBuf::from_document(doc).unwrap();
    row_doc.into_bytes()
}

pub fn bson_value_from_proto_value(value: &Value) -> std::result::Result<Bson, DB3Error> {
    if let Some(value_type) = &value.value_type {
        match value_type {
            ValueType::BooleanValue(b) => Ok(Bson::Boolean(*b)),
            ValueType::IntegerValue(n) => Ok(Bson::Int64(*n)),
            ValueType::StringValue(s) => Ok(Bson::String(s.to_string())),
            _ => Err(DB3Error::InvalidFilterValue(
                "value is not support".to_string(),
            )),
        }
    } else {
        Err(DB3Error::InvalidFilterValue("value is none".to_string()))
    }
}

fn field_filter_from_json_value(
    filter_doc: &Document,
) -> std::result::Result<Option<Filter>, DB3Error> {
    let field = filter_doc.get_str("field").map_err(|e| {
        DB3Error::InvalidFilterJson(format!("filed is required in filter json for {e}"))
    })?;
    let value = match filter_doc.get("value") {
        Some(v) => filter_value_from_bson_value(v)?,
        None => {
            return Err(DB3Error::InvalidFilterJson(
                "value is required in filter json".to_string(),
            ));
        }
    };

    let op_str = filter_doc
        .get_str("op")
        .map_err(|_| DB3Error::InvalidFilterJson("op is required in filter json".to_string()))?;
    let op = match op_str {
        "==" => Operator::Equal,
        ">" => Operator::GreaterThan,
        "<" => Operator::LessThan,
        ">=" => Operator::GreaterThanOrEqual,
        "<=" => Operator::LessThanOrEqual,
        "!=" => {
            return Err(DB3Error::InvalidFilterOp(format!(
                "OP {} un-support currently",
                op_str
            )));
        }
        _ => {
            return Err(DB3Error::InvalidFilterOp(format!("Invalid OP {}", op_str)));
        }
    };

    Ok(Some(Filter {
        filter_type: Some(FilterType::FieldFilter(FieldFilter {
            field: field.to_string(),
            op: op.into(),
            value: Some(value),
        })),
    }))
}

fn composite_filter_from_json_value(
    filters_doc: &Array,
    op: CompositeOp,
) -> std::result::Result<Option<Filter>, DB3Error> {
    if filters_doc.is_empty() {
        return Err(DB3Error::InvalidFilterJson("filters is empty".to_string()));
    }
    let mut filters = vec![];
    for filter in filters_doc {
        if let Some(filter_doc) = filter.as_document() {
            let op_str = filter_doc.get_str("op").map_err(|_| {
                DB3Error::InvalidFilterJson("op is required in filter json".to_string())
            })?;

            // only support == in composite filter
            if op_str != "==" {
                return Err(DB3Error::InvalidFilterJson(format!(
                    "{} is not support in composite filter",
                    op_str
                )));
            };
            if let Ok(Some(filter)) = field_filter_from_json_value(filter_doc) {
                filters.push(filter);
            } else {
                return Err(DB3Error::InvalidFilterJson(
                    "invalid field filter".to_string(),
                ));
            }
        } else {
            return Err(DB3Error::InvalidFilterJson("invalid document".to_string()));
        }
    }

    Ok(Some(Filter {
        filter_type: Some(FilterType::CompositeFilter(CompositeFilter {
            filters,
            op: op.into(),
        })),
    }))
}

pub fn filter_from_json_value(json_str: &str) -> std::result::Result<Option<Filter>, DB3Error> {
    if json_str.is_empty() {
        Ok(None)
    } else {
        let filter_doc = json_str_to_bson_document(json_str)
            .map_err(|e| DB3Error::InvalidFilterValue(format!("{:?}", e)))?;

        if filter_doc.contains_key("field") {
            field_filter_from_json_value(&filter_doc)
        } else if filter_doc.contains_key("AND") {
            if let Ok(filters) = filter_doc.get_array("AND") {
                composite_filter_from_json_value(filters, CompositeOp::And)
            } else {
                Err(DB3Error::InvalidFilterJson(
                    "filter json is invalid".to_string(),
                ))
            }
        } else if filter_doc.contains_key("and") {
            if let Ok(filters) = filter_doc.get_array("and") {
                composite_filter_from_json_value(filters, CompositeOp::And)
            } else {
                Err(DB3Error::InvalidFilterJson(
                    "filter json is invalid".to_string(),
                ))
            }
        } else {
            Err(DB3Error::InvalidFilterJson(
                "filter json is invalid".to_string(),
            ))
        }
    }
}

pub fn filter_value_from_bson_value(value: &Bson) -> std::result::Result<Value, DB3Error> {
    match value {
        Bson::Boolean(b) => Ok(Value {
            value_type: Some(ValueType::BooleanValue(*b)),
        }),
        Bson::Int32(n) => Ok(Value {
            value_type: Some(ValueType::IntegerValue(*n as i64)),
        }),
        Bson::Int64(n) => Ok(Value {
            value_type: Some(ValueType::IntegerValue(*n)),
        }),
        Bson::String(s) => Ok(Value {
            value_type: Some(ValueType::StringValue(s.to_string())),
        }),
        _ => Err(DB3Error::InvalidFilterValue(format!(
            "type {:?} un-support for filter value",
            value.element_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bson_util::{
        bson_document_into_bytes, bytes_to_bson_document, json_str_to_bson_document,
    };
    use bson::Bson;
    use db3_proto::db3_database_proto::index::index_field::{Order, ValueMode};

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
    fn field_filter_from_json_value_ut() {
        let filter = filter_from_json_value("").unwrap();
        assert!(filter.is_none());

        let filter = filter_from_json_value(r#"{"field": "name", "value": "Bill", "op": "=="}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"{"filter_type":{"FieldFilter":{"field":"name","op":5,"value":{"value_type":{"StringValue":"Bill"}}}}}"#,
            serde_json::to_string(&filter).unwrap()
        );

        let filter = filter_from_json_value(r#"{"field": "name", "value": 45, "op": "=="}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"{"filter_type":{"FieldFilter":{"field":"name","op":5,"value":{"value_type":{"IntegerValue":45}}}}}"#,
            serde_json::to_string(&filter).unwrap()
        );

        let filter = filter_from_json_value(r#"{"field": "flag", "value": true, "op": "=="}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"{"filter_type":{"FieldFilter":{"field":"flag","op":5,"value":{"value_type":{"BooleanValue":true}}}}}"#,
            serde_json::to_string(&filter).unwrap()
        );

        let filter = filter_from_json_value(r#"{"field": "flag", "value": true, "op": ">="}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"{"filter_type":{"FieldFilter":{"field":"flag","op":4,"value":{"value_type":{"BooleanValue":true}}}}}"#,
            serde_json::to_string(&filter).unwrap()
        );
        let filter = filter_from_json_value(r#"{"field": "flag", "value": true, "op": ">"}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"{"filter_type":{"FieldFilter":{"field":"flag","op":3,"value":{"value_type":{"BooleanValue":true}}}}}"#,
            serde_json::to_string(&filter).unwrap()
        );
        let filter = filter_from_json_value(r#"{"field": "flag", "value": true, "op": "<="}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"{"filter_type":{"FieldFilter":{"field":"flag","op":2,"value":{"value_type":{"BooleanValue":true}}}}}"#,
            serde_json::to_string(&filter).unwrap()
        );
        let filter = filter_from_json_value(r#"{"field": "flag", "value": true, "op": "<"}"#)
            .unwrap()
            .unwrap();
        assert_eq!(
            r#"{"filter_type":{"FieldFilter":{"field":"flag","op":1,"value":{"value_type":{"BooleanValue":true}}}}}"#,
            serde_json::to_string(&filter).unwrap()
        );

        assert!(filter_from_json_value("{}").is_err());
        assert!(filter_from_json_value(r#"{"field": "name"}"#).is_err());
    }

    #[test]
    fn composite_filter_from_json_value_ut() {
        let filter = filter_from_json_value(
            r#"{
            "and": [
                {"field": "name", "value": "Bill", "op": "=="},
                {"field": "age", "value": 44, "op": "=="}
            ]
        }"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            r#"{"filter_type":{"CompositeFilter":{"op":1,"filters":[{"filter_type":{"FieldFilter":{"field":"name","op":5,"value":{"value_type":{"StringValue":"Bill"}}}}},{"filter_type":{"FieldFilter":{"field":"age","op":5,"value":{"value_type":{"IntegerValue":44}}}}}]}}}"#,
            serde_json::to_string(&filter).unwrap()
        );

        assert!(filter_from_json_value(
            r#"{
            "and": []
        }"#
        )
        .is_err());
        assert!(filter_from_json_value(
            r#"{
            "or": [
                {"field": "name", "value": "Bill", "op": "=="},
                {"field": "age", "value": 44, "op": "=="}
            ]
        }"#
        )
        .is_err());
        assert!(filter_from_json_value(
            r#"{
            "and": [
                {"field": "name", "value": "Bill", "op": "=="},
                {"field": "age", "value": 44, "op": ">="}
            ]
        }"#
        )
        .is_err());
    }

    #[test]
    fn bson_value_from_proto_value_ut() {
        assert!(bson_value_from_proto_value(&Value { value_type: None }).is_err());
        assert_eq!(
            (Bson::Boolean(true)),
            bson_value_from_proto_value(&Value {
                value_type: Some(ValueType::BooleanValue(true))
            })
            .unwrap()
        );

        assert_eq!(
            (Bson::Boolean(false)),
            bson_value_from_proto_value(&Value {
                value_type: Some(ValueType::BooleanValue(false))
            })
            .unwrap()
        );

        assert_eq!(
            (Bson::Int64(i64::MAX)),
            bson_value_from_proto_value(&Value {
                value_type: Some(ValueType::IntegerValue(i64::MAX))
            })
            .unwrap()
        );
        assert_eq!(
            (Bson::Int64(i64::MIN)),
            bson_value_from_proto_value(&Value {
                value_type: Some(ValueType::IntegerValue(i64::MIN))
            })
            .unwrap()
        );
        assert_eq!(
            (Bson::Int64(0)),
            bson_value_from_proto_value(&Value {
                value_type: Some(ValueType::IntegerValue(0))
            })
            .unwrap()
        );

        assert_eq!(
            (Bson::String("".to_string())),
            bson_value_from_proto_value(&Value {
                value_type: Some(ValueType::StringValue("".to_string()))
            })
            .unwrap()
        );

        assert_eq!(
            (Bson::String("aaaaaaaaaaaaaaaaaaaaaaaaaa".to_string())),
            bson_value_from_proto_value(&Value {
                value_type: Some(ValueType::StringValue(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()
                ))
            })
            .unwrap()
        );
    }

    #[test]
    fn json_str_to_index_ut() {
        let json_str = r#"{"name":"idx1","fields":[{"field_path":"name","value_mode":{"Order":1}}, {"field_path":"age","value_mode":{"Order":1}}]}"#;
        let res = json_str_to_index(json_str, 1);
        assert!(res.is_ok());
        let index = res.unwrap();
        println!("{:?}", index);
        let expect = Index {
            id: 1,
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
        assert_eq!(expect, index);
    }

    #[test]
    fn json_str_to_index_wrong_path_1_ut() {
        let json_str = r#"{"fields":[{"field_path":"name","value_mode":{"Order":1}}, {"field_path":"age","value_mode":{"Order":1}}]}"#;
        let res = json_str_to_index(json_str, 1);
        assert!(res.is_err());
    }

    #[test]
    fn json_str_to_index_wrong_path_2_ut() {
        let json_str = r#"{"name": "idx1""#;
        let res = json_str_to_index(json_str, 1);
        assert!(res.is_err());
    }
}
