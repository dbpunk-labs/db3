//! Key/value store application integration tests.

mod node_integration {
    use bytes::BytesMut;
    use db3_crypto::db3_signer::Db3MultiSchemeSigner;
    use db3_proto::db3_base_proto::{BroadcastMeta, ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::{DatabaseAction, DatabaseMutation};
    use db3_proto::db3_mutation_proto::{PayloadType, WriteRequest};
    use prost::Message;
    use std::time::{SystemTime, UNIX_EPOCH};
    use subtle_encoding::base64;

    fn current_seconds() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(_) => 0,
        }
    }

    fn create_a_database_mutation() -> DatabaseMutation {
        let meta = BroadcastMeta {
            //TODO get from network
            nonce: current_seconds(),
            //TODO use config
            chain_id: ChainId::DevNet.into(),
            //TODO use config
            chain_role: ChainRole::StorageShardChain.into(),
        };
        let dm = DatabaseMutation {
            meta: Some(meta),
            collection_mutations: vec![],
            db_address: vec![],
            action: DatabaseAction::CreateDb.into(),
            document_mutations: vec![],
            db_desc: "".to_string(),
        };
        dm
    }

    #[actix_web::test]
    async fn json_rpc_database_smoke_test() {
        let json_rpc_url = "http://127.0.0.1:26670";
        let client = awc::Client::default();
        let kp = db3_cmd::keystore::KeyStore::get_keypair(None).unwrap();
        let signer = Db3MultiSchemeSigner::new(kp);
        let dm = create_a_database_mutation();
        let mut mbuf = BytesMut::with_capacity(1024 * 4);
        dm.encode(&mut mbuf).unwrap();
        let mbuf = mbuf.freeze();
        let signature = signer.sign(mbuf.as_ref()).unwrap();
        let request = WriteRequest {
            signature: signature.as_ref().to_vec(),
            payload: mbuf.as_ref().to_vec().to_owned(),
            payload_type: PayloadType::DatabasePayload.into(),
        };
        let mut buf = BytesMut::with_capacity(1024 * 4);
        request.encode(&mut buf).unwrap();
        let buf = buf.freeze();
        // encode request to base64
        let data = base64::encode(buf.as_ref());
        let base64_str = String::from_utf8_lossy(data.as_ref()).to_string();
        let request = serde_json::json!(
            {"method": "broadcast",
            "params": vec![base64_str],
            "id": 1,
            "jsonrpc": "2.0"
            }
        );
        let mut response = client.post(json_rpc_url).send_json(&request).await.unwrap();
        if let serde_json::Value::Object(val) = response.json::<serde_json::Value>().await.unwrap()
        {
            if let Some(serde_json::Value::String(s)) = val.get("result") {
                assert!(s.len() > 0);
            } else {
                assert!(false)
            }
        } else {
            assert!(false)
        }
    }
}
