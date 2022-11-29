mod json_rpc_test {
    use actix_cors::Cors;
    use actix_web::{test, web, App};
    use db3_node::auth_storage::AuthStorage;
    use db3_node::context::Context;
    use db3_node::json_rpc::Response;
    use db3_node::json_rpc_impl;
    use db3_node::node_storage::NodeStorage;
    use merk::Merk;
    use serde_json::Number;
    use serde_json::Value;
    use std::sync::Arc;
    use std::sync::Mutex;
    use bytes::BytesMut;
    use subtle_encoding::base64;
    use tendermint_rpc::HttpClient;
    use db3_crypto::signer::Db3Signer;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::{KvPair, Mutation, MutationAction, WriteRequest};
    use prost::Message;

    pub async fn init_context() -> Context {
        let db_path = "../db";
        let tm_port = "26657".to_string();
        let merk = Merk::open(&db_path).unwrap();
        let node_store = Arc::new(Mutex::new(Box::pin(NodeStorage::new(AuthStorage::new(
            merk,
        )))));

        let tm_addr = format!("http://127.0.0.1:{}", tm_port);
        let client = HttpClient::new(tm_addr.as_str()).unwrap();
        let context = Context {
            node_store: node_store.clone(),
            client,
        };

        context
    }

    #[actix_web::test]
    async fn test_latest_blocks() {
        let context = init_context().await;

        let app = test::init_service({
            let cors = Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header()
                .max_age(3600);
            App::new()
                .app_data(web::Data::new(context.clone()))
                .wrap(cors)
                .service(web::resource("/").route(web::post().to(json_rpc_impl::rpc_router)))
        })
            .await;

        let body = serde_json::json!(
            {
                "method": "latest_blocks",
                "params": [],
                "id": 1,
                "jsonrpc": "2.0"
            }
        );

        let req = test::TestRequest::post()
            .uri("/")
            .set_json(&body)
            .to_request();
        let resp: Response = test::call_and_read_body_json(&app, req).await;

        assert_eq!(resp.jsonrpc, "2.0".to_string());
        assert_eq!(resp.error.is_some(), false);
        assert_eq!(resp.id, Value::Number(Number::from(1)));
        assert!(resp.result["block_metas"].is_array());
    }

    #[actix_web::test]
    async fn test_blocks() {
        let context = init_context().await;

        let app = test::init_service({
            let cors = Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header()
                .max_age(3600);
            App::new()
                .app_data(web::Data::new(context.clone()))
                .wrap(cors)
                .service(web::resource("/").route(web::post().to(json_rpc_impl::rpc_router)))
        })
            .await;

        let body_latest = serde_json::json!(
            {
                "method": "latest_blocks",
                "params": [],
                "id": 1,
                "jsonrpc": "2.0"
            }
        );

        let req_latest = test::TestRequest::post()
            .uri("/")
            .set_json(&body_latest)
            .to_request();

        let resp_latest: Response = test::call_and_read_body_json(&app, req_latest).await;

        let hash = &resp_latest.result["block_metas"][0]["block_id"]["hash"];

        let body = serde_json::json!(
            {
                "method": "block",
                "params": [*hash],
                "id": 1,
                "jsonrpc": "2.0"
            }
        );

        let req = test::TestRequest::post()
            .uri("/")
            .set_json(&body)
            .to_request();
        let resp: Response = test::call_and_read_body_json(&app, req).await;

        assert_eq!(resp.jsonrpc, "2.0".to_string());
        assert_eq!(resp.error.is_some(), false);
        assert_eq!(resp.id, Value::Number(Number::from(1)));
        assert_eq!(resp.result["block_id"]["hash"], *hash);
    }

    #[actix_web::test]
    async fn test_broadcast() {
        let context = init_context().await;

        let app = test::init_service({
            let cors = Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header()
                .max_age(3600);
            App::new()
                .app_data(web::Data::new(context.clone()))
                .wrap(cors)
                .service(web::resource("/").route(web::post().to(json_rpc_impl::rpc_router)))
        })
            .await;

        let kp = db3_cmd::get_key_pair(false).unwrap();
        let signer = Db3Signer::new(kp);
        let kv = KvPair {
            key: format!("kkkkk_tt{}", 2).as_bytes().to_vec(),
            value: format!("vkalue_tt{}", 2).as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let mutation = Mutation {
            ns: "ns1".as_bytes().to_vec(),
            kv_pairs: vec![kv],
            nonce: 1110,
            chain_id: ChainId::MainNet.into(),
            chain_role: ChainRole::StorageShardChain.into(),
            gas_price: None,
            gas: 10,
        };
        let mut mbuf = BytesMut::with_capacity(1024 * 4);
        mutation.encode(&mut mbuf).unwrap();
        let mbuf = mbuf.freeze();

        let signature = signer.sign(mbuf.as_ref()).unwrap();
        let request = WriteRequest {
            signature,
            mutation: mbuf.as_ref().to_vec().to_owned(),
        };
        let mut buf = BytesMut::with_capacity(1024 * 4);
        request.encode(&mut buf).unwrap();
        let buf = buf.freeze();

        // encode request to base64
        let data = base64::encode(buf.as_ref());
        let base64_str = String::from_utf8_lossy(data.as_ref()).to_string();

        let body = serde_json::json!(
            {
                "method": "broadcast",
                "params": vec![base64_str],
                "id": 1,
                "jsonrpc": "2.0"
            }
        );

        let req = test::TestRequest::post()
            .uri("/")
            .set_json(&body)
            .to_request();
        let resp: Response = test::call_and_read_body_json(&app, req).await;

        assert_eq!(resp.jsonrpc, "2.0".to_string());
        assert_eq!(resp.error.is_some(), false);
        assert_eq!(resp.id, Value::Number(Number::from(1)));
        assert!(resp.result.is_string());
    }

    #[actix_web::test]
    async fn test_mutation() {
        let context = init_context().await;

        let app = test::init_service({
            let cors = Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header()
                .max_age(3600);
            App::new()
                .app_data(web::Data::new(context.clone()))
                .wrap(cors)
                .service(web::resource("/").route(web::post().to(json_rpc_impl::rpc_router)))
        })
            .await;


        let body = serde_json::json!(
            {
                "method": "mutation",
                "params": ["YligvOsZjwNXg4j+uiEZHGC9aSvEu2/Hk7Wr4bJpZC0="],
                "id": 1,
                "jsonrpc": "2.0"
            }
        );

        let req = test::TestRequest::post()
            .uri("/")
            .set_json(&body)
            .to_request();
        let resp: Response = test::call_and_read_body_json(&app, req).await;

        assert_eq!(resp.jsonrpc, "2.0".to_string());
        assert_eq!(resp.error.is_some(), false);
        assert_eq!(resp.id, Value::Number(Number::from(1)));
        assert!(resp.result.is_object());
    }
}
