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
    use tendermint_rpc::HttpClient;

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
}
