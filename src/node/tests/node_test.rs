//! Key/value store application integration tests.

mod node_integration {
    use bytes::BytesMut;
    use db3_crypto::signer::Db3Signer;
    use db3_proto::db3_base_proto::{ChainId, ChainRole, UnitType, Units};
    use db3_proto::db3_mutation_proto::WriteRequest;
    use db3_proto::db3_mutation_proto::{KvPair, Mutation, MutationAction};
    use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
    use db3_proto::db3_node_proto::SessionStatus;
    use db3_sdk::mutation_sdk::MutationSDK;
    use db3_sdk::store_sdk::StoreSDK;
    use db3_session::session_manager::{DEFAULT_SESSION_PERIOD, DEFAULT_SESSION_QUERY_LIMIT};
    use prost::Message;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::{thread, time};
    use subtle_encoding::base64;
    use tonic::transport::Endpoint;
    fn get_mutation_sdk() -> MutationSDK {
        let public_grpc_url = "http://127.0.0.1:26659";
        // create storage node sdk
        let kp = db3_cmd::get_key_pair(false).unwrap();
        let signer = Db3Signer::new(kp);
        let rpc_endpoint = Endpoint::new(public_grpc_url).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        // broadcast client
        let sdk = MutationSDK::new(client, signer);
        sdk
    }

    fn get_store_sdk() -> StoreSDK {
        let public_grpc_url = "http://127.0.0.1:26659";
        // create storage node sdk
        let kp = db3_cmd::get_key_pair(false).unwrap();
        let signer = Db3Signer::new(kp);
        let rpc_endpoint = Endpoint::new(public_grpc_url).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        StoreSDK::new(client, signer)
    }

    fn current_seconds() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(_) => 0,
        }
    }

    #[actix_web::test]
    async fn json_rpc_smoke_test() {
        let json_rpc_url = "http://127.0.0.1:26670";
        let client = awc::Client::default();
        let kp = db3_cmd::get_key_pair(false).unwrap();
        let signer = Db3Signer::new(kp);
        let kv = KvPair {
            key: format!("kkkkk_tt{}", 1).as_bytes().to_vec(),
            value: format!("vkalue_tt{}", 1).as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let mutation = Mutation {
            ns: "my_twitter".as_bytes().to_vec(),
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

    #[tokio::test]
    async fn smoke_test() {
        // create Mutation SDk

        let sdk = get_mutation_sdk();
        let mut store_sdk = get_store_sdk();
        let ns = "test_ns";

        let mut session_id_1: String = String::new();
        // session restart
        {
            let res = store_sdk.open_session().await;
            assert!(res.is_ok());
            let session_info = res.unwrap();
            session_id_1 = session_info.session_token.clone();
            assert_eq!(session_info.max_query_limit, DEFAULT_SESSION_QUERY_LIMIT);
            assert_eq!(session_info.session_timeout_second, DEFAULT_SESSION_PERIOD);
        }

        // session info
        {
            let info = store_sdk.get_session_info(&session_id_1).await.unwrap();
            assert_eq!(
                SessionStatus::from_i32(info.status).unwrap(),
                SessionStatus::Running
            );
            assert_eq!(info.query_count, 0);
        }

        // put test_ns k1 v1 k2 v2 k3 v4
        {
            let pairs = vec![
                KvPair {
                    key: "k1".as_bytes().to_vec(),
                    value: "v1".as_bytes().to_vec(),
                    action: MutationAction::InsertKv.into(),
                },
                KvPair {
                    key: "k2".as_bytes().to_vec(),
                    value: "v2".as_bytes().to_vec(),
                    action: MutationAction::InsertKv.into(),
                },
                KvPair {
                    key: "k3".as_bytes().to_vec(),
                    value: "v3".as_bytes().to_vec(),
                    action: MutationAction::InsertKv.into(),
                },
            ];
            let mutation = Mutation {
                ns: ns.as_bytes().to_vec(),
                kv_pairs: pairs.to_owned(),
                nonce: current_seconds(),
                gas_price: Some(Units {
                    utype: UnitType::Tai.into(),
                    amount: 100,
                }),
                gas: 100,
                chain_id: ChainId::DevNet.into(),
                chain_role: ChainRole::StorageShardChain.into(),
            };

            assert!(sdk.submit_mutation(&mutation).await.is_ok());
            thread::sleep(time::Duration::from_secs(2));
        }

        // get ns_test k1
        {
            if let Ok(Some(values)) = store_sdk
                .batch_get(ns.as_bytes(), vec!["k1".as_bytes().to_vec()], &session_id_1)
                .await
            {
                assert_eq!(values.values.len(), 1);
                assert_eq!(values.values[0].key, "k1".as_bytes());
                assert_eq!(values.values[0].value, "v1".as_bytes());
            } else {
                assert!(false);
            }
        }

        // session info
        {
            let info = store_sdk.get_session_info(&session_id_1).await.unwrap();
            assert_eq!(
                SessionStatus::from_i32(info.status).unwrap(),
                SessionStatus::Running.into()
            );
            assert_eq!(info.query_count, 1);
        }

        // query times == DEFAULT_SESSION_QUERY_LIMIT
        {
            for _ in 0..DEFAULT_SESSION_QUERY_LIMIT - 1 {
                if let Ok(Some(values)) = store_sdk
                    .batch_get(ns.as_bytes(), vec!["k1".as_bytes().to_vec()], &session_id_1)
                    .await
                {
                    assert_eq!(values.values.len(), 1);
                    assert_eq!(values.values[0].key, "k1".as_bytes());
                    assert_eq!(values.values[0].value, "v1".as_bytes());
                } else {
                    assert!(false)
                }
            }
        }
        // session blocked because query times >= limit
        {
            let result = store_sdk
                .batch_get(ns.as_bytes(), vec!["k1".as_bytes().to_vec()], &session_id_1)
                .await;
            assert!(result.is_err());
            assert_eq!(
                result.err().unwrap().message(),
                "Fail to query in this session. Please restart query session"
            );
        }
        {
            let info = store_sdk.get_session_info(&session_id_1).await.unwrap();
            assert_eq!(
                SessionStatus::from_i32(info.status).unwrap(),
                SessionStatus::Blocked
            );
            assert_eq!(info.query_count, DEFAULT_SESSION_QUERY_LIMIT);
        }

        // open another session 2
        let mut session_id_2 = String::new();
        {
            let res = store_sdk.open_session().await;
            assert!(res.is_ok());
            let session_info = res.unwrap();
            // verify session id increase 1
            assert_ne!(session_info.session_token, session_id_1.clone());
            assert_eq!(session_info.max_query_limit, DEFAULT_SESSION_QUERY_LIMIT);

            // update current session id
            session_id_2 = session_info.session_token;
        }
        {
            let info = store_sdk.get_session_info(&session_id_2).await.unwrap();
            assert_eq!(
                SessionStatus::from_i32(info.status).unwrap(),
                SessionStatus::Running
            );
            assert_eq!(info.query_count, 0);
        }
        // delete k1
        {
            {
                let pairs = vec![KvPair {
                    key: "k1".as_bytes().to_vec(),
                    value: vec![],
                    action: MutationAction::DeleteKv.into(),
                }];
                let mutation = Mutation {
                    ns: ns.as_bytes().to_vec(),
                    kv_pairs: pairs.to_owned(),
                    nonce: current_seconds(),
                    gas_price: Some(Units {
                        utype: UnitType::Tai.into(),
                        amount: 100,
                    }),
                    gas: 100,
                    chain_id: ChainId::DevNet.into(),
                    chain_role: ChainRole::StorageShardChain.into(),
                };
                assert!(sdk.submit_mutation(&mutation).await.is_ok());
                thread::sleep(time::Duration::from_secs(4));
            }
            {
                let info = store_sdk.get_session_info(&session_id_2).await.unwrap();
                assert_eq!(
                    SessionStatus::from_i32(info.status).unwrap(),
                    SessionStatus::Running
                );
                assert_eq!(info.query_count, 0);
            }
            {
                let result = store_sdk
                    .batch_get(ns.as_bytes(), vec!["k1".as_bytes().to_vec()], &session_id_2)
                    .await;
                assert!(result.is_ok());
                if let Ok(Some(values)) = result {
                    assert_eq!(values.values.len(), 0);
                } else {
                    assert!(false);
                }
            }
        }

        // close session 1
        {
            assert!(store_sdk.close_session(&session_id_1).await.is_ok())
        }
        // close session 2
        {
            assert!(store_sdk.close_session(&session_id_2).await.is_ok());
        }
        // close session 3
        {
            let res = store_sdk
                .close_session(&"UNKNOW_SESSION_TOKEN".to_string())
                .await;
            assert!(res.is_err());
        }
    }
}
