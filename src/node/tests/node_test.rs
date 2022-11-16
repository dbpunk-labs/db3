//! Key/value store application integration tests.

mod node_integration {
    use db3_base::get_address_from_pk;
    use db3_crypto::signer::Db3Signer;
    use db3_proto::db3_base_proto::{ChainId, ChainRole, UnitType, Units};
    use db3_proto::db3_mutation_proto::{KvPair, Mutation, MutationAction};
    use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
    use db3_proto::db3_node_proto::SessionStatus;
    use db3_sdk::mutation_sdk::MutationSDK;
    use db3_sdk::store_sdk::StoreSDK;
    use db3_session::session_manager::DEFAULT_SESSION_POOL_SIZE_LIMIT;
    use db3_session::session_manager::{DEFAULT_SESSION_PERIOD, DEFAULT_SESSION_QUERY_LIMIT};
    use fastcrypto::traits::KeyPair;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::{thread, time};
    use tendermint_rpc::HttpClient;
    use tonic::transport::Endpoint;

    fn get_mutation_sdk() -> MutationSDK {
        // create Mutation SDk
        let public_json_rpc_url = "http://127.0.0.1:26657";
        let kp = db3_cmd::get_key_pair(true).unwrap();
        // broadcast client
        let client = HttpClient::new(public_json_rpc_url).unwrap();
        let signer = Db3Signer::new(kp);
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

    #[tokio::test]
    async fn smoke_test() {
        // create Mutation SDk

        let sdk = get_mutation_sdk();
        let mut store_sdk = get_store_sdk();
        let ns = "test_ns";

        let mut session_id_1 = 0;
        // session restart
        {
            let kp = db3_cmd::get_key_pair(false).unwrap();
            let addr = get_address_from_pk(&kp.public().pubkey);
            let res = store_sdk.open_session(&addr).await;
            assert!(res.is_ok());
            let session_info = res.unwrap();
            session_id_1 = session_info.session_id;
            assert_eq!(session_info.max_query_limit, DEFAULT_SESSION_QUERY_LIMIT);
            assert_eq!(session_info.session_timeout_second, DEFAULT_SESSION_PERIOD);
        }

        // session info
        {
            let kp = db3_cmd::get_key_pair(false).unwrap();
            let addr = get_address_from_pk(&kp.public().pubkey);
            let info = store_sdk
                .get_session_info(&addr, session_id_1)
                .await
                .unwrap();
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
                .batch_get(ns.as_bytes(), vec!["k1".as_bytes().to_vec()], session_id_1)
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
            let kp = db3_cmd::get_key_pair(false).unwrap();
            let addr = get_address_from_pk(&kp.public().pubkey);
            let info = store_sdk
                .get_session_info(&addr, session_id_1)
                .await
                .unwrap();
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
                    .batch_get(ns.as_bytes(), vec!["k1".as_bytes().to_vec()], session_id_1)
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
                .batch_get(ns.as_bytes(), vec!["k1".as_bytes().to_vec()], session_id_1)
                .await;
            assert!(result.is_err());
            assert_eq!(
                result.err().unwrap().message(),
                "Fail to query in this session. Please restart query session"
            );
        }
        {
            let kp = db3_cmd::get_key_pair(false).unwrap();
            let addr = get_address_from_pk(&kp.public().pubkey);
            let info = store_sdk
                .get_session_info(&addr, session_id_1)
                .await
                .unwrap();
            assert_eq!(
                SessionStatus::from_i32(info.status).unwrap(),
                SessionStatus::Blocked
            );
            assert_eq!(info.query_count, DEFAULT_SESSION_QUERY_LIMIT);
        }

        // open another session 2
        let mut session_id_2 = 0;
        {
            let kp = db3_cmd::get_key_pair(false).unwrap();
            let addr = get_address_from_pk(&kp.public().pubkey);
            let res = store_sdk.open_session(&addr).await;
            assert!(res.is_ok());
            let session_info = res.unwrap();
            // verify session id increase 1
            assert_eq!(session_info.session_id, session_id_1 + 1);
            assert_eq!(session_info.max_query_limit, DEFAULT_SESSION_QUERY_LIMIT);

            // update current session id
            session_id_2 = session_info.session_id;
        }
        {
            let kp = db3_cmd::get_key_pair(false).unwrap();
            let addr = get_address_from_pk(&kp.public().pubkey);
            let info = store_sdk
                .get_session_info(&addr, session_id_2)
                .await
                .unwrap();
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
                thread::sleep(time::Duration::from_secs(2));
            }
            {
                let kp = db3_cmd::get_key_pair(false).unwrap();
                let addr = get_address_from_pk(&kp.public().pubkey);
                let info = store_sdk
                    .get_session_info(&addr, session_id_2)
                    .await
                    .unwrap();
                assert_eq!(
                    SessionStatus::from_i32(info.status).unwrap(),
                    SessionStatus::Running
                );
                assert_eq!(info.query_count, 0);
            }
            {
                let result = store_sdk
                    .batch_get(ns.as_bytes(), vec!["k1".as_bytes().to_vec()], session_id_2)
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
            let kp = db3_cmd::get_key_pair(false).unwrap();
            let addr = get_address_from_pk(&kp.public().pubkey);
            assert_eq!(
                store_sdk.close_session(session_id_1).await.unwrap(),
                session_id_1
            );
        }
        // close session 2
        {
            let kp = db3_cmd::get_key_pair(false).unwrap();
            let addr = get_address_from_pk(&kp.public().pubkey);
            assert_eq!(
                store_sdk.close_session(session_id_2).await.unwrap(),
                session_id_2
            );
        }
        // close session 3
        {
            let kp = db3_cmd::get_key_pair(false).unwrap();
            let addr = get_address_from_pk(&kp.public().pubkey);
            let res = store_sdk.close_session(session_id_2 + 100).await;
            assert!(res.is_err());
        }
    }
}
