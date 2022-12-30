use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use db3_base::{get_a_random_nonce, get_a_static_keypair, get_address_from_pk};
use db3_crypto::signer::Db3Signer;
use db3_proto::db3_base_proto::{ChainId, ChainRole};
use db3_proto::db3_mutation_proto::KvPair;
use db3_proto::db3_mutation_proto::{Mutation, MutationAction};
use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
use db3_sdk::mutation_sdk::MutationSDK;
use db3_sdk::store_sdk::StoreSDK;
use db3_session::session_manager::DEFAULT_SESSION_QUERY_LIMIT;
use std::sync::Arc;
use std::time;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};
use tonic::transport::Endpoint;

// Here we have an async function to benchmark
// run batch_get_key 1000 during a session
async fn run_batch_get_key(keys: &Vec<Vec<u8>>) {
    let nonce = get_a_random_nonce();
    let ep = "http://127.0.0.1:26659";
    let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
    let channel = rpc_endpoint.connect_lazy();
    let client = Arc::new(StorageNodeClient::new(channel));
    let ns_vec = "my_twitter".as_bytes().to_vec();
    let kp = get_a_static_keypair();
    let addr = get_address_from_pk(&kp.public);
    let signer = Db3Signer::new(kp);
    let mut sdk = StoreSDK::new(client, signer);
    let res = sdk.open_session().await;
    assert!(res.is_ok());
    let session_info = res.unwrap();
    for i in 0..DEFAULT_SESSION_QUERY_LIMIT {
        if let Ok(Some(values)) = sdk
            .batch_get(&ns_vec, keys.clone(), &session_info.session_token)
            .await
        {
            assert_eq!(values.values.len(), keys.len());
        } else {
            println!("fail to query keys");
        }
    }
    let res = sdk.close_session(&session_info.session_token).await;
}

async fn init_kv_store(kv_size: i32) {
    let nonce = get_a_random_nonce();

    let ep = "http://127.0.0.1:26659";
    let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
    let channel = rpc_endpoint.connect_lazy();
    let client = Arc::new(StorageNodeClient::new(channel));
    let mclient = client.clone();
    let sclient = client.clone();

    let ns_vec = "my_twitter".as_bytes().to_vec();
    let kp = get_a_static_keypair();
    let signer = Db3Signer::new(kp);
    let msdk = MutationSDK::new(mclient, signer);
    let mut kv_pairs = vec![];
    for i in 0..kv_size {
        kv_pairs.push(KvPair {
            key: format!("bm_key_{}", i).as_bytes().to_vec(),
            value: format!("bm_value_{}", i).as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        });
    }
    let mutation = Mutation {
        ns: ns_vec.clone(),
        kv_pairs,
        nonce,
        chain_id: ChainId::MainNet.into(),
        chain_role: ChainRole::StorageShardChain.into(),
        gas_price: None,
        gas: 10,
    };
    println!("start submit mutation");
    let result = msdk.submit_mutation(&mutation).await;
    assert!(result.is_ok(), "{}", result.err().unwrap());
    let two_sec = Duration::from_millis(2000);
    println!("done submit mutation");
    sleep(two_sec).await;
    println!("wake up after submit");
}
fn criterion_benchmark(c: &mut Criterion) {
    println!("criterion_benchmark....");

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        init_kv_store(100).await;
    });
    let mut group = c.benchmark_group("batch get key 1000 requests per session");
    for key_size in [1, 10, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("keys size", key_size),
            key_size,
            |b, &key_size| {
                let mut keys = vec![];
                for i in 0..key_size {
                    keys.push(format!("bm_key_{}", i).as_bytes().to_vec());
                }
                b.to_async(&rt).iter(|| async {
                    run_batch_get_key(&keys).await;
                })
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
