use criterion::{black_box, criterion_group, criterion_main, Criterion};
use bytes::BytesMut;
use chrono::Utc;
use db3_base::{get_a_random_nonce, get_a_static_keypair, get_address_from_pk};
use db3_proto::db3_base_proto::{ChainId, ChainRole};
use db3_proto::db3_mutation_proto::KvPair;
use db3_proto::db3_mutation_proto::{Mutation, MutationAction};
use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
use std::sync::Arc;
use std::time;
use tonic::transport::Endpoint;
use uuid::Uuid;
use db3_crypto::signer::Db3Signer;
use db3_sdk::mutation_sdk::MutationSDK;
use db3_sdk::store_sdk::StoreSDK;
use db3_session::session_manager::DEFAULT_SESSION_QUERY_LIMIT;
use tonic::Code;
// This is a struct that tells Criterion.rs to use the "futures" crate's current-thread executor
use criterion::async_executor::FuturesExecutor;
use std::process::exit;
use std::cell::RefCell;
use std::rc;
// This is a struct that tells Criterion.rs to use the "futures" crate's current-thread executor
use std::rc::Rc;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};
use futures_lite::future::block_on;

// Here we have an async function to benchmark
async fn run_batch_get() {
    println!("start run batch get");
    let nonce = get_a_random_nonce();
    let ep = "http://127.0.0.1:26659";
    let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
    let channel = rpc_endpoint.connect_lazy();
    let client = Arc::new(StorageNodeClient::new(channel));
    let key_vec = format!("kkkkk_tt{}", 10).as_bytes().to_vec();
    let value_vec = format!("vkalue_tt{}", 10).as_bytes().to_vec();
    let ns_vec = "my_twitter".as_bytes().to_vec();

    let kp = get_a_static_keypair();
    let addr = get_address_from_pk(&kp.public);
    let signer = Db3Signer::new(kp);
    let mut sdk = StoreSDK::new(client, signer);
    let res = sdk.open_session().await;
    assert!(res.is_ok());
    let session_info = res.unwrap();
    println!("open session with token {}", &session_info.session_token);
    for i in 0..DEFAULT_SESSION_QUERY_LIMIT {
        // if i % 10 == 0 {
        // }
        println!("process {} queries", i);
        if let Ok(Some(values)) = sdk
            .batch_get(&ns_vec, vec![key_vec.clone()], &session_info.session_token)
            .await
        {
            assert_eq!(values.values.len(), 1);
        } else {
            println!("fail to query keys");
        }
        println!("done process {} queries", i);
    }
    let res = sdk.close_session(&session_info.session_token).await;
    println!("close session ");
}

async fn foo() {
    // ...
}
async fn init_kv_store() {
    let nonce = get_a_random_nonce();

    let ep = "http://127.0.0.1:26659";
    let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
    let channel = rpc_endpoint.connect_lazy();
    let client = Arc::new(StorageNodeClient::new(channel));
    let mclient = client.clone();
    let sclient = client.clone();
    let key_vec = format!("kkkkk_tt{}", 10).as_bytes().to_vec();
    let value_vec = format!("vkalue_tt{}", 10).as_bytes().to_vec();
    let ns_vec = "my_twitter".as_bytes().to_vec();
    let kp = get_a_static_keypair();
    let signer = Db3Signer::new(kp);
    let msdk = MutationSDK::new(mclient, signer);
    let kv = KvPair {
        key: key_vec.clone(),
        value: value_vec.clone(),
        action: MutationAction::InsertKv.into(),
    };
    let mutation = Mutation {
        ns: ns_vec.clone(),
        kv_pairs: vec![kv],
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
fn criterion_benchmark_compile_fail(c: &mut Criterion) {
    println!("criterion_benchmark....");

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        init_kv_store().await;
    });
    c.bench_function("batch get bench", move |b| {
            b.to_async(FuturesExecutor)
                // ., |_| async move { run_batch_get().await;});
                .iter(|| run_batch_get())
    });
}
fn criterion_benchmark(c: &mut Criterion) {
    println!("criterion_benchmark....");

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        init_kv_store().await;
    });
    c.bench_function("batch get bench", move |b| {
        rt.block_on(async {
            b.to_async(FuturesExecutor).iter(|| async move { run_batch_get().await; });
        });
    });
}
// Example: https://github.com/bheisler/criterion.rs/tree/master/benches/benchmarks

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);