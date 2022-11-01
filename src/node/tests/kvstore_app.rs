//! Key/value store application integration tests.

mod kvstore_app_integration {
    use std::thread;

    use bytes::{Bytes, BytesMut};
    use db3_base::get_address_from_pk;
    use db3_kvstore::KeyValueStoreApp;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::{KvPair, Mutation, MutationAction, WriteRequest};
    use fastcrypto::secp256k1::Secp256k1KeyPair;
    use fastcrypto::secp256k1::Secp256k1Signature;
    use fastcrypto::traits::KeyPair;
    use fastcrypto::traits::Signer;
    use hex;
    use merk::Merk;
    use prost::Message;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use tempdir::TempDir;
    use tendermint_abci::{ClientBuilder, ServerBuilder};
    use tendermint_proto::abci::{RequestDeliverTx, RequestQuery};
    #[test]
    fn test_commit() {
        let tmp_dir_path = TempDir::new("test_commit").expect("create temp dir");
        let mut rng = StdRng::from_seed([0; 32]);
        let mut merk = Merk::open(tmp_dir_path).unwrap();
        let kp = Secp256k1KeyPair::generate(&mut rng);
        let kv = KvPair {
            key: "k1".as_bytes().to_vec(),
            value: "value1".as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let mutation = Mutation {
            ns: "my_twitter".as_bytes().to_vec(),
            kv_pairs: vec![kv],
            nonce: 1,
            chain_id: ChainId::MainNet.into(),
            chain_role: ChainRole::StorageShardChain.into(),
            gas_price: None,
            gas: 1,
        };
        let mut buf = BytesMut::with_capacity(1024 * 4);
        mutation.encode(&mut buf);
        let buf = buf.freeze();
        let signature: Secp256k1Signature = kp.sign(buf.as_ref());
        let request = WriteRequest {
            signature: signature.as_ref().to_vec(),
            mutation: buf.as_ref().to_vec(),
        };
        let addr = get_address_from_pk(&kp.name.pubkey);
        let mut buf = BytesMut::with_capacity(1024 * 4);
        request.encode(&mut buf);
        let buf = buf.freeze();
        let mutation_encoded = hex::encode_upper(buf.as_ref());
        let addr_str = hex::encode(addr.as_bytes());
        println!("addr {}", addr_str);
        let app = KeyValueStoreApp::new(merk);
        let server = ServerBuilder::default().bind("127.0.0.1:0", app).unwrap();
        let server_addr = server.local_addr();
        thread::spawn(move || server.listen());
        let mut client = ClientBuilder::default().connect(server_addr).unwrap();
        client
            .deliver_tx(RequestDeliverTx {
                tx: mutation_encoded.into(),
            })
            .unwrap();
        client.commit().unwrap();
        let query = RequestQuery {
            data: addr_str.as_bytes().to_vec(),
            path: "account".to_string(),
            height: 0,
            prove: false,
        };
        let response = client.query(query).unwrap();
        assert_eq!(0, response.code);
        let json_data = String::from_utf8(response.value).unwrap();
        println!("{}", json_data);
    }
}
