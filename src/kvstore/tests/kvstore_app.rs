//! Key/value store application integration tests.

mod kvstore_app_integration {
    use std::thread;

    use bytes::{Bytes, BytesMut};
    use db3_kvstore::KeyValueStoreApp;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::{KvPair, Mutation, WriteRequest};
    use fastcrypto::secp256k1::Secp256k1KeyPair;
    use fastcrypto::secp256k1::Secp256k1Signature;
    use fastcrypto::traits::KeyPair;
    use fastcrypto::traits::Signer;
    use hex;
    use prost::Message;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use tendermint_abci::{ClientBuilder, ServerBuilder};
    use tendermint_proto::abci::{RequestDeliverTx, RequestQuery};

    #[test]
    fn happy_path() {
        let mut rng = StdRng::from_seed([0; 32]);
        let kp = Secp256k1KeyPair::generate(&mut rng);
        let kv = KvPair {
            key: "k1".as_bytes().to_vec(),
            value: "value1".as_bytes().to_vec(),
        };
        let mutation = Mutation {
            ns: "my_twitter".as_bytes().to_vec(),
            kv_pairs: vec![kv],
            nonce: 1,
            chain_id: ChainId::MainNet.into(),
            chain_role: ChainRole::StorageShardChain.into(),
            gas_price: 1,
            start_gas: 2,
        };
        let mut buf = BytesMut::with_capacity(1024 * 4);
        mutation.encode(&mut buf);
        let buf = buf.freeze();
        let signature: Secp256k1Signature = kp.sign(buf.as_ref());
        let request = WriteRequest {
            signature: signature.as_ref().to_vec(),
            mutation: buf.as_ref().to_vec(),
            public_key: kp.public().as_ref().to_vec(),
        };
        let mut buf = BytesMut::with_capacity(1024 * 4);
        request.encode(&mut buf);
        let buf = buf.freeze();
        let mutation_encoded = hex::encode_upper(buf.as_ref());
        println!("{}", mutation_encoded);
        let (app, driver) = KeyValueStoreApp::new();
        let server = ServerBuilder::default().bind("127.0.0.1:0", app).unwrap();
        let server_addr = server.local_addr();
        thread::spawn(move || driver.run());
        thread::spawn(move || server.listen());
        let mut client = ClientBuilder::default().connect(server_addr).unwrap();
        client
            .deliver_tx(RequestDeliverTx {
                tx: mutation_encoded.into(),
            })
            .unwrap();
        client.commit().unwrap();
        let res = client
            .query(RequestQuery {
                data: "k1".as_bytes().to_owned(),
                path: "".to_string(),
                height: 0,
                prove: false,
            })
            .unwrap();
        assert_eq!(res.value, "value1".as_bytes().to_owned());
    }
}
