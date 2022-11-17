import { useEffect, useState } from "react";
import reactLogo from "./assets/react.svg";
import * as bip39 from "@scure/bip39";
import { wordlist } from "@scure/bip39/wordlists/english";
import { gen_key, sign } from "./pkg/db3_jsdk";
import db3_mutation_pb from "./pkg/db3_mutation_pb";
import db3_base_pb from "./pkg/db3_base_pb";
import db3_node_pb from "./pkg/db3_node_pb";
import { StorageNodeClient } from "./pkg/Db3_nodeServiceClientPb";
import rpc from "./api/rpc.api";
import "./App.css";

var jspb = require("google-protobuf");
function encodeUint8Array(text: string) {
	return new TextEncoder().encode(text);
}

function App() {
	useEffect(() => {
		const client = new StorageNodeClient("http://localhost:26659/");
		const kv_pair = new db3_mutation_pb.KVPair();
		kv_pair.setKey(encodeUint8Array("name"));
		kv_pair.setValue(encodeUint8Array("test"));
		kv_pair.setAction(db3_mutation_pb.MutationAction.INSERTKV);

		const mutation = new db3_mutation_pb.Mutation();
		mutation.setNs(encodeUint8Array("my_twitter"));
		mutation.setKvPairsList([kv_pair]);
		mutation.setNonce(22333);
		mutation.setChainId(db3_base_pb.ChainId.MAINNET);
		mutation.setChainRole(db3_base_pb.ChainRole.STORAGESHARDCHAIN);
		mutation.setGasPrice();
		mutation.setGas(10);

		const mn = bip39.generateMnemonic(wordlist);
		bip39.mnemonicToSeed(mn, "password").then((seed: Uint8Array) => {
			const [pk, sk] = gen_key(seed);
			const signature = sign(mutation.serializeBinary(), sk);
			const writeRequest = new db3_mutation_pb.WriteRequest();
			writeRequest.setMutation(mutation.serializeBinary());
			writeRequest.setSignature(signature);

			const broadcastRequest = new db3_node_pb.BroadcastRequest();
			broadcastRequest.setBody(writeRequest.serializeBinary());
			// rpc("broadcast", [
			// 	jspb.Message.bytesAsB64(broadcastRequest.serializeBinary()),
			// ]);
			client.broadcast(
				broadcastRequest,
				null,
				(err: any, response: any) => {
					if (err) {
						console.error(err);
					} else {
						console.log(response);
					}
				},
			);
		});
	}, []);

	return (
		<div className='App'>
			<div>
				<a href='https://vitejs.dev' target='_blank'>
					<img src='/vite.svg' className='logo' alt='Vite logo' />
				</a>
				<a href='https://reactjs.org' target='_blank'>
					<img
						src={reactLogo}
						className='logo react'
						alt='React logo'
					/>
				</a>
			</div>
			<h1>Vite + React</h1>
		</div>
	);
}

export default App;
