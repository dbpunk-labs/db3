import { useEffect, useState } from "react";
import reactLogo from "./assets/react.svg";
import * as bip39 from "@scure/bip39";
import { wordlist } from "@scure/bip39/wordlists/english";
import { gen_key, sign } from "./pkg/db3_jsdk";
import db3_mutation_pb from "./pkg/db3_mutation_pb";
import db3_base_pb from "./pkg/db3_base_pb";
import db3_bill_pb from "./pkg/db3_bill_pb";
import { StorageNodeClient } from "./pkg/Db3_nodeServiceClientPb";
import "./App.css";

var jspb = require("google-protobuf");
function encodeUint8Array(text: string) {
	return new TextEncoder().encode(text);
}

function App() {
	useEffect(() => {
		const client = new StorageNodeClient("http://localhost:26659/");
		const nsUint8Array = new TextEncoder().encode("detwitter");
		const kv_pair = new db3_mutation_pb.KVPair({
			key: encodeUint8Array("name"),
			value: encodeUint8Array("test"),
			action: db3_mutation_pb.MutationAction.INSERTKV,
		});

		const mutationRequest = new db3_mutation_pb.Mutation({
			ns: nsUint8Array,
			kvPairs: [kv_pair],
			nonce: 1110,
			chainId: db3_base_pb.ChainId.MAINNET,
			chainRole: db3_base_pb.ChainRole.STORAGESHARDCHAIN,
			gasPrice: null,
			gas: 10,
		});
		const writeRequest = new db3_mutation_pb.WriteRequest();
		writeRequest.setMutation(mutationRequest.serializeBinary());
		const bill = new db3_bill_pb.Bill();
		bill.setBlockHeight(19595);

		const queryBillRequest = new db3_bill_pb.BillQueryRequest();
		queryBillRequest.setBlockHeight(19595);
		console.log(
			jspb.Message.bytesAsB64(queryBillRequest.serializeBinary()),
		);

		client.queryBill(bill, {}, (err: any, response: any) => {
			if (err) {
				console.error(err);
			} else {
				console.log(response);
			}
		});
		const mn = bip39.generateMnemonic(wordlist);
		bip39.mnemonicToSeed(mn, "password").then((seed: Uint8Array) => {
			const [pk, sk] = gen_key(seed);
			const signature = sign(new TextEncoder().encode("test"), sk);
			writeRequest.setSignature(signature);
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
