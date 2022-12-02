import db3_mutation_pb from "../pkg/db3_mutation_pb";
import type { KVPair } from "../pkg/db3_mutation_pb";
import db3_base_pb from "../pkg/db3_base_pb";
import db3_node_pb from "../pkg/db3_node_pb";
import { StorageNodeClient } from "../pkg/Db3_nodeServiceClientPb";
import * as jspb from 'google-protobuf';

export interface Mutation {
	ns: string;
	gasLimit: number;
	data: Record<string, any>;
}

export interface DB3_Instance {
	submitMutation(mutation: Mutation, signature?: Uint8Array | string): any;
}

export interface DB3_Options {
	mode: "DEV" | "PROD";
}

function encodeUint8Array(text: string) {
	return jspb.Message.bytesAsU8(text);
}

export class DB3 {
	private client: StorageNodeClient;
	constructor(node: string, options?: DB3_Options) {
		this.client = new StorageNodeClient(node, null, null);
	}

	async submitMutaition(
		mutation: Mutation,
		sign: (target: Uint8Array) => [Uint8Array, Uint8Array],
	) {
		const kvPairsList: KVPair[] = [];
		Object.keys(mutation.data).forEach((key: string) => {
			const kv_pair = new db3_mutation_pb.KVPair();
			kv_pair.setKey(encodeUint8Array(key));
			kv_pair.setValue(encodeUint8Array(mutation.data[key]));
			kv_pair.setAction(db3_mutation_pb.MutationAction.INSERTKV);
			kvPairsList.push(kv_pair);
		});

		const mutationObj = new db3_mutation_pb.Mutation();
		mutationObj.setNs(encodeUint8Array(mutation.ns));
		mutationObj.setKvPairsList(kvPairsList);
		mutationObj.setNonce(Date.now());
		mutationObj.setChainId(db3_base_pb.ChainId.MAINNET);
		mutationObj.setChainRole(db3_base_pb.ChainRole.STORAGESHARDCHAIN);
		mutationObj.setGasPrice();
		mutationObj.setGas(mutation.gasLimit);

		const [signature, public_key] = await sign(mutationObj.serializeBinary());

		const writeRequest = new db3_mutation_pb.WriteRequest();
		writeRequest.setMutation(mutationObj.serializeBinary());
		writeRequest.setSignature(signature);
        writeRequest.setPublicKey(public_key);

		const broadcastRequest = new db3_node_pb.BroadcastRequest();
		broadcastRequest.setBody(writeRequest.serializeBinary());

		const res = await this.client.broadcast(broadcastRequest, {});
		return res.toObject();
	}
}
