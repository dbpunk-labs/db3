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

	async submitRawMutation(
		                   ns:string,
						   kv_pairs:KVPair[],
		                   sign: (target: Uint8Array) => [Uint8Array, Uint8Array],	
	) {
		const mutation = new db3_mutation_pb.Mutation();
		mutation.setNs(encodeUint8Array(ns));
		mutation.setKvPairsList(kv_pairs);
		mutation.setNonce(Date.now());
		mutation.setChainId(db3_base_pb.ChainId.MAINNET);
		mutation.setChainRole(db3_base_pb.ChainRole.STORAGESHARDCHAIN);
		mutation.setGasPrice();
		mutation.setGas(100);
		const mbuffer = mutation.serializeBinary();
		const [signature, public_key] = await sign(mbuffer);
		const writeRequest = new db3_mutation_pb.WriteRequest();
		writeRequest.setMutation(mbuffer);
		writeRequest.setSignature(signature);
        writeRequest.setPublicKey(public_key);
		const broadcastRequest = new db3_node_pb.BroadcastRequest();
		broadcastRequest.setBody(writeRequest.serializeBinary());
		const res = await this.client.broadcast(broadcastRequest, {});
		return res.toObject();
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

		const mbuffer = mutationObj.serializeBinary();
		const [signature, public_key] = await sign(mbuffer);
		const writeRequest = new db3_mutation_pb.WriteRequest();
		writeRequest.setMutation(mbuffer);
		writeRequest.setSignature(signature);
        writeRequest.setPublicKey(public_key);

		const broadcastRequest = new db3_node_pb.BroadcastRequest();
		broadcastRequest.setBody(writeRequest.serializeBinary());

		const res = await this.client.broadcast(broadcastRequest, {});
		return res.toObject();
	}
}
