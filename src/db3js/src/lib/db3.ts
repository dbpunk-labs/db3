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

export interface BatchGetKeyRequest {
	ns: string;
	keyList: string[];
	sessionToken: string;
}

export interface QuerySession {
	sessionInfo: db3_node_pb.QuerySessionInfo.AsObject;
	sessionToken: string;
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
	private sessionToken?: string;
	private querySessionInfo?: db3_node_pb.QuerySessionInfo.AsObject; 
	constructor(node: string, options?: DB3_Options) {
		this.client = new StorageNodeClient(node, null, null);
	}

	async submitRawMutation(
		                   ns:string,
						   kv_pairs:KVPair[],
		                   sign: (target: Uint8Array) => [Uint8Array, Uint8Array],	
						   nonce?:number
	) {
		const mutation = new db3_mutation_pb.Mutation();
		mutation.setNs(encodeUint8Array(ns));
		mutation.setKvPairsList(kv_pairs);
		if (typeof nonce !== 'undefined') {
			mutation.setNonce(nonce);
		}else {
			mutation.setNonce(Date.now());
		}
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
    sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>,
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
		if (typeof nonce !== 'undefined') {
			mutationObj.setNonce(nonce);
		}else {
			mutationObj.setNonce(Date.now());
		}
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
		try {
			const res = await this.client.broadcast(broadcastRequest, {});
			return res.toObject();
		} catch (error) {
			throw error;
		}
		
	}
	async openQuerySession(sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>) {
		if (this.querySessionInfo) {
			throw new Error("The current db3js instance has already opened a session, so do not open it repeatedly");
		}
		const sessionRequest = new db3_node_pb.OpenSessionRequest();
		const header = window.crypto.getRandomValues(new Uint8Array(32));
		// const header = encodeUint8Array('Header');
		const [signature, public_key] = await sign(header);
		sessionRequest.setHeader(header);
		sessionRequest.setSignature(signature);
		sessionRequest.setPublicKey(public_key);
		try {
			const res = await this.client.openQuerySession(sessionRequest, {});
			this.sessionToken = res.getSessionToken();
			this.querySessionInfo = res.getQuerySessionInfo()?.toObject();
			return res.toObject();
		} catch (error) {
			throw error;
		}
		
	}
	async getKey(request: BatchGetKeyRequest) {
		const getKeyRequest = new db3_node_pb.GetKeyRequest();
		const batchGetKeyRequest = new db3_node_pb.BatchGetKey();
		batchGetKeyRequest.setNs(request.ns);
		batchGetKeyRequest.setKeysList(request.keyList);
		batchGetKeyRequest.setSessionToken(request.sessionToken);
		getKeyRequest.setBatchGet(batchGetKeyRequest)
		try {
			const res = await this.client.getKey(getKeyRequest, {});
			return res.toObject();
		} catch (error) {
			throw error;
		}
		
	}
	async closeQuerySession(querySessionInfoAsObject: db3_node_pb.QuerySessionInfo.AsObject, sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>){
		if (!this.sessionToken) {
			throw new Error('SessionToken is not defined');
		}
		const querySessionInfo = new db3_node_pb.QuerySessionInfo();
		const {id, startTime, status, queryCount} = querySessionInfoAsObject;
		querySessionInfo.setId(id);
		querySessionInfo.setStatus(status);
		querySessionInfo.setStartTime(startTime);
		querySessionInfo.setQueryCount(queryCount);

		const payload = new db3_node_pb.CloseSessionPayload();
		payload.setSessionInfo(querySessionInfo);
		payload.setSessionToken(this.sessionToken);

		const payloadU8 = payload.serializeBinary();
		const [signature, public_key] = await sign(payloadU8);

		const closeQuerySessionRequest = new db3_node_pb.CloseSessionRequest();
		closeQuerySessionRequest.setPayload(payloadU8);
		closeQuerySessionRequest.setSignature(signature);
		closeQuerySessionRequest.setPublicKey(public_key);
		try {
			const res = await this.client.closeQuerySession(closeQuerySessionRequest, {});
			this.querySessionInfo = undefined;
			return res.toObject();
		} catch (error) {
			throw error;
		}
		
	}
}
