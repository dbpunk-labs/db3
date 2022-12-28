// @ts-nocheck
import {WriteRequest, PayloadType, KVPair, Mutation, MutationAction} from '../pkg/db3_mutation'
import {Erc20Token, Price} from '../pkg/db3_base'
import {ChainId, ChainRole} from '../pkg/db3_base'
import {GetRangeRequest, RangeKey, Range, CloseSessionRequest, BatchGetKey, GetKeyRequest, BroadcastRequest, GetNamespaceRequest, OpenSessionRequest, GetAccountRequest} from '../pkg/db3_node'
import {QueryPrice, Namespace} from '../pkg/db3_namespace'
import {CloseSessionPayload, QuerySessionInfo, OpenSessionPayload} from '../pkg/db3_session'
import {GrpcWebFetchTransport, GrpcWebOptions} from '@protobuf-ts/grpcweb-transport';
import { StorageNodeClient } from '../pkg/db3_node.client'
import * as jspb from 'google-protobuf'

export interface Mutation {
    ns: string
    gasLimit: number
    data: Record<string, any>
}

export interface NsSimpleDesc {
    name: string,
    desc: string,
    erc20Token: string,
    price: number,
    queryCount: number,
}

export interface BatchGetKeyRequest {
    ns: string
    keyList: string[] | Uint8Array[]
}

export interface QuerySession {
    sessionInfo: QuerySessionInfo.AsObject
    sessionToken: string
}

export interface DB3_Instance {
    submitMutation(mutation: Mutation, signature?: Uint8Array | string): any
}

export interface DB3_Options {
    mode: 'DEV' | 'PROD'
}

function encodeUint8Array(text: string) {
    return jspb.Message.bytesAsU8(text)
}

export class DB3 {

    private client: StorageNodeClient
    public sessionToken?: string
    private querySessionInfo?: QuerySessionInfo
    constructor(node: string, options?: DB3_Options) {
	    const goptions:GrpcWebOptions =  {
		baseUrl: node,
		deadline: Date.now() + 2000,
		format: 'binary',

		// simple example for how to add auth headers to each request
		// see `RpcInterceptor` for documentation
		interceptors: [
		],
		// you can set global request headers here
		meta: {}
	   }
       const transport = new GrpcWebFetchTransport(goptions)
       this.client = new StorageNodeClient(transport)
    }

    async createSimpleNs(
        desc:NsSimpleDesc,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>,
        nonce?: number
    ) {
        const token:Erc20Token = {
            symbal:desc.erc20Token,
            units: [desc.erc20Token],
            scalar: ["1"]
        }

        const priceProto:Price = {
            amount: desc.price,
            unit:desc.erc20Token,
            token:token
        }

        const queryPrice:QueryPrice = {
            price: priceProto,
            queryCount: desc.queryCount
        }

        const namespaceProto:Namespace = {
            name: desc.name,
            price: queryPrice,
            ts: Date.now(),
            description: desc.desc
        }

        return await this.createNs(namespaceProto, sign, nonce)
    }

    async createNs(
        ns: Namespace,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>,
        nonce?: number
    ) {
        const mbuffer =Namespace.toBinary(ns)
        const [signature, public_key] = await sign(mbuffer)
        const writeRequest:WriteRequest = {
            payload: mbuffer,
            signature: signature,
            publicKey: public_key,
            payloadType:PayloadType.NamespacePayload
        }
        const broadcastRequest:BroadcastRequest = {
            body: WriteRequest.toBinary(writeRequest)
        }
        const call = this.client.broadcast(broadcastRequest)
        const response = await call.response
        return response
    }

    async getNsList(
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        const token = await this.keepSession(sign)
        const getNsListRequest:GetNamespaceRequest = {
            sessionToken: token!
        }
        const res = await this.client.getNamespace(getNsListRequest, {})
        const count = this.querySessionInfo!.getQueryCount() + 1
        return res.toObject()
    }

    async submitRawMutation(
        ns: string,
        kv_pairs: KVPair[],
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>,
        nonce?: number
    ) {
        const mutation:Mutation = {
            ns: encodeUint8Array(ns),
            kvPairs: kv_pairs,
            nonce: Date.now(),
            chainId: ChainId.MainNet,
            chainRole: ChainRole.StorageShardChain,
            gasPrice:null,
            gas:"100"
        }
        const mbuffer = Mutation.toBinary(mutation)
        const [signature, public_key] = await sign(mbuffer)
        const writeRequest:WriteRequest = {
            payload: mbuffer,
            signature: signature,
            publicKey: public_key,
            payloadType:PayloadType.MutationPayload
        }
        const broadcastRequest :BroadcastRequest = {
            body: WriteRequest.toBinary(writeRequest)
        }
        const call = this.client.broadcast(broadcastRequest)
        const response = await call.response
        console.log("response", response)
        return response
    }

    async submitMutaition(
        mutation: Mutation,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        const kvPairsList: KVPair[] = []
        Object.keys(mutation.data).forEach((key: string) => {
            const kv_pair = new KVPair()
            kv_pair.setKey(encodeUint8Array(key))
            kv_pair.setValue(encodeUint8Array(mutation.data[key]))
            kv_pair.setAction(MutationAction.INSERTKV)
            kvPairsList.push(kv_pair)
        })
        const mutationObj = new Mutation()
        mutationObj.setNs(encodeUint8Array(mutation.ns))
        mutationObj.setKvPairsList(kvPairsList)
        mutationObj.setNonce(Date.now())
        mutationObj.setChainId(ChainId.MAINNET)
        mutationObj.setChainRole(ChainRole.STORAGESHARDCHAIN)
        mutationObj.setGasPrice()
        mutationObj.setGas(mutation.gasLimit)

        const mbuffer = mutationObj.serializeBinary()
        const [signature, public_key] = await sign(mbuffer)
        const writeRequest = new WriteRequest()
        writeRequest.setPayload(mbuffer)
        writeRequest.setSignature(signature)
        writeRequest.setPublicKey(public_key)
        writeRequest.setPayloadType(PayloadType.MUTATIONPAYLOAD)
        const broadcastRequest = new BroadcastRequest()
        broadcastRequest.setBody(writeRequest.serializeBinary())
        try {
            const res = await this.client.broadcast(broadcastRequest, {})
            return res.toObject()
        } catch (error) {
            throw error
        }
    }

    async keepSession(
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        if (!this.querySessionInfo) {
            // try to open session
            await this.openQuerySession(sign)
        }
        if (this.querySessionInfo!.getQueryCount() > 1000) {
            await this.closeQuerySession(sign)
            await this.openQuerySession(sign)
        }
        return this.sessionToken
    }

    async openQuerySession(
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        if (this.querySessionInfo) {
            return {}
        }


        const sessionRequest = new OpenSessionRequest()
        const header = window.crypto.getRandomValues(new Uint8Array(32))
        const payload = new OpenSessionPayload()
        payload.setHeader(header.toString())
        payload.setStartTime(Math.floor(Date.now() / 1000))
        const payloadU8 = payload.serializeBinary()
        const [signature, public_key] = await sign(payloadU8)
        sessionRequest.setPayload(payloadU8)
        sessionRequest.setSignature(signature)
        sessionRequest.setPublicKey(public_key)

        try {
            const res = await this.client.openQuerySession(sessionRequest, {})
            this.sessionToken = res.getSessionToken()
            this.querySessionInfo = res.getQuerySessionInfo()
            return res.toObject()
        } catch (error) {
            throw error
        }
    }

    async getAccount(address: string) {
        const getAccountRequest = new GetAccountRequest()
        getAccountRequest.setAddr(address)
        try {
            const response = await this.client.getAccount(getAccountRequest, {})
            return response.toObject()
        } catch (error) {
            throw error
        }
    }

    async getKey(batchGetRequest: BatchGetKeyRequest) {
        if (!this.sessionToken) {
            throw new Error('SessionToken is not defined')
        }
        const getKeyRequest = new GetKeyRequest()
        const batchGetKey = new BatchGetKey()
        batchGetKey.setNs(batchGetRequest.ns)
        batchGetKey.setKeysList(batchGetRequest.keyList)
        //todo handle null session token
        batchGetKey.setSessionToken(this.sessionToken)
        getKeyRequest.setBatchGet(batchGetKey)
        try {
            const res = await this.client.getKey(getKeyRequest, {})
            const count = this.querySessionInfo!.getQueryCount() + 1
            this.querySessionInfo?.setQueryCount(count)
            return res
        } catch (error) {
            throw error
        }
    }

    async closeQuerySession(
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        if (!this.sessionToken) {
            throw new Error('SessionToken is not defined')
        }
        const payload = new CloseSessionPayload()
        payload.setSessionInfo(this.querySessionInfo)
        payload.setSessionToken(this.sessionToken)

        const payloadU8 = payload.serializeBinary()
        const [signature, public_key] = await sign(payloadU8)

        const closeQuerySessionRequest = new CloseSessionRequest()
        closeQuerySessionRequest.setPayload(payloadU8)
        closeQuerySessionRequest.setSignature(signature)
        closeQuerySessionRequest.setPublicKey(public_key)
        try {
            const res = await this.client.closeQuerySession(
                closeQuerySessionRequest,
                {}
            )
            this.querySessionInfo = undefined
            return res.toObject()
        } catch (error) {
            throw error
        }
    }

    async getRange(ns: string, startKey: Uint8Array, endKey: Uint8Array) {
        if (!this.sessionToken) {
            throw new Error('SessionToken is not defined')
        }
        const range = new Range()
        range.setStart(startKey)
        range.setEnd(endKey)

        const rangeKeys = new RangeKey()
        rangeKeys.setNs(ns)
        rangeKeys.setRange(range)
        rangeKeys.setSessionToken(this.sessionToken)

        const rangeRequest = new GetRangeRequest()
        rangeRequest.setRangeKeys(rangeKeys)

        try {
            const res = await this.client.getRange(rangeRequest, {})
            this.querySessionInfo?.setQueryCount(
                this.querySessionInfo.getQueryCount() + 1
            )
            return res
        } catch (error) {
            throw error
        }
    }
    async deleteKey(
        ns: string,
        key: string | Uint8Array,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        const kvPairsList: KVPair[] = []
        const kv_pair = new KVPair()
        if (typeof key === 'string') {
            kv_pair.setKey(encodeUint8Array(key))
        } else {
            kv_pair.setKey(key)
        }

        kv_pair.setAction(MutationAction.DELETEKV)
        kvPairsList.push(kv_pair)
        const mutationObj = new Mutation()
        mutationObj.setNs(encodeUint8Array(ns))
        mutationObj.setKvPairsList(kvPairsList)
        mutationObj.setNonce(Date.now())
        mutationObj.setChainId(ChainId.MAINNET)
        mutationObj.setChainRole(ChainRole.STORAGESHARDCHAIN)
        mutationObj.setGasPrice()
        // mutationObj.setGas(0)

        const mbuffer = mutationObj.serializeBinary()
        const [signature, public_key] = await sign(mbuffer)
        const writeRequest = new WriteRequest()
        writeRequest.setPayload(mbuffer)
        writeRequest.setSignature(signature)
        writeRequest.setPublicKey(public_key)
        writeRequest.setPayloadType(PayloadType.MUTATIONPAYLOAD)
        const broadcastRequest = new BroadcastRequest()
        broadcastRequest.setBody(writeRequest.serializeBinary())
        try {
            const res = await this.client.broadcast(broadcastRequest, {})
            return res.toObject()
        } catch (error) {
            throw error
        }
    }
}
