import db3_mutation_pb from '../pkg/db3_mutation_pb'
import db3_base_pb from '../pkg/db3_base_pb'
import db3_node_pb from '../pkg/db3_node_pb'
import { StorageNodeClient } from '../pkg/Db3_nodeServiceClientPb'
import * as jspb from 'google-protobuf'

export interface Mutation {
    ns: string
    gasLimit: number
    data: Record<string, any>
}

export interface BatchGetKeyRequest {
    ns: string
    keyList: string[]
}

export interface QuerySession {
    sessionInfo: db3_node_pb.QuerySessionInfo.AsObject
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
    private querySessionInfo?: db3_node_pb.QuerySessionInfo
    constructor(node: string, options?: DB3_Options) {
        this.client = new StorageNodeClient(node, null, null)
    }

    async submitRawMutation(
        ns: string,
        kv_pairs: db3_mutation_pb.KVPair[],
        sign: (target: Uint8Array) => [Uint8Array, Uint8Array],
        nonce?: number
    ) {
        const mutation = new db3_mutation_pb.Mutation()
        mutation.setNs(encodeUint8Array(ns))
        mutation.setKvPairsList(kv_pairs)
        if (typeof nonce !== 'undefined') {
            mutation.setNonce(nonce)
        } else {
            mutation.setNonce(Date.now())
        }
        mutation.setChainId(db3_base_pb.ChainId.MAINNET)
        mutation.setChainRole(db3_base_pb.ChainRole.STORAGESHARDCHAIN)
        mutation.setGasPrice()
        mutation.setGas(100)
        const mbuffer = mutation.serializeBinary()
        const [signature, public_key] = await sign(mbuffer)
        const writeRequest = new db3_mutation_pb.WriteRequest()
        writeRequest.setMutation(mbuffer)
        writeRequest.setSignature(signature)
        writeRequest.setPublicKey(public_key)
        const broadcastRequest = new db3_node_pb.BroadcastRequest()
        broadcastRequest.setBody(writeRequest.serializeBinary())
        const res = await this.client.broadcast(broadcastRequest, {})
        return res.toObject()
    }

    async submitMutaition(
        mutation: Mutation,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        const kvPairsList: db3_mutation_pb.KVPair[] = []
        Object.keys(mutation.data).forEach((key: string) => {
            const kv_pair = new db3_mutation_pb.KVPair()
            kv_pair.setKey(encodeUint8Array(key))
            kv_pair.setValue(encodeUint8Array(mutation.data[key]))
            kv_pair.setAction(db3_mutation_pb.MutationAction.INSERTKV)
            kvPairsList.push(kv_pair)
        })
        const mutationObj = new db3_mutation_pb.Mutation()
        mutationObj.setNs(encodeUint8Array(mutation.ns))
        mutationObj.setKvPairsList(kvPairsList)
        if (typeof nonce !== 'undefined') {
            mutationObj.setNonce(nonce)
        } else {
            mutationObj.setNonce(Date.now())
        }
        mutationObj.setChainId(db3_base_pb.ChainId.MAINNET)
        mutationObj.setChainRole(db3_base_pb.ChainRole.STORAGESHARDCHAIN)
        mutationObj.setGasPrice()
        mutationObj.setGas(mutation.gasLimit)

        const mbuffer = mutationObj.serializeBinary()
        const [signature, public_key] = await sign(mbuffer)
        const writeRequest = new db3_mutation_pb.WriteRequest()
        writeRequest.setMutation(mbuffer)
        writeRequest.setSignature(signature)
        writeRequest.setPublicKey(public_key)

        const broadcastRequest = new db3_node_pb.BroadcastRequest()
        broadcastRequest.setBody(writeRequest.serializeBinary())
        try {
            const res = await this.client.broadcast(broadcastRequest, {})
            return res.toObject()
        } catch (error) {
            throw error
        }
    }

    async keepSession(sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>) {
        if (!this.querySessionInfo) {
            // try to open session
            await this.openQuerySession(sign)
        }
        //TODO handle exeception
        if (this.querySessionInfo?.getQueryCount() > 1000) {
            await this.closeQuerySession(sign)
            await this.openQuerySession(sign)
        }
        return this.sessionToken
    }

    async openQuerySession(sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>) {
        if (this.querySessionInfo) {
            throw new Error(
                'The current db3js instance has already opened a session, so do not open it repeatedly'
            )
        }
        const sessionRequest = new db3_node_pb.OpenSessionRequest()
        const header = window.crypto.getRandomValues(new Uint8Array(32))
        // const header = encodeUint8Array('Header');
        const [signature, public_key] = await sign(header)
        sessionRequest.setHeader(header)
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
        const getAccountRequest = new db3_node_pb.GetAccountRequest()
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
        const getKeyRequest = new db3_node_pb.GetKeyRequest()
        const batchGetKey = new db3_node_pb.BatchGetKey()
        batchGetKey.setNs(batchGetRequest.ns)
        batchGetKey.setKeysList(batchGetRequest.keyList)
        //todo handle null session token
        batchGetKey.setSessionToken(this.sessionToken)
        getKeyRequest.setBatchGet(batchGetKey)
        try {
            const res = await this.client.getKey(getKeyRequest, {})
            const count = this.querySessionInfo?.getQueryCount() + 1
            this.querySessionInfo?.setQueryCount(count)
            return res
        } catch (error) {
            throw error
        }
    }

    async closeQuerySession(sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>) {
        if (!this.sessionToken) {
            throw new Error('SessionToken is not defined')
        }

        const payload = new db3_node_pb.CloseSessionPayload()
        payload.setSessionInfo(this.querySessionInfo)
        payload.setSessionToken(this.sessionToken)

        const payloadU8 = payload.serializeBinary()
        const [signature, public_key] = await sign(payloadU8)

        const closeQuerySessionRequest = new db3_node_pb.CloseSessionRequest()
        closeQuerySessionRequest.setPayload(payloadU8)
        closeQuerySessionRequest.setSignature(signature)
        closeQuerySessionRequest.setPublicKey(public_key)
        try {
            const res = await this.client.closeQuerySession(closeQuerySessionRequest, {})
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
        const _range = new db3_node_pb.Range()
        _range.setStart(startKey)
        _range.setEnd(endKey)

        const rangeKeys = new db3_node_pb.RangeKey()
        rangeKeys.setNs(ns)
        rangeKeys.setRange(_range)
        rangeKeys.setSessionToken(this.sessionToken)

        const rangeRequest = new db3_node_pb.GetRangeRequest()
        rangeRequest.setRangeKeys(rangeKeys)

        try {
            const res = await this.client.getRange(rangeRequest, {})
            this.querySessionInfo?.setQueryCount(this.querySessionInfo.getQueryCount() + 1)
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
        const kvPairsList: db3_mutation_pb.KVPair[] = []
        const kv_pair = new db3_mutation_pb.KVPair()
        if (typeof key === 'string') {
            kv_pair.setKey(encodeUint8Array(key))
        } else {
            kv_pair.setKey(key)
        }

        kv_pair.setAction(db3_mutation_pb.MutationAction.DELETEKV)
        kvPairsList.push(kv_pair)
        const mutationObj = new db3_mutation_pb.Mutation()
        mutationObj.setNs(encodeUint8Array(ns))
        mutationObj.setKvPairsList(kvPairsList)
        mutationObj.setNonce(Date.now())
        mutationObj.setChainId(db3_base_pb.ChainId.MAINNET)
        mutationObj.setChainRole(db3_base_pb.ChainRole.STORAGESHARDCHAIN)
        mutationObj.setGasPrice()
        // mutationObj.setGas(0)

        const mbuffer = mutationObj.serializeBinary()
        const [signature, public_key] = await sign(mbuffer)
        const writeRequest = new db3_mutation_pb.WriteRequest()
        writeRequest.setMutation(mbuffer)
        writeRequest.setSignature(signature)
        writeRequest.setPublicKey(public_key)

        const broadcastRequest = new db3_node_pb.BroadcastRequest()
        broadcastRequest.setBody(writeRequest.serializeBinary())
        try {
            const res = await this.client.broadcast(broadcastRequest, {})
            return res.toObject()
        } catch (error) {
            throw error
        }
    }
}
