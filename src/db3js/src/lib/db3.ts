// @ts-nocheck
import {
    WriteRequest,
    PayloadType,
    KVPair,
    Mutation,
    MutationAction,
} from '../pkg/db3_mutation'
import { Erc20Token, Price } from '../pkg/db3_base'
import { ChainId, ChainRole } from '../pkg/db3_base'
import {
    GetRangeRequest,
    RangeKey,
    Range,
    CloseSessionRequest,
    BatchGetKey,
    GetKeyRequest,
    BroadcastRequest,
    GetNamespaceRequest,
    OpenSessionRequest,
    GetAccountRequest,
} from '../pkg/db3_node'
import { QueryPrice, Namespace } from '../pkg/db3_namespace'
import {
    CloseSessionPayload,
    QuerySessionInfo,
    OpenSessionPayload,
} from '../pkg/db3_session'
import {
    GrpcWebFetchTransport,
    GrpcWebOptions,
} from '@protobuf-ts/grpcweb-transport'
import { StorageNodeClient } from '../pkg/db3_node.client'
import * as jspb from 'google-protobuf'

export interface KvMutation {
    ns: string
    gasLimit: number
    data: Record<string, any>
}

export interface NsSimpleDesc {
    name: string
    desc: string
    erc20Token: string
    price: number
    queryCount: number
}

export interface BatchGetKeyRequest {
    ns: string
    keyList: string[] | Uint8Array[]
}

export interface QuerySession {
    sessionInfo: QuerySessionInfo
    sessionToken: string
}

export interface DB3_Options {
    mode: 'DEV' | 'PROD'
}

function encodeUint8Array(text: string) {
    return new TextEncoder('utf-8').encode(text)
}

function uint8ToBase64(arr: Uint8Array) {
    return btoa(
        Array(arr.length)
            .fill('')
            .map((_, i) => String.fromCharCode(arr[i]))
            .join('')
    )
}

export class DB3 {
    private client: StorageNodeClient
    public sessionToken?: string
    private querySessionInfo?: QuerySessionInfo
    constructor(node: string, options?: DB3_Options) {
        const goptions: GrpcWebOptions = {
            baseUrl: node,
            // simple example for how to add auth headers to each request
            // see `RpcInterceptor` for documentation
            interceptors: [],
            // you can set global request headers here
            meta: {},
        }
        const transport = new GrpcWebFetchTransport(goptions)
        this.client = new StorageNodeClient(transport)
    }

    async createSimpleNs(
        desc: NsSimpleDesc,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>,
        nonce?: number
    ) {
        const token: Erc20Token = {
            symbal: desc.erc20Token,
            units: [desc.erc20Token],
            scalar: ['1'],
        }

        const priceProto: Price = {
            amount: desc.price,
            unit: desc.erc20Token,
            token: token,
        }

        const queryPrice: QueryPrice = {
            price: priceProto,
            queryCount: desc.queryCount,
        }

        const namespaceProto: Namespace = {
            name: desc.name,
            price: queryPrice,
            ts: Date.now(),
            description: desc.desc,
        }

        return await this.createNs(namespaceProto, sign, nonce)
    }

    async createNs(
        ns: Namespace,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>,
        nonce?: number
    ) {
        const mbuffer = Namespace.toBinary(ns)
        const [signature, public_key] = await sign(mbuffer)
        const writeRequest: WriteRequest = {
            payload: mbuffer,
            signature: signature,
            publicKey: public_key,
            payloadType: PayloadType.NamespacePayload,
        }
        const broadcastRequest: BroadcastRequest = {
            body: WriteRequest.toBinary(writeRequest),
        }
        const { response } = await this.client.broadcast(broadcastRequest)
        return uint8ToBase64(response.hash)
    }

    async getNsList(
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        const token = await this.keepSession(sign)
        const request: GetNamespaceRequest = {
            sessionToken: token,
        }
        const { response } = await this.client.getNamespace(request)
        const count = this.querySessionInfo!.queryCount + 1
        this.querySessionInfo!.queryCount = count
        return response
    }

    async submitRawMutation(
        ns: string,
        kv_pairs: KVPair[],
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>,
        nonce?: number
    ) {
        const mutation: Mutation = {
            ns: encodeUint8Array(ns),
            kvPairs: kv_pairs,
            nonce: Date.now(),
            chainId: ChainId.MainNet,
            chainRole: ChainRole.StorageShardChain,
            gas_price: null,
            gas: '100',
        }
        const mbuffer = Mutation.toBinary(mutation)
        const [signature, public_key] = await sign(mbuffer)
        const writeRequest: WriteRequest = {
            payload: mbuffer,
            signature: signature,
            publicKey: public_key,
            payloadType: PayloadType.MutationPayload,
        }
        const broadcastRequest: BroadcastRequest = {
            body: WriteRequest.toBinary(writeRequest),
        }
        const { response } = await this.client.broadcast(broadcastRequest)
        const id = uint8ToBase64(response.hash)
        return id
    }

    async submitMutaition(
        mutation: KvMutation,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        const kvPairsList: KVPair[] = []
        Object.keys(mutation.data).forEach((key: string) => {
            const kvPair: KVPair = {
                key: encodeUint8Array(key),
                value: encodeUint8Array(mutation.data[key]),
                action: MutationAction.InsertKv,
            }
            kvPairsList.push(kvPair)
        })
        return await this.submitRawMutation(mutation.ns, kvPairsList, sign)
    }

    async keepSession(
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        if (!this.querySessionInfo) {
            // try to open session
            await this.openQuerySession(sign)
        }
        if (this.querySessionInfo!.queryCount > 1000) {
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
        let header = ''
        if (typeof window === 'undefined') {
            header =
                new Date().getTime() +
                '' +
                '_Header_' +
                Math.floor(Math.random() * 1000)
        } else {
            header = window.crypto.getRandomValues(new Uint8Array(32))
        }
        const payload: OpenSessionPayload = {
            header: header.toString(),
            startTime: Math.floor(Date.now() / 1000),
        }
        const payloadU8 = OpenSessionPayload.toBinary(payload)
        const [signature, public_key] = await sign(payloadU8)
        const sessionRequest: OpenSessionRequest = {
            payload: payloadU8,
            signature: signature,
            publicKey: public_key,
        }
        const { response } = await this.client.openQuerySession(sessionRequest)
        this.sessionToken = response.sessionToken
        this.querySessionInfo = response.querySessionInfo
        return response
    }

    async getAccount(address: string) {
        const getAccountRequest: GetAccountRequest = {
            addr: address,
        }
        const { response } = await this.client.getAccount(getAccountRequest)
        return response
    }

    async getKey(batchGetRequest: BatchGetKeyRequest) {
        if (!this.sessionToken) {
            throw new Error('SessionToken is not defined')
        }

        const keys: Uint8Array[] = []
        batchGetRequest.keyList.forEach((key: string | Uint8Array) => {
            if (typeof key === 'string') {
                keys.push(encodeUint8Array(key))
            } else {
                keys.push(key)
            }
        })

        const batchGetKey: BatchGetKey = {
            ns: encodeUint8Array(batchGetRequest.ns),
            keys: keys,
            sessionToken: this.sessionToken,
        }

        const getKeyRequest: GetKeyRequest = {
            batchGet: batchGetKey,
        }

        const { response } = await this.client.getKey(getKeyRequest)
        const count = this.querySessionInfo!.queryCount + 1
        this.querySessionInfo!.queryCount = count
        return response
    }

    async closeQuerySession(
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        if (!this.sessionToken) {
            throw new Error('SessionToken is not defined')
        }
        const payload: CloseSessionPayload = {
            sessionInfo: this.querySessionInfo,
            sessionToken: this.sessionToken,
        }

        const payloadU8 = CloseSessionPayload.toBinary(payload)
        const [signature, public_key] = await sign(payloadU8)
        const closeQuerySessionRequest: CloseSessionRequest = {
            payload: payloadU8,
            signature: signature,
            publicKey: public_key,
        }

        const { response } = await this.client.closeQuerySession(
            closeQuerySessionRequest
        )
        this.querySessionInfo = undefined
        this.sessionToken = undefined
        return response
    }

    async getRange(ns: string, startKey: Uint8Array, endKey: Uint8Array) {
        if (!this.sessionToken) {
            throw new Error('SessionToken is not defined')
        }
        const range: Range = {
            start: startKey,
            end: endKey,
        }
        const rangeKeys: RangeKey = {
            ns: encodeUint8Array(ns),
            range: range,
            sessionToken: this.sessionToken,
        }

        const rangeRequest: GetRangeRequest = {
            rangeKeys: rangeKeys,
        }

        const { response } = await this.client.getRange(rangeRequest)
        const count = this.querySessionInfo!.queryCount + 1
        this.querySessionInfo!.queryCount = count
        return response
    }

    async deleteKey(
        ns: string,
        key: string | Uint8Array,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        const kvPairsList: KVPair[] = []
        if (typeof key === 'string') {
            const kv_pair: KVPair = {
                action: MutationAction.DeleteKv,
                key: encodeUint8Array(key),
                value: new Uint8Array(0),
            }
            kvPairsList.push(kv_pair)
        } else {
            const kv_pair: KVPair = {
                action: MutationAction.DeleteKv,
                key: key,
                value: new Uint8Array(0),
            }
            kvPairsList.push(kv_pair)
        }
        const id = await this.submitRawMutation(ns, kvPairsList, sign)
        return id
    }
}
