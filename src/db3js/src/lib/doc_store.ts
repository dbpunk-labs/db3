import { DB3 } from './db3'
import db3_mutation_pb, { KVPair } from '../pkg/db3_mutation_pb'
import { SmartBuffer, SmartBufferOptions } from 'smart-buffer'
import * as jspb from 'google-protobuf'

// the type of doc key
// the string and number are supported
export enum DocKeyType {
    STRING = 0,
    NUMBER,
}

export interface DocKey {
    name: string
    keyType: DocKeyType
}

export interface DocIndex {
    keys: DocKey[]
    ns: string
    docName: string
}

function genStartKey(index: DocIndex) {
    const buff = new SmartBuffer()
    type ObjectKey = keyof typeof doc
    // write the doc name to the key
    var offset = 0
    buff.writeString(index.docName, offset)
    offset += index.docName.length
    index.keys.forEach((key: DocKey) => {
        switch (key.keyType) {
            case DocKeyType.STRING: {
                const objectKey = key.name as ObjectKey
                buff.writeString('' as unknown as string, offset)
                offset += ''.length
                break
            }
            case DocKeyType.NUMBER: {
                buff.writeBigInt64BE(BigInt(0 as unknown as number), offset)
                offset += 8
                break
            }
        }
    })
    const buffer = buff.toBuffer().buffer
    return new Uint8Array(buffer, 0, offset)
}

function genEndKey(index: DocIndex) {
    const buff = new SmartBuffer()
    type ObjectKey = keyof typeof doc
    // write the doc name to the key
    var offset = 0
    buff.writeString(index.docName, offset)
    offset += index.docName.length
    index.keys.forEach((key: DocKey) => {
        switch (key.keyType) {
            case DocKeyType.STRING: {
                const objectKey = key.name as ObjectKey
                buff.writeString('~' as unknown as string, offset)
                offset += '~'.length
                break
            }
            case DocKeyType.NUMBER: {
                //TODO not the exaclty the end
                buff.writeBigInt64BE(BigInt(Number.MAX_SAFE_INTEGER), offset)
                offset += 8
                break
            }
        }
    })
    const buffer = buff.toBuffer().buffer
    return new Uint8Array(buffer, 0, offset)
}

export function genPrimaryKey(index: DocIndex, doc: Object) {
    const buff = new SmartBuffer()
    type ObjectKey = keyof typeof doc
    // write the doc name to the key
    var offset = 0
    buff.writeString(index.docName, offset)
    offset += index.docName.length
    index.keys.forEach((key: DocKey) => {
        switch (key.keyType) {
            case DocKeyType.STRING: {
                const objectKey = key.name as ObjectKey
                let value = doc[objectKey]
                buff.writeString(value as unknown as string, offset)
                offset += (value as unknown as string).length
                break
            }
            case DocKeyType.NUMBER: {
                const objectKey = key.name as ObjectKey
                let value = doc[objectKey]
                buff.writeBigInt64BE(BigInt(value as unknown as number), offset)
                offset += 8
                break
            }
        }
    })
    const buffer = buff.toBuffer().buffer
    return new Uint8Array(buffer, 0, offset)
}

export function object2Buffer(doc: Object) {
    const buff = new SmartBuffer()
    const json_str = JSON.stringify(doc)
    buff.writeString(json_str)
    const buffer = buff.toBuffer().buffer
    return new Uint8Array(buffer, 0, json_str.length)
}

export class DocMetaManager {
    private doc_store: DocStore
    constructor(db3: DB3) {
        this.doc_store = new DocStore(db3)
    }

    async get_all_doc_metas(ns:string,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>,
                          ) {
        const static_doc_index = {
            keys: [
                {
                    name: 'doc_name',
                    keyType: DocKeyType.STRING,
                },
                {
                    name: 'ts',
                    keyType: DocKeyType.NUMBER,
                },
            ],
            ns: ns,
            docName: '_meta_',
        }
        return await this.doc_store.queryAllDocs(ns, static_doc_index, sign)
    }

    async create_doc_meta(doc_index: DocIndex, desc: string,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>,
                      ) {
        const static_doc_index = {
            keys: [
                {
                    name: 'doc_name',
                    keyType: DocKeyType.STRING,
                },
                {
                    name: 'ts',
                    keyType: DocKeyType.NUMBER,
                },
            ],
            ns: doc_index.ns,
            docName: '_meta_',
        }
        const doc_meta = {
            'doc_name': doc_index.docName,
            'ts': Date.now(),
            'index': doc_index,
            'desc': desc
        }
        //TODO check if the doc meta exists
        return await this.doc_store.insertDocs(static_doc_index, [doc_meta], sign)
    }
}

export class DocStore {
    private db3: DB3
    constructor(db3: DB3) {
        this.db3 = db3
    }

    async insertDocs(
        index: DocIndex,
        docs: Object[],
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>,
        nonce?: number
    ) {
        const kvPairs: db3_mutation_pb.KVPair[] = []
        docs.forEach((doc: Object) => {
            const key = genPrimaryKey(index, doc) as Uint8Array
            const kvPair = new db3_mutation_pb.KVPair()
            kvPair.setKey(key)
            kvPair.setValue(object2Buffer(doc))
            kvPair.setAction(db3_mutation_pb.MutationAction.INSERTKV)
            kvPairs.push(kvPair)
        })
        return await this.db3.submitRawMutation(index.ns, kvPairs, sign, nonce)
    }

    async getDocs(
        index: DocIndex,
        queries: Object[],
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        const keys: Uint8Array[] = []
        queries.forEach((doc: Object) => {
            const key = genPrimaryKey(index, doc) as Uint8Array
            keys.push(key)
        })
        await this.db3.keepSession(sign)
        const response = await this.db3.getKey({
            ns: index.ns,
            keyList: keys,
        })
        const docs: Object[] = []
        response
            .getBatchGetValues()
            ?.getValuesList()
            .forEach((kvPair: db3_mutation_pb.KVPair) => {
                docs.push(
                    JSON.parse(
                        new TextDecoder('utf-8').decode(kvPair.getValue_asU8())
                    )
                )
            })

        return docs
    }

    async queryAllDocs(
        ns: String,
        index: DocIndex,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        try {
            await this.db3.keepSession(sign)
            const docs: Record<string, any>[] = []
            const res = await this.db3.getRange(
                ns,
                genStartKey(index),
                genEndKey(index)
            )
            res.getRangeValue()
                ?.getValuesList()
                .forEach((kvPair: db3_mutation_pb.KVPair) => {
                    docs.push(
                        JSON.parse(
                            new TextDecoder('utf-8').decode(
                                kvPair.getValue_asU8()
                            )
                        )
                    )
                })
            return docs
        } catch (error) {
            throw error
        }
    }

    async queryDocsByRange(
        ns: string,
        index: DocIndex,
        startKey: Record<string, any>,
        endKey: Record<string, any>,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        try {
            await this.db3.keepSession(sign)
            const docs: Record<string, any>[] = []
            const res = await this.db3.getRange(
                ns,
                genPrimaryKey(index, startKey),
                genPrimaryKey(index, endKey)
            )
            res.getRangeValue()
                ?.getValuesList()
                .forEach((kvPair: db3_mutation_pb.KVPair) => {
                    docs.push(
                        JSON.parse(
                            new TextDecoder('utf-8').decode(
                                kvPair.getValue_asU8()
                            )
                        )
                    )
                })
            return docs
        } catch (error) {
            throw error
        }
    }

    async deleteDoc(
        ns: string,
        index: DocIndex,
        doc: Record<string, any>,
        sign: (target: Uint8Array) => Promise<[Uint8Array, Uint8Array]>
    ) {
        const key = genPrimaryKey(index, doc)
        try {
            const res = await this.db3.deleteKey(ns, key, sign)
            return res
        } catch (error) {
            throw error
        }
    }
}
