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
                buff.writeString((value as unknown) as string, offset)
                offset += ((value as unknown) as string).length
                break
            }
            case DocKeyType.NUMBER: {
                const objectKey = key.name as ObjectKey
                let value = doc[objectKey]
                buff.writeBigInt64BE(
                    BigInt((value as unknown) as number),
                    offset
                )
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
        const session = await this.db3.keepSession(sign)
        const response = await this.db3.getKey({
            ns: index.ns,
            keyList: keys,
            sessionToken: session,
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
}
