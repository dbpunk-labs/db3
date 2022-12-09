import { describe, expect, test } from '@jest/globals'
import { DB3 } from './db3'
import { DocStore, DocIndex, DocKey, DocKeyType, genPrimaryKey, object2Buffer } from './doc_store'
import { sign, getATestStaticKeypair, getAddress } from './keys'
import { TextEncoder, TextDecoder } from 'util'
global.TextEncoder = TextEncoder
global.TextDecoder = TextDecoder

describe('test db3js api', () => {
    async function getSign() {
        const [sk, public_key] = await getATestStaticKeypair()
        async function _sign(data: Uint8Array): Promise<[Uint8Array, Uint8Array]> {
            return [await sign(data, sk), public_key]
        }
        return _sign
    }

    test('test submitMutation', async () => {
        const db3_instance = new DB3('http://127.0.0.1:26659')
        const _sign = await getSign()
        const result = await db3_instance.submitMutaition(
            {
                ns: 'my_twitter',
                gasLimit: 10,
                data: { test1: 'value123' },
            },
            _sign
        )
        expect(result).toBeDefined()
    })
    test('test openQuerySession', async () => {
        const db3_instance = new DB3('http://127.0.0.1:26659')
        const _sign = await getSign()
        try {
            const { sessionToken } = await db3_instance.openQuerySession(_sign)
            expect(typeof sessionToken).toBe('string')
        } catch (error) {
            throw error
        }
    })
    test('test getKey', async () => {
        const db3_instance = new DB3('http://127.0.0.1:26659')
        const _sign = await getSign()
        try {
            await db3_instance.submitMutaition(
                {
                    ns: 'my_twitter',
                    gasLimit: 10,
                    data: { key123: 'value123' },
                },
                _sign
            )
            await db3_instance.openQuerySession(_sign)
            await new Promise(r => setTimeout(r, 2000))
            const queryRes = await db3_instance.getKey({
                ns: 'my_twitter',
                keyList: ['key123'],
            })
            expect(queryRes.toObject().batchGetValues?.valuesList[0].value).toBe('value123')
        } catch (error) {
            throw error
        }
    })
    test('test db3 submit data and query data', async () => {
        const db3_instance = new DB3('http://127.0.0.1:26659')
        const _sign = await getSign()
        try {
            await db3_instance.submitMutaition(
                {
                    ns: 'my_twitter',
                    gasLimit: 10,
                    data: { test2: 'value123' },
                },
                _sign
            )
            await new Promise(r => setTimeout(r, 2000))
            await db3_instance.openQuerySession(_sign)
            const queryRes = await db3_instance.getKey({
                ns: 'my_twitter',
                keyList: ['test2'],
            })
            expect(queryRes.toObject().batchGetValues?.valuesList[0].value).toBe('value123')
            const closeRes = await db3_instance.closeQuerySession(_sign)
            expect(closeRes).toBeDefined()
        } catch (error) {
            console.error(error)
            throw error
        }
    })
    test('gen primary key', async () => {
        const doc_index = {
            keys: [
                {
                    name: 'address',
                    keyType: DocKeyType.STRING,
                },
                {
                    name: 'ts',
                    keyType: DocKeyType.NUMBER,
                },
            ],
            ns: 'ns1',
            docName: 'transaction',
        }
        const transacion = {
            address: '0x11111',
            ts: 9527,
        }
        const pk = genPrimaryKey(doc_index, transacion)
        const uint8ToBase64 = (arr: Uint8Array): string =>
            btoa(
                Array(arr.length)
                    .fill('')
                    .map((_, i) => String.fromCharCode(arr[i]))
                    .join('')
            )
        expect(uint8ToBase64(pk)).toBe('dHJhbnNhY3Rpb24weDExMTExAAAAAAAAJTc=')
        expect(uint8ToBase64(object2Buffer(transacion))).toBe(
            'eyJhZGRyZXNzIjoiMHgxMTExMSIsInRzIjo5NTI3fQ=='
        )
    })
    test('test insert a doc', async () => {
        const [sk, public_key] = await getATestStaticKeypair()
        const db3_instance = new DB3('http://127.0.0.1:26659')
        const doc_store = new DocStore(db3_instance)
        const _sign = await getSign()
        const doc_index = {
            keys: [
                {
                    name: 'address',
                    keyType: DocKeyType.STRING,
                },
                {
                    name: 'ts',
                    keyType: DocKeyType.NUMBER,
                },
            ],
            ns: 'ns1',
            docName: 'transaction',
        }
        const transacion = {
            address: '0x11111',
            ts: 9527,
            amount: 10,
        }
        const result = await doc_store.insertDocs(doc_index, [transacion], _sign, 1)
        expect(result.hash).toBe('ZBv3EfQajYQ9ibANS/SMl9X2FYwvqG11+8B4eTH5mUA=')
        await new Promise(r => setTimeout(r, 2000))
        const query = {
            address: '0x11111',
            ts: 9527,
        }
        const docs = await doc_store.getDocs(doc_index, [query], _sign)
        expect(docs.length).toBe(1)
        expect(docs[0].amount).toBe(10)
    })

    test('query document range by keys', async () => {
        const db3_instance = new DB3('http://127.0.0.1:26659')
        const doc_store = new DocStore(db3_instance)
        const _sign = await getSign()
        const doc_index = {
            keys: [
                {
                    name: 'address',
                    keyType: DocKeyType.STRING,
                },
                {
                    name: 'ts',
                    keyType: DocKeyType.NUMBER,
                },
            ],
            ns: 'ns1',
            docName: 'transaction',
        }
        const transacions = [
            {
                address: '0x11111',
                ts: 9529,
            },
            {
                address: '0x11112',
                ts: 9530,
            },
            {
                address: '0x11113',
                ts: 9533,
            },
            {
                address: '0x11114',
                ts: 9534,
            },
        ]
        await doc_store.insertDocs(doc_index, transacions, _sign, 1)
        await new Promise(r => setTimeout(r, 2000))
        const res = await doc_store.queryDocsByRange(
            'ns1',
            [
                doc_index,
                {
                    address: '0x11111',
                    ts: 9529,
                },
            ],
            [
                doc_index,
                {
                    address: '0x11114',
                    ts: 9534,
                },
            ],
            _sign
        )
        expect(res[2].address).toBe('0x11113')
    })
})
