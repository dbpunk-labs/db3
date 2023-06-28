//
// crypto_test.ts
// Copyright (C) 2023 db3.network Author imotai <codego.me@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

import { describe, expect, test } from '@jest/globals'
import {
    DB3ClientV2,
    createClient,
    syncAccountNonce,
    getMutationHeader,
    getMutationBody,
    scanMutationHeaders,
    getStorageNodeStatus,
    getIndexNodeStatus,
    configRollup,
    getContractSyncStatus,
    setupStorageNode,
} from '../src/client/client_v2'
import {
    addDoc,
    deleteDoc,
    updateDoc,
    queryDoc,
} from '../src/store/document_v2'
import {
    createFromPrivateKey,
    createRandomAccount,
} from '../src/account/db3_account'
import {
    createDocumentDatabase,
    createEventDatabase,
    showDatabase,
    createCollection,
    getDatabase,
    getCollection,
} from '../src/store/database_v2'
import { Index, IndexType } from '../src/proto/db3_database_v2'

interface Profile {
    city: string
    author: string
    age: number
}

describe('test db3.js client module', () => {
    async function createTestClient() {
        const db3_account = createRandomAccount()
        const client = createClient(
            'http://127.0.0.1:26619',
            'http://127.0.0.1:26639',
            db3_account
        )
        const nonce = await syncAccountNonce(client)
        return client
    }

    async function createAdminClient() {
        const privateKey =
            '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'
        const db3_account = createFromPrivateKey(privateKey)
        const client = createClient(
            'http://127.0.0.1:26619',
            'http://127.0.0.1:26639',
            db3_account
        )
        const nonce = await syncAccountNonce(client)
        return client
    }

    test('test setup storage system status', async () => {
        const client = await createAdminClient()
        const response = await setupStorageNode(
            client,
            '9527',
            '1000000',
            '1000000'
        )
        expect('0').toBe(response.code)
        const status = await getStorageNodeStatus(client)
        expect(true).toBe(status.hasInited)
        expect('9527').toBe(status.config?.networkId)
        expect('1000000').toBe(status.config?.minRollupSize)
        expect('1000000').toBe(status.config?.rollupInterval)
    })

    test('test get index system status', async () => {
        const client = await createTestClient()
        const response = await getIndexNodeStatus(client)
        console.log(response)
    })

    test('create event db smoke test', async () => {
        const client = await createTestClient()
        expect(1).toBe(client.nonce)
        const abi = `
        [{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"deposit","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"},{"name":"","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"guy","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Deposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Withdrawal","type":"event"}]
        `
        const evmNodeUrl =
            'wss://polygon-mainnet.g.alchemy.com/v2/EH9ZSJ0gS7a1DEIohAWMbhP33lK6qHj9'
        const response = await createEventDatabase(
            client,
            'desc',
            '0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270',
            ['Transfer', 'Deposit', 'Approval', 'Withdrawal'],
            abi,
            evmNodeUrl
        )
        console.log(response)
        await new Promise((r) => setTimeout(r, 10000))
        console.log(await getContractSyncStatus(client))
    })

    test('create client smoke test', async () => {
        const client = await createTestClient()
        expect(1).toBe(client.nonce)

        const rollupInterval = 10 * 60 * 1000
        const minRollupSize = 10 * 1024 * 1024
        console.log(await getStorageNodeStatus(client))
        console.log(await getIndexNodeStatus(client))
        //    console.log(await configRollup(client, rollupInterval, minRollupSize))
    })

    test('test query document', async () => {
        const client = await createTestClient()
        try {
            const { db, result } = await createDocumentDatabase(client, 'db1')
            const index: Index = {
                path: '/city',
                indexType: IndexType.StringKey,
            }
            {
                const { collection, result } = await createCollection(
                    db,
                    'col',
                    [index]
                )
                await new Promise((r) => setTimeout(r, 3000))
                await addDoc(collection, {
                    city: 'beijing',
                    author: 'imotai',
                    age: 10,
                })

                await addDoc(collection, {
                    city: 'beijing2',
                    author: 'imotai1',
                    age: 1,
                })
                await new Promise((r) => setTimeout(r, 3000))
                {
                    const queryStr = '/[city = beijing]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].doc.city).toBe('beijing')
                    expect(resultSet.docs[0].doc.author).toBe('imotai')
                    expect(resultSet.docs[0].doc.age).toBe(10)
                }
                {
                    const queryStr = '/* | limit 1'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].doc.city).toBe('beijing2')
                    expect(resultSet.docs[0].doc.author).toBe('imotai1')
                    expect(resultSet.docs[0].doc.age).toBe(1)
                }

                {
                    const queryStr = '/[age = :age]'
                    const parameter: QueryParameter = {
                        name: 'age',
                        parameter: {
                            oneofKind: 'int64Value',
                            int64Value: 10,
                        },
                        idx: 0,
                    }
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr,
                        [parameter]
                    )
                    console.log(resultSet)
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].doc.city).toBe('beijing')
                    expect(resultSet.docs[0].doc.author).toBe('imotai')
                    expect(resultSet.docs[0].doc.age).toBe(10)
                }
                {
                    const queryStr = '/{age}'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr,
                        []
                    )
                    console.log(resultSet.docs)
                }
            }
        } catch (e) {
            console.log(e)
            expect(1).toBe(0)
        }
    })
    test('test create/update/delete document', async () => {
        const client = await createTestClient()
        try {
            const { db, result } = await createDocumentDatabase(
                client,
                'db_for_update_delete'
            )
            const index: Index = {
                path: '/city',
                indexType: IndexType.StringKey,
            }
            {
                const { collection, result } = await createCollection(
                    db,
                    'col',
                    [index]
                )
                await new Promise((r) => setTimeout(r, 1000))
                const doc1Ret = await addDoc(collection, {
                    city: 'beijing',
                    author: 'imotai',
                    age: 10,
                })

                const doc2Ret = await addDoc(collection, {
                    city: 'beijing2',
                    author: 'imotai1',
                    age: 1,
                })
                await new Promise((r) => setTimeout(r, 1000))
                {
                    const queryStr = '/[city = beijing]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].doc.city).toBe('beijing')
                    expect(resultSet.docs[0].doc.author).toBe('imotai')
                    expect(resultSet.docs[0].doc.age).toBe(10)
                    expect(resultSet.docs[0].id).toBe(doc1Ret.id)
                }
                {
                    const queryStr = '/[city = beijing2]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].doc.city).toBe('beijing2')
                    expect(resultSet.docs[0].doc.author).toBe('imotai1')
                    expect(resultSet.docs[0].doc.age).toBe(1)
                    expect(resultSet.docs[0].id).toBe(doc2Ret.id)
                }
                await updateDoc(
                    collection,
                    doc1Ret.id,
                    {
                        city: 'beijing3',
                        author: 'imotai3',
                        age: 3,
                    },
                    []
                )
                await new Promise((r) => setTimeout(r, 1000))
                {
                    const queryStr = '/[city = beijing]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(0).toBe(resultSet.docs.length)
                }
                {
                    const queryStr = '/[city = beijing3]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].doc.city).toBe('beijing3')
                    expect(resultSet.docs[0].doc.author).toBe('imotai3')
                    expect(resultSet.docs[0].doc.age).toBe(3)
                    expect(resultSet.docs[0].id).toBe(doc1Ret.id)
                }
                await deleteDoc(collection, [doc1Ret.id])
                await new Promise((r) => setTimeout(r, 1000))
                {
                    const queryStr = '/[city = beijing3]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(0).toBe(resultSet.docs.length)
                }
            }
        } catch (e) {
            console.log(e)
            expect(1).toBe(0)
        }
    })
    test('test delete doc id not found', async () => {
        const client = await createTestClient()
        try {
            const { db, result } = await createDocumentDatabase(
                client,
                'db_for_update_delete'
            )
            const index: Index = {
                path: '/city',
                indexType: IndexType.StringKey,
            }
            {
                const { collection, result } = await createCollection(
                    db,
                    'col',
                    [index]
                )
                await new Promise((r) => setTimeout(r, 2000))
                const docId2 = await addDoc(collection, {
                    city: 'beijing',
                    author: 'imotai',
                    age: 10,
                })
                await new Promise((r) => setTimeout(r, 2000))
                {
                    const queryStr = '/[city = beijing]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].doc.city).toBe('beijing')
                    expect(resultSet.docs[0].doc.author).toBe('imotai')
                    expect(resultSet.docs[0].doc.age).toBe(10)
                    expect(resultSet.docs[0].id).toBe(docId2.id)
                }

                await deleteDoc(collection, [docId2.id])
                await new Promise((r) => setTimeout(r, 2000))
                {
                    const queryStr = '/[city = beijing3]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(0).toBe(resultSet.docs.length)
                }
                try {
                    await deleteDoc(collection, [docId2.id])
                    fail('should not reach here')
                } catch (e) {
                    expect(decodeURI(e.message)).toBe(
                        'fail to verify the owner with error doc id is not found'
                    )
                }
            }
        } catch (e) {
            console.log(e)
            expect(1).toBe(0)
        }
    })
    test('test update doc id not found', async () => {
        const client = await createTestClient()
        try {
            const { db, result } = await createDocumentDatabase(
                client,
                'db_for_update_delete'
            )
            const index: Index = {
                path: '/city',
                indexType: IndexType.StringKey,
            }
            {
                const { collection, result } = await createCollection(
                    db,
                    'col',
                    [index]
                )
                await new Promise((r) => setTimeout(r, 3000))
                const docId2 = await addDoc(collection, {
                    city: 'beijing',
                    author: 'imotai',
                    age: 10,
                })
                await new Promise((r) => setTimeout(r, 2000))
                {
                    const queryStr = '/[city = beijing]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].doc.city).toBe('beijing')
                    expect(resultSet.docs[0].doc.author).toBe('imotai')
                    expect(resultSet.docs[0].doc.age).toBe(10)
                    expect(resultSet.docs[0].id).toBe(docId2.id)
                }

                await deleteDoc(collection, [docId2.id])
                await new Promise((r) => setTimeout(r, 2000))
                {
                    const queryStr = '/[city = beijing3]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(0).toBe(resultSet.docs.length)
                }
                try {
                    await updateDoc(
                        collection,
                        docId2.id,
                        {
                            city: 'beijing3',
                            author: 'imotai3',
                            age: 3,
                        },
                        []
                    )
                    fail('should not reach here')
                } catch (e) {
                    expect(decodeURI(e.message)).toBe(
                        'fail to verify the owner with error doc id is not found'
                    )
                }
            }
        } catch (e) {
            console.log(e)
            expect(1).toBe(0)
        }
    })
    test('test ownership verify for update/delete document', async () => {
        const client = await createTestClient()
        const client2 = await createTestClient()
        try {
            const { db, result } = await createDocumentDatabase(
                client,
                'db_for_update_delete'
            )
            const index: Index = {
                path: '/city',
                indexType: IndexType.StringKey,
            }
            {
                const { collection, result } = await createCollection(
                    db,
                    'col',
                    [index]
                )
                const collection2 = await getCollection(db.addr, 'col', client2)

                await new Promise((r) => setTimeout(r, 3000))
                const docId2 = await addDoc(collection, {
                    city: 'beijing',
                    author: 'imotai',
                    age: 10,
                })

                const docId3 = await addDoc(collection2, {
                    city: 'beijing2',
                    author: 'imotai1',
                    age: 1,
                })
                await new Promise((r) => setTimeout(r, 3000))
                {
                    const queryStr = '/[city = beijing]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].doc.city).toBe('beijing')
                    expect(resultSet.docs[0].doc.author).toBe('imotai')
                    expect(resultSet.docs[0].doc.age).toBe(10)
                    expect(resultSet.docs[0].id).toBe(docId2.id)
                }
                {
                    const queryStr = '/[city = beijing]'
                    const resultSet = await queryDoc<Profile>(
                        collection2,
                        queryStr
                    )
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].doc.city).toBe('beijing')
                    expect(resultSet.docs[0].doc.author).toBe('imotai')
                    expect(resultSet.docs[0].doc.age).toBe(10)
                    expect(resultSet.docs[0].id).toBe(docId2.id)
                }

                try {
                    await deleteDoc(collection, [docId3.id])
                    fail('should not be here')
                } catch (e) {
                    expect(decodeURI(e.message)).toBe(
                        'fail to verify the owner with error doc owner is not the sender'
                    )
                }
                // verify docId3 is not deleted
                await new Promise((r) => setTimeout(r, 3000))
                {
                    const queryStr = '/[city = beijing2]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].id).toBe(docId3.id)
                }
                await deleteDoc(collection2, [docId3.id])
                await new Promise((r) => setTimeout(r, 3000))
                {
                    const queryStr = '/[city = beijing2]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(0).toBe(resultSet.docs.length)
                }

                // update doc fail with ownership verify error
                try {
                    await updateDoc(
                        collection2,
                        docId2.id,
                        {
                            city: 'beijing_new',
                            author: 'imotai_new',
                            age: 3,
                        },
                        []
                    )
                } catch (e) {
                    expect(decodeURI(e.message)).toBe(
                        'fail to verify the owner with error doc owner is not the sender'
                    )
                }
                // verify docId2 is not updated
                await new Promise((r) => setTimeout(r, 3000))
                {
                    const queryStr = '/[city = beijing]'
                    const resultSet = await queryDoc<Profile>(
                        collection,
                        queryStr
                    )
                    expect(1).toBe(resultSet.docs.length)
                    expect(resultSet.docs[0].id).toBe(docId2.id)
                }
            }
        } catch (e) {
            console.log(e)
            expect(1).toBe(0)
        }
    })
    test('create database v2', async () => {
        const client = await createTestClient()
        try {
            const { db, result } = await createDocumentDatabase(client, 'desc')
            const header = await getMutationHeader(
                client,
                result.block,
                result.order
            )
            if (!header.header) {
                expect(1).toBe(0)
            }
            const body = await getMutationBody(client, result.id)
            expect(body).toBeDefined()
            const databases = await showDatabase(client.account.address, client)
            expect(1).toBe(databases.length)
        } catch (e) {
            console.log(e)
            expect(1).toBe(0)
        }
    })

    test('test scan mutation headers', async () => {
        const client = await createTestClient()
        try {
            const { db, result } = await createDocumentDatabase(client, 'desc')
            const headers = await scanMutationHeaders(client, 0, 1)
            expect(headers.length).toBe(1)
        } catch (e) {
            console.log(e)
            expect(1).toBe(0)
        }
    })

    test('test add large mutations', async () => {
        const client = await createTestClient()
        try {
            for (var i = 0; i < 1; i++) {
                const { db, result } = await createDocumentDatabase(
                    client,
                    'desc'
                )
                const index: Index = {
                    path: '/city',
                    indexType: IndexType.StringKey,
                }
                {
                    const { collection, result } = await createCollection(
                        db,
                        'col',
                        [index]
                    )
                    await addDoc(collection, {
                        name: 'book1',
                        author: 'db3 developers',
                        id: '0x10b1b560b2fd9a66ae5bce29e5050ffcef6bcc9663d5d116e9877b6a4dda13aa',
                        time: '1686285013',
                        fee: '0.069781',
                    })
                }
            }
        } catch (e) {
            console.log(e)
            expect(1).toBe(0)
        }
    })
})
