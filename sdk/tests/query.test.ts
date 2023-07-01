//
// query_test.ts
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
    getMutationState,
    createReadonlyClient,
} from '../src/client/client_v2'

import { Client } from '../src/client/types'

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

describe('test db3.js document_v2 module', () => {
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

    async function createReadClient() {
        const client = createReadonlyClient(
            'http://127.0.0.1:26619',
            'http://127.0.0.1:26639'
        )
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

    async function prepareTheDataset(client: Client) {
        const { db } = await createDocumentDatabase(client, 'db1')
        const { collection } = await createCollection(db, 'profile')
        await new Promise((r) => setTimeout(r, 1000))
        const doc1 = {
            firstName: 'John',
            lastName: 'Doe',
            age: 28,
            pets: [
                {
                    name: 'Rexy rex',
                    kind: 'dog',
                    likes: ['bones', 'jumping', 'toys'],
                },
                {
                    name: 'Grenny',
                    kind: 'parrot',
                    likes: ['green color', 'night', 'toys'],
                },
            ],
        }
        await addDoc(collection, doc1)
        await new Promise((r) => setTimeout(r, 1000))
        return collection
    }

    test('test query with count', async () => {
        const client = await createTestClient()
        const collection = await prepareTheDataset(client)
        const queryStr = '/* | count '
        const resultSet = await queryDoc(collection, queryStr)
        expect(1).toBe(resultSet.count)
    })

    test('test query with projection', async () => {
        const client = await createTestClient()
        const collection = await prepareTheDataset(client)
        const queryStr = '/* |/{firstName} '
        const resultSet = await queryDoc(collection, queryStr)
        expect(1).toBe(resultSet.docs.length)
        expect(false).toBe('lastName' in resultSet.docs[0])
    })

    test('test query with limit', async () => {
        const client = await createTestClient()
        const collection = await prepareTheDataset(client)
        const queryStr = '/* | limit 1'
        const resultSet = await queryDoc(collection, queryStr)
        expect(1).toBe(resultSet.docs.length)
        expect(resultSet.docs[0].doc['firstName']).toBe('John')
        expect(resultSet.docs[0].doc['lastName']).toBe('Doe')
    })
})
