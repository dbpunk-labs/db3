//
// provider.test.ts
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
    Mutation,
    MutationAction,
    Mutation_BodyWrapper,
    DocumentDatabaseMutation,
} from '../src/proto/db3_mutation_v2'
import { StorageProviderV2 } from '../src/provider/storage_provider_v2'
import { toB64, fromB64, fromHEX } from '../src/crypto/crypto_utils'
import {
    createRandomAccount,
    createFromPrivateKey,
} from '../src/account/db3_account'

describe('test db3.js provider module', () => {

    test('provider send mutation test', async () => {
        const privateKey =
            '0xad689d9b7751da07b0fb39c5091672cbfe50f59131db015f8a0e76c9790a6fcc'
        const db3_account = createFromPrivateKey(privateKey)
        expect(db3_account.address).toBe(
            '0xc793b74C568a3953a82C150FDcD0F7D27b60f8Ba'
        )
        const provider = new StorageProviderV2(
            'http://127.0.0.1:26619',
            db3_account
        )
        const docDatabaseMutation: DocumentDatabaseMutation = {
            dbDesc: 'desc',
        }
        const body: Mutation_BodyWrapper = {
            body: { oneofKind: 'docDatabaseMutation', docDatabaseMutation },
            dbAddress: new Uint8Array(0),
        }
        const dm: Mutation = {
            action: MutationAction.CreateDocumentDB,
            bodies: [body],
        }
        const nonce = await provider.getNonce()
        const payload = Mutation.toBinary(dm)
        const response = await provider.sendMutation(payload, nonce)
        const response1 = await provider.getMutationBody(response.id)
        const [td, m, sig] = provider.parseMutationBody(response1.body)
        expect(m.action).toBe(dm.action)
        expect(td.message.nonce).toBe(nonce)
        const response2 = await provider.sendMutation(payload, '1')
        expect(response2.code).toBe(1)
    })

    test('provider get mutation header test', async () => {
        const db3_account = createRandomAccount()
        const provider = new StorageProviderV2(
            'http://127.0.0.1:26619',
            db3_account
        )
        const nonce = await provider.getNonce()
        expect(nonce).toBe('1')
        const docDatabaseMutation: DocumentDatabaseMutation = {
            dbDesc: 'desc',
        }
        const body: Mutation_BodyWrapper = {
            body: { oneofKind: 'docDatabaseMutation', docDatabaseMutation },
            dbAddress: new Uint8Array(0),
        }
        const dm: Mutation = {
            action: MutationAction.CreateDocumentDB,
            bodies: [body],
        }
        const payload = Mutation.toBinary(dm)
        const response = await provider.sendMutation(payload, nonce)
        expect(response.code).toBe(0)
        const mutation_header = await provider.getMutationHeader(
            response.block,
            response.order
        )
        if (mutation_header.header) {
            expect(mutation_header.header.blockId).toBe(response.block)
            expect(mutation_header.header.orderId).toBe(response.order)
        } else {
            expect(1).toBe(0)
        }
        const mutation_body = await provider.getMutationBody(response.id)
        if (mutation_body.body) {
            const [typedData, m, sig] = provider.parseMutationBody(
                mutation_body.body
            )
            expect(m.action).toBe(MutationAction.CreateDocumentDB)
        } else {
            expect(1).toBe(0)
        }
    })
})
