//
// client_v2.ts
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

import {
    Mutation,
    MutationAction,
    CollectionMutation,
    DocumentMutation,
    DocumentMask,
    Mutation_BodyWrapper,
    DocumentDatabaseMutation,
} from '../proto/db3_mutation_v2'
import type { DocumentData, DocumentEntry } from './base'
import type { DB3Account } from '../account/types'
import type { Client } from './types'
import { Index } from '../proto/db3_database_v2'
import { StorageProviderV2 } from '../provider/storage_provider_v2'
import { IndexerProvider } from '../provider/indexer_provider'
import { fromHEX } from '../crypto/crypto_utils'
import { BSON } from 'db3-bson'

/**
 *
 * Create a client of db3 network
 *
 * ```ts
 * const client = createClient("http://127.0.0.1:26619",
 *                             "http://127.0.0.1:26620",
 *                             account)
 * ```
 *
 * @param rollup_node_url  - the url of db3 rollup node
 * @param index_node_url   - the url of db3 index node
 * @param account          - the instance of db3 account
 * @returns the client instance
 *
 **/
export function createClient(
    rollupNodeUrl: string,
    indexNodeUrl: string,
    account: DB3Account
) {
    const provider = new StorageProviderV2(rollupNodeUrl, account)
    const indexer = new IndexerProvider(indexNodeUrl)
    return {
        provider,
        indexer,
        account,
        nonce: 0,
    } as Client
}

export async function setupStorageNode(
    client: Client,
    network: string,
    rollupInterval: string,
    minRollupSize: string
) {
    return await client.provider.setup(network, rollupInterval, minRollupSize)
}

/**
 *
 * Get the system status of storage node
 *
 * ```ts
 *  const status = getStorageNodeStatus(client)
 * ```
 *
 * @param client     - the client of db3 network
 * @returns the storage system status
 *
 **/
export async function getStorageNodeStatus(client: Client) {
    const response = await client.provider.getSystemStatus()
    return response
}

/**
 *
 * Get the system status of index node
 *
 * ```ts
 *  const status = getIndexNodeStatus(client)
 * ```
 *
 * @param client     - the client of db3 network
 * @returns the Index system status
 *
 **/
export async function getIndexNodeStatus(client: Client) {
    const response = await client.indexer.getSystemStatus()
    return response
}

/**
 *
 * Get the mutation content by the id
 *
 * ```ts
 * const body = getMutationBody(client, '0x....')
 * ```
 *
 * @param client    - the instance of client
 * @param id        - the id of mutation
 * @returns the mutation
 *
 **/
export async function getMutationBody(client: Client, id: string) {
    const response = await client.provider.getMutationBody(id)
    if (response.body) {
        return client.provider.parseMutationBody(response.body)
    }
    throw new Error('mutation not found')
}

/**
 *
 * Sync the nonce of account
 *
 * ```ts
 *  const nonce = syncAccountNonce(client)
 * ```
 *
 * @param client - the instance of client
 * @returns the nonce
 *
 **/
export async function syncAccountNonce(client: Client) {
    const nonce = await client.provider.getNonce()
    client.nonce = parseInt(nonce)
    return client.nonce
}

/**
 *
 * Get the mutation header by block and order
 *
 * ```ts
 * const header = getMutationHeader(client, 1, 100)
 * ```
 *
 * @param client    - the instance of client
 * @param block     - the block id
 * @param order     - the order
 * @returns the mutation header
 *
 **/
export async function getMutationHeader(
    client: Client,
    block: string,
    order: number
) {
    const response = await client.provider.getMutationHeader(block, order)
    return response
}

export async function scanMutationHeaders(
    client: Client,
    start: number,
    limit: number
) {
    const response = await client.provider.scanMutationHeaders(start, limit)
    return response.headers
}

/**
 *
 * Scan the rollup records
 *
 * ```ts
 * const records = scanRollupRecords(client, 1, 1000)
 * ```
 *
 * @param client    - the instance of client
 * @param start     - the start offset
 * @param limit     - the records limit
 * @returns the records
 *
 **/
export async function scanRollupRecords(
    client: Client,
    start: number,
    limit: number
) {
    const response = await client.provider.scanRollupRecords(start, limit)
    return response.records
}

/**
 *
 * Scan the gc rollup records
 *
 * ```ts
 * const records = scanGcRecords(client, 1, 1000)
 * ```
 *
 * @param client    - the instance of client
 * @param start     - the start offset
 * @param limit     - the records limit
 * @returns the records
 *
 **/
export async function scanGcRecords(
    client: Client,
    start: number,
    limit: number
) {
    const response = await client.provider.scanGcRecords(start, limit)
    return response.records
}

/**
 *
 * Get the contract sync status
 *
 *
 * @param client    - the instance of client
 * @returns the records
 *
 **/
export async function getContractSyncStatus(client: Client) {
    const response = await client.indexer.getContractSyncStatus()
    return response.statusList
}
