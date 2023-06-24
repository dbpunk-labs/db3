//
// database_v2.ts
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

import type {
    CreateCollectionResult,
    Database,
    CreateDBResult,
    MutationResult,
    Collection,
} from './types'

import {
    Mutation,
    MutationAction,
    CollectionMutation,
    DocumentMutation,
    DocumentMask,
    Mutation_BodyWrapper,
    DocumentDatabaseMutation,
    EventDatabaseMutation,
} from '../proto/db3_mutation_v2'

import { Client } from '../client/types'
import { toHEX, fromHEX } from '../crypto/crypto_utils'
import { Index } from '../proto/db3_database_v2'

/**
 *
 * Create an event database to store contract events
 *
 * ```ts
 * const {db, result} = await createEventDatabase(client, "my_db")
 * ```
 * @param client - the db3 client instance
 * @param desc   - the description for the database
 * @returns the {@link CreateDBResult}
 *
 **/
export async function createEventDatabase(
    client: Client,
    desc: string,
    contractAddress: string,
    tables: string[],
    abi: string,
    evmNodeUrl: string
) {
    const collections = tables.map((name) => {
        const collection: CollectionMutation = {
            indexFields: [],
            collectionName: name,
        }
        return collection
    })

    const mutation: EventDatabaseMutation = {
        contractAddress,
        ttl: '0',
        desc,
        tables: collections,
        eventsJsonAbi: abi,
        evmNodeUrl,
    }
    const body: Mutation_BodyWrapper = {
        body: {
            oneofKind: 'eventDatabaseMutation',
            eventDatabaseMutation: mutation,
        },
        dbAddress: new Uint8Array(0),
    }

    const dm: Mutation = {
        action: MutationAction.CreateEventDB,
        bodies: [body],
    }

    const payload = Mutation.toBinary(dm)
    const response = await client.provider.sendMutation(
        payload,
        client.nonce.toString()
    )
    if (response.code == 0) {
        client.nonce += 1
        return {
            db: {
                addr: response.items[0].value,
                client,
            } as Database,
            result: {
                id: response.id,
                block: response.block,
                order: response.order,
            } as MutationResult,
        }
    } else {
        throw new Error('fail to create database')
    }
}
/**
 *
 * Create a document database to group the collections
 *
 * ```ts
 * const {db, result} = await createDocumentDatabase(client, "my_db")
 * ```
 * @param client - the db3 client instance
 * @param desc   - the description for the database
 * @returns the {@link CreateDBResult}
 *
 **/
export async function createDocumentDatabase(client: Client, desc: string) {
    const docDatabaseMutation: DocumentDatabaseMutation = {
        dbDesc: desc,
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
    const response = await client.provider.sendMutation(
        payload,
        client.nonce.toString()
    )
    if (response.code == 0) {
        client.nonce += 1
        return {
            db: {
                addr: response.items[0].value,
                client,
            } as Database,
            result: {
                id: response.id,
                block: response.block,
                order: response.order,
            } as MutationResult,
        }
    } else {
        throw new Error('fail to create database')
    }
}

/**
 *
 * Get the collection by an db address and collection name
 *
 * ```ts
 * const database = await getCollection("0x....", "col1", client)
 * ```
 * @param addr  - a hex format string address
 * @param name  - the name of collection
 * @param client- the db3 client instance
 * @returns the {@link Database}[]
 *
 **/
export async function getCollection(
    addr: string,
    name: string,
    client: Client
) {
    const db = await getDatabase(addr, client)
    const collections = await showCollection(db)
    const targetCollections = collections.filter((item) => item.name === name)
    if (targetCollections.length > 0) {
        return targetCollections[0]
    } else {
        throw new Error(
            'db with addr ' + addr + ' has no collection with name ' + name
        )
    }
}
/**
 *
 * Get the database by an address
 *
 * ```ts
 * const database = await getDatabase("0x....", client)
 * ```
 * @param addr - a hex format string address
 * @param client- the db3 client instance
 * @returns the {@link Database}[]
 *
 **/
export async function getDatabase(addr: string, client: Client) {
    const response = await client.provider.getDatabase(addr)
    const db = response.database
    if (!db) {
        throw new Error('db with addr ' + addr + ' does not exist')
    }
    if (db.database.oneofKind === 'docDb') {
        return {
            addr,
            client,
            internal: db,
        }
    } else {
        return {
            addr,
            client,
            internal: db,
        }
    }
}

/**
 *
 * Query the all databases created by an address
 *
 * ```ts
 * const databases = await showDatabase("0x....", client)
 * ```
 * @param owner - a hex format string address
 * @param client- the db3 client instance
 * @returns the {@link Database}[]
 *
 **/
export async function showDatabase(owner: string, client: Client) {
    const response = await client.provider.getDatabaseOfOwner(owner)
    return response.databases
        .filter((item) => item.database.oneofKind != undefined)
        .map((db) => {
            if (db.database.oneofKind === 'docDb') {
                return {
                    addr: '0x' + toHEX(db.database.docDb.address),
                    client,
                    internal: db,
                }
            } else if (db.database.oneofKind === 'eventDb') {
                return {
                    addr: '0x' + toHEX(db.database.eventDb.address),
                    client,
                    internal: db,
                }
            } else {
                //will not go here
                return {
                    addr: '',
                    client,
                    internal: undefined,
                }
            }
        })
}

/**
 *
 * Create a collection to store json documents and you can specify the index to accelerate query speed
 *
 * ```ts
 * const index1:Index = {
 *    path:'/city', // a top level field name 'city' and the path will be '/city'
 *    indexType: Indextype.StringKey
 * }
 * const {collection, result} = await createCollection(db, "test_collection", [index1])
 * ```
 * current all supported index types are 'IndexType.Uniquekey' , 'IndexType.StringKey', 'IndexType.Int64key' and 'IndexType.Doublekey'
 *
 * @param db          - the instance of database
 * @param name        - the name of collection
 * @param indexFields - the fields for index
 * @returns the {@link CreateCollectionResult}
 *
 **/
export async function createCollection(
    db: Database,
    name: string,
    indexFields: Index[]
) {
    const collection: CollectionMutation = {
        indexFields,
        collectionName: name,
    }
    const body: Mutation_BodyWrapper = {
        body: {
            oneofKind: 'collectionMutation',
            collectionMutation: collection,
        },
        dbAddress: fromHEX(db.addr),
    }
    const dm: Mutation = {
        action: MutationAction.AddCollection,
        bodies: [body],
    }
    const payload = Mutation.toBinary(dm)
    const response = await db.client.provider.sendMutation(
        payload,
        db.client.nonce.toString()
    )

    if (response.code == 0) {
        db.client.nonce += 1
        const col: Collection = {
            name,
            db,
            indexFields,
            internal: undefined,
        }

        const result: MutationResult = {
            id: response.id,
            block: response.block,
            order: response.order,
        }

        return {
            collection: col,
            result,
        } as CreateCollectionResult
    } else {
        throw new Error('fail to create collection')
    }
}

/**
 *
 * Query collections in the database
 *
 * ```ts
 * const collections = await showCollection(db)
 * ```
 *
 * @param db  - the instance of database
 * @returns the {@link Collection[]}
 *
 **/
export async function showCollection(db: Database) {
    const response = await db.client.provider.getCollectionOfDatabase(db.addr)
    const collectionList = response.collections.map((c) => {
        return {
            name: c.name,
            db,
            indexFields: c.indexFields,
            internal: c,
        } as Collection
    })
    return collectionList
}
