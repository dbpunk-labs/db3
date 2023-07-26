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
    AddIndexMutation,
    DeleteEventDatabaseMutation,
} from '../proto/db3_mutation_v2'

import { Client, ReadClient } from '../client/types'
import { toHEX, fromHEX } from '../crypto/crypto_utils'
import { Index } from '../proto/db3_database_v2'

/**
 *
 * Delete the event database
 *
 * ```ts
 * const result = await deleteEventDatabase(client,
 *                                          "0x....")
 * ```
 * @param client            - the client instance
 * @param address           - the address of event database
 * @returns the {@link MutationResult}
 * @note only the owner of event database can delete the event database
 *
 **/
export async function deleteEventDatabase(client: Client, dbAddress: string) {
    const mutation: DeleteEventDatabaseMutation = {}
    const body: Mutation_BodyWrapper = {
        body: {
            oneofKind: 'deleteEventDatabaseMutation',
            deleteEventDatabaseMutation: mutation,
        },
        dbAddress: fromHEX(dbAddress),
    }

    const dm: Mutation = {
        action: MutationAction.DeleteEventDB,
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
            id: response.id,
            block: response.block,
            order: response.order,
        } as MutationResult
    } else {
        throw new Error('fail to create database')
    }
}

/**
 *
 * Create an event database to store contract events
 *
 * ```ts
 * const {db, result} = await createEventDatabase(client,
 *        "my contract event db",
 *        "0x...",
 *        ["DepositEvent"],
 *        "{...}",
 *        "wss://xxxxx",
 *        "100000"
 *        )
 * ```
 * @param client            - the client instance
 * @param desc              - the description for the event database
 * @param contractAddress   - the contract address
 * @param tables            - the contract event list
 * @param abi               - the json abi of contract
 * @param evmNodeUrl        - the websocket url of evm node
 * @param startBlock        - the start block to subscribe, 0 start from the latest block
 * @returns the {@link CreateDBResult}
 *
 **/
export async function createEventDatabase(
    client: Client,
    desc: string,
    contractAddress: string,
    tables: string[],
    abi: string,
    evmNodeUrl: string,
    startBlock: string
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
        startBlock,
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
 * Add index the existing Collection
 *
 * ```ts
 *
 *  const index:Index = {
 *    path:'/city', // a top level field name 'city' and the path will be '/city'
 *    indexType: IndexType.StringKey
 *  }
 *  const result = await addIndex(collection, [index])
 * ```
 * @param client    - the db3 client instance
 * @param indexes   - the index list of {@link Index}
 * @returns the {@link MutationResult}
 *
 **/
export async function addIndex(collection: Collection, indexes: Index[]) {
    if (indexes.filter((item) => !item.path.startsWith('/')).length > 0) {
        throw new Error('the index path must start with /')
    }
    const addIndexMutation: AddIndexMutation = {
        collectionName: collection.name,
        indexFields: indexes,
    }

    const body: Mutation_BodyWrapper = {
        body: { oneofKind: 'addIndexMutation', addIndexMutation },
        dbAddress: fromHEX(collection.db.addr),
    }

    const dm: Mutation = {
        action: MutationAction.AddIndex,
        bodies: [body],
    }
    const payload = Mutation.toBinary(dm)
    try {
        const response = await collection.db.client.provider.sendMutation(
            payload,
            collection.db.client.nonce.toString()
        )
        if (response.code == 0) {
            return {
                result: {
                    id: response.id,
                    block: response.block,
                    order: response.order,
                } as MutationResult,
            }
        } else {
            throw new Error('fail to add index with err ' + response.msg)
        }
    } catch (e) {
        throw e
    } finally {
        collection.db.client.nonce += 1
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
 * const collection = await getCollection("0x....", "col1", client)
 * ```
 * @param addr  - a hex format string database address
 * @param name  - the name of collection
 * @param client- the client instance
 * @returns the {@link Collection}
 *
 **/
export async function getCollection(
    addr: string,
    name: string,
    client: Client | ReadClient
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
export async function getDatabase(addr: string, client: Client | ReadClient) {
    const response = await client.provider.getDatabase(addr)
    const db = response.database
    if (!db) {
        throw new Error('db with addr ' + addr + ' does not exist')
    }
    return {
        addr,
        client,
        internal: db,
        state: response.state,
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
export async function showDatabase(owner: string, client: Client | ReadClient) {
    const response = await client.provider.getDatabaseOfOwner(owner)
    return response.databases
        .filter((item) => item.database.oneofKind != undefined)
        .map((db, index) => {
            if (db.database.oneofKind === 'docDb') {
                return {
                    addr: '0x' + toHEX(db.database.docDb.address),
                    client,
                    internal: db,
                    state: response.states[index],
                }
            } else if (db.database.oneofKind === 'eventDb') {
                return {
                    addr: '0x' + toHEX(db.database.eventDb.address),
                    client,
                    internal: db,
                    state: response.states[index],
                }
            } else {
                //will not go here
                return {
                    addr: '',
                    client,
                    internal: undefined,
                    state: response.states[index],
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
 *    indexType: IndexType.StringKey
 * }
 * const {collection, result} = await createCollection(db, "test_collection", [index1])
 * ```
 * current all supported index types are 'IndexType.Uniquekey' , 'IndexType.StringKey', 'IndexType.Int64key' and 'IndexType.Doublekey'
 *
 * @param db          - the instance of database
 * @param name        - the name of collection
 * @param indexFields - the fields for {@link Index}
 * @returns the {@link CreateCollectionResult}
 *
 **/
export async function createCollection(
    db: Database,
    name: string,
    indexFields?: Index[]
) {
    const collection: CollectionMutation = {
        indexFields: indexFields ? indexFields : [],
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
            indexFields: indexFields ? indexFields : [],
            internal: undefined,
            state: undefined,
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
    const collectionList = response.collections.map((c, index) => {
        return {
            name: c.name,
            db,
            indexFields: c.indexFields,
            internal: c,
            state: response.states[index],
        } as Collection
    })
    return collectionList
}

/**
 *
 * Query collections in the database from the index
 *
 * ```ts
 * const collections = await showCollectionFromIndex(db)
 * ```
 *
 * @param db  - the instance of database
 * @returns the {@link Collection[]}
 *
 **/
export async function showCollectionFromIndex(db: Database) {
    const response = await db.client.indexer.getCollectionOfDatabase(db.addr)
    const collectionList = response.collections.map((c, index) => {
        return {
            name: c.name,
            db,
            indexFields: c.indexFields,
            internal: c,
            state: response.states[index],
        } as Collection
    })
    return collectionList
}
