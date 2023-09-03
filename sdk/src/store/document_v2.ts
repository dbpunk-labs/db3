//
// document_v2.ts
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
    DocumentMutation,
    DocumentMask,
    Mutation_BodyWrapper,
    MutationAction,
} from '../proto/db3_mutation_v2'
import { BSON } from 'db3-bson'
import { fromHEX } from '../crypto/crypto_utils'
import type { Collection, QueryResult } from './types'
import type { Query, QueryParameter } from '../proto/db3_database_v2'
import type { DocumentData, DocumentEntry } from '../client/base'

async function runQueryInternal<T>(col: Collection, query: Query) {
    const response = await col.db.client.indexer.runQuery(
        col.db.addr,
        col.name,
        query
    )
    const entries = response.documents.map((doc) => {
        return {
            doc: JSON.parse(doc.doc) as T,
            id: doc.id,
        } as DocumentEntry<T>
    })

    return {
        docs: entries,
        collection: col,
        count: response.count,
    } as QueryResult<T>
}

/**
 *
 * Query document with a query language
 *
 * The usage of query language
 * for example we have a document looks like the following
 * ```json
 *     {
 *     "firstName": "John",
 *     "lastName": "Doe",
 *     "age": 28,
 *     "pets": [
 *       {"name": "Rexy rex", "kind": "dog", "likes": ["bones", "jumping", "toys"]},
 *       {"name": "Grenny", "kind": "parrot", "likes": ["green color", "night", "toys"]}
 *     ]
 *    }
 * ```
 * 1. Query one document from collection
 * ```ts
 * const queryStr = '/* | limit 1'
 * const resultSet = await queryDoc<Profile>(collection, queryStr)
 * ```
 * 2. Query documents with filter
 * ```ts
 * const queryByName = '/[firstname="John"]'
 * const resultSet = await queryDoc<Profile>(collection, queryByName)
 * const queryByFirstAndLast = '/[firstName="John"] and [lastName="Doe"]'
 * const resultSet = await queryDoc<Profile>(collection, queryByName)
 * ```
 * 3. Query documents with filter and projection
 * ```ts
 * // only query the firstName and lastName
 * const queryByName = '/[firstname="John"] | / {firstName, lastName}'
 * const resultSet = await queryDoc<Profile>(collection, queryByName)
 * ```
 * 4. Query documents with filter and aggregate count
 * ```ts
 * // only query the firstName and lastName
 * const queryByNameAndCount = '/[firstname="John"] | count'
 * const resultSet = await queryDoc<Profile>(collection, queryByNameAndCount)
 * ```
 *
 * @param col        - the instance of collection
 * @param queryStr   - a document query string
 * @param parameters - an optional query parameters
 * @returns the {@link QueryResult}
 *
 **/
export async function queryDoc<T = DocumentData>(
    col: Collection,
    queryStr: string,
    parameters?: QueryParameter[]
) {
    if (!parameters) {
        const query: Query = {
            queryStr,
            parameters: [],
        }
        return runQueryInternal(col, query)
    } else {
        const query: Query = {
            queryStr,
            parameters,
        }
        return runQueryInternal<T>(col, query)
    }
}

/**
 *
 * This function gets a document from the database by its ID.
 *
 * ```ts
 * const doc = await getDoc(collection, "10")
 * const id = doc.id
 * const content = doc.doc
 * ```
 * @param col    - the instance of collection
 * @param id     - the id of document
 * @returns the {@link DocumentEntry} if the document was found. Otherwise, raises an error.
 **/
export async function getDoc<T = DocumentData>(col: Collection, id: string) {
    const response = await col.db.client.indexer.getDoc(
        col.db.addr,
        col.name,
        id
    )
    if (response.document) {
        return {
            doc: JSON.parse(response.document.doc) as T,
            id: response.document.id,
        } as DocumentEntry<T>
    } else {
        throw new Error('no document was found with id ' + id)
    }
}

export async function deleteDoc(col: Collection, ids: string[]) {
    const documentMutation: DocumentMutation = {
        collectionName: col.name,
        documents: [],
        ids,
        masks: [],
    }
    const body: Mutation_BodyWrapper = {
        body: {
            oneofKind: 'documentMutation',
            documentMutation,
        },
        dbAddress: fromHEX(col.db.addr),
    }

    const dm: Mutation = {
        action: MutationAction.DeleteDocument,
        bodies: [body],
    }

    const payload = Mutation.toBinary(dm)
    const response = await col.db.client.provider.sendMutation(
        payload,
        col.db.client.nonce.toString()
    )

    if (response.code == 0) {
        col.db.client.nonce += 1
        return {
            mid: response.id,
            block: response.block,
            order: response.order,
        }
    } else {
        throw new Error('fail to delete doc')
    }
}

export async function updateDoc(
    col: Collection,
    id: string,
    doc: DocumentData
) {
    const documentMask: DocumentMask = {
        fields: [],
    }
    const documentMutation: DocumentMutation = {
        collectionName: col.name,
        documents: [BSON.serialize(doc)],
        ids: [id],
        masks: [documentMask],
    }
    const body: Mutation_BodyWrapper = {
        body: {
            oneofKind: 'documentMutation',
            documentMutation,
        },
        dbAddress: fromHEX(col.db.addr),
    }
    const dm: Mutation = {
        action: MutationAction.UpdateDocument,
        bodies: [body],
    }
    const payload = Mutation.toBinary(dm)
    const response = await col.db.client.provider.sendMutation(
        payload,
        col.db.client.nonce.toString()
    )
    if (response.code == 0) {
        col.db.client.nonce += 1
        return {
            mid: response.id,
            block: response.block,
            order: response.order,
        }
    } else {
        throw new Error('fail to update doc')
    }
}

/**
 * Add a document to the collection.
 *
 * @param col The collection to add the document to.
 * @param doc The document to add.
 * @returns The ID of the newly added document.
 */
export async function addDoc(col: Collection, doc: DocumentData) {
    const documentMutation: DocumentMutation = {
        collectionName: col.name,
        documents: [BSON.serialize(doc)],
        ids: [],
        masks: [],
    }
    const body: Mutation_BodyWrapper = {
        body: {
            oneofKind: 'documentMutation',
            documentMutation,
        },
        dbAddress: fromHEX(col.db.addr),
    }

    const dm: Mutation = {
        action: MutationAction.AddDocument,
        bodies: [body],
    }

    const payload = Mutation.toBinary(dm)
    const response = await col.db.client.provider.sendMutation(
        payload,
        col.db.client.nonce.toString()
    )

    if (response.code == 0 && response.items.length > 0) {
        col.db.client.nonce += 1
        return {
            mid: response.id,
            block: response.block,
            order: response.order,
            id: response.items[0].value,
        }
    } else {
        throw new Error(
            'fail to addDoc, maybe you can syncAccountNonce to resolve the problem'
        )
    }
}
