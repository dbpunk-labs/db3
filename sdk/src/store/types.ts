//
// types.ts
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

import { Client } from '../client/types'
import { DocumentEntry, DocumentData } from '../client/base'
import {
    DatabaseMessage as InternalDatabase,
    Index,
    Collection as InternalCollection,
} from '../proto/db3_database_v2'

export type CreateDBResult = {
    db: Database
    result: MutationResult
}

export type CreateCollectionResult = {
    collection: Collection
    result: MutationResult
}

export type Database = {
    addr: string
    client: Client
    internal: InternalDatabase | undefined
}

export type MutationResult = {
    id: string
    block: string
    order: number
}

export type Collection = {
    name: string
    db: Database
    indexFields: Index[]
    internal: InternalCollection | undefined
}

export type QueryResult<T = DocumentData> = {
    docs: Array<DocumentEntry<T>>
    collection: Collection
}

export type EventDatabaseOption = {
    ttl: string
}
