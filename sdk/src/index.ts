/**
 * Copyright 2023 db3 network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export {
    createFromPrivateKey,
    createRandomAccount,
    signTypedData,
    createFromExternal,
} from './account/db3_account'

export type { DB3Account } from './account/types'

export type { Client } from './client/types'

export { addDoc, updateDoc, deleteDoc, queryDoc } from './store/document_v2'

export {
    createClient,
    syncAccountNonce,
    getMutationHeader,
    getMutationBody,
    scanMutationHeaders,
    scanGcRecords,
    scanRollupRecords,
    getStorageNodeStatus,
    getIndexNodeStatus,
    setupStorageNode,
    getContractSyncStatus,
} from './client/client_v2'

export {
    createDocumentDatabase,
    createEventDatabase,
    showDatabase,
    createCollection,
    showCollection,
    getDatabase,
    getCollection,
} from './store/database_v2'

export { Index, IndexType } from './proto/db3_database_v2'

export { MutationHeader, MutationBody } from './proto/db3_mutation_v2'

export { db3MetaStoreContractConfig } from './abi/metastore_abi'
