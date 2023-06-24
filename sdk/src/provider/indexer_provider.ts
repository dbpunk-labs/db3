//
// indexer_provider.ts
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
    GrpcWebFetchTransport,
    GrpcWebOptions,
} from '@protobuf-ts/grpcweb-transport'
import { IndexerNodeClient } from '../proto/db3_indexer.client'
import {
    RunQueryRequest,
    GetSystemStatusRequest,
    GetContractSyncStatusRequest,
} from '../proto/db3_indexer'
import { Query } from '../proto/db3_database_v2'
import { DB3Error } from './error'
import { RpcError } from '@protobuf-ts/runtime-rpc'

export class IndexerProvider {
    readonly client: IndexerNodeClient
    constructor(url: string) {
        const goptions: GrpcWebOptions = {
            baseUrl: url,
            // simple example for how to add auth headers to each request
            // see `RpcInterceptor` for documentation
            interceptors: [],
            // you can set global request headers here
            meta: {},
        }
        const transport = new GrpcWebFetchTransport(goptions)
        this.client = new IndexerNodeClient(transport)
    }

    async runQuery(db: string, colName: string, query: Query) {
        const request: RunQueryRequest = {
            db,
            colName,
            query,
        }
        try {
            const { response } = await this.client.runQuery(request)
            return response
        } catch (e) {
            throw new DB3Error(e as RpcError)
        }
    }

    async getSystemStatus() {
        const request: GetSystemStatusRequest = {}
        try {
            const { response } = await this.client.getSystemStatus(request)
            return response
        } catch (e) {
            throw new DB3Error(e as RpcError)
        }
    }

    async getContractSyncStatus() {
        const request: GetContractSyncStatusRequest = {}

        try {
            const { response } = await this.client.getContractSyncStatus(
                request
            )
            return response
        } catch (e) {
            throw new DB3Error(e as RpcError)
        }
    }
}
