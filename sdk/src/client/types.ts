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

import type { DB3Account } from '../account/types'

import { StorageProviderV2 } from '../provider/storage_provider_v2'
import { IndexerProvider } from '../provider/indexer_provider'

export type Client = {
    provider: StorageProviderV2
    indexer: IndexerProvider
    account: DB3Account
    nonce: number
}
