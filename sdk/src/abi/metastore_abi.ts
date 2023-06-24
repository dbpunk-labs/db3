//
// metastore_abi.ts
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

export const db3MetaStoreContractConfig = {
    address: '0xb9709cE5E749b80978182db1bEdfb8c7340039A9',
    abi: [
        {
            inputs: [
                {
                    internalType: 'uint64',
                    name: 'networkId',
                    type: 'uint64',
                },
                {
                    internalType: 'string',
                    name: 'rollupNodeUrl',
                    type: 'string',
                },
                {
                    internalType: 'address',
                    name: 'rollupNodeAddress',
                    type: 'address',
                },
                {
                    internalType: 'string[]',
                    name: 'indexNodeUrls',
                    type: 'string[]',
                },
                {
                    internalType: 'address[]',
                    name: 'indexNodeAddresses',
                    type: 'address[]',
                },
            ],
            name: 'registerNetwork',
            outputs: [],
            stateMutability: 'nonpayable',
            type: 'function',
        },
    ],
} as const
