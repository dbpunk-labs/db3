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
    address: '0x5FbDB2315678afecb367f032d93F642f64180aa3',
    abi: [
        {
            inputs: [
                {
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    internalType: 'address',
                    name: 'db',
                    type: 'address',
                },
                {
                    internalType: 'bytes32',
                    name: 'name',
                    type: 'bytes32',
                },
                {
                    internalType: 'bytes32',
                    name: 'licenseName',
                    type: 'bytes32',
                },
                {
                    internalType: 'bytes32',
                    name: 'licenseContent',
                    type: 'bytes32',
                },
            ],
            name: 'createCollection',
            outputs: [],
            stateMutability: 'nonpayable',
            type: 'function',
        },
        {
            inputs: [
                {
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    internalType: 'bytes32',
                    name: 'description',
                    type: 'bytes32',
                },
            ],
            name: 'createDocDatabase',
            outputs: [],
            stateMutability: 'nonpayable',
            type: 'function',
        },
        {
            inputs: [
                {
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    internalType: 'address',
                    name: 'db',
                    type: 'address',
                },
                {
                    internalType: 'bytes32',
                    name: 'name',
                    type: 'bytes32',
                },
            ],
            name: 'getCollection',
            outputs: [
                {
                    components: [
                        {
                            internalType: 'bytes32',
                            name: 'name',
                            type: 'bytes32',
                        },
                        {
                            internalType: 'bytes32',
                            name: 'licenseName',
                            type: 'bytes32',
                        },
                        {
                            internalType: 'bytes32',
                            name: 'licenseContent',
                            type: 'bytes32',
                        },
                        {
                            internalType: 'bool',
                            name: 'created',
                            type: 'bool',
                        },
                    ],
                    internalType: 'struct Types.Collection',
                    name: 'collection',
                    type: 'tuple',
                },
            ],
            stateMutability: 'view',
            type: 'function',
        },
        {
            inputs: [
                {
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
            ],
            name: 'getDataNetwork',
            outputs: [
                {
                    components: [
                        {
                            internalType: 'uint256',
                            name: 'id',
                            type: 'uint256',
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
                        {
                            internalType: 'address',
                            name: 'admin',
                            type: 'address',
                        },
                        {
                            internalType: 'bytes32',
                            name: 'latestArweaveTx',
                            type: 'bytes32',
                        },
                        {
                            internalType: 'uint256',
                            name: 'latestRollupTime',
                            type: 'uint256',
                        },
                        {
                            internalType: 'bytes32',
                            name: 'description',
                            type: 'bytes32',
                        },
                    ],
                    internalType: 'struct Types.DataNetwork',
                    name: 'dataNetwork',
                    type: 'tuple',
                },
            ],
            stateMutability: 'view',
            type: 'function',
        },
        {
            inputs: [
                {
                    internalType: 'uint256',
                    name: 'id',
                    type: 'uint256',
                },
                {
                    internalType: 'address',
                    name: 'db',
                    type: 'address',
                },
            ],
            name: 'getDatabase',
            outputs: [
                {
                    components: [
                        {
                            internalType: 'address',
                            name: 'db',
                            type: 'address',
                        },
                        {
                            internalType: 'address',
                            name: 'sender',
                            type: 'address',
                        },
                        {
                            internalType: 'bytes32',
                            name: 'description',
                            type: 'bytes32',
                        },
                        {
                            internalType: 'uint64',
                            name: 'counter',
                            type: 'uint64',
                        },
                    ],
                    internalType: 'struct Types.Database',
                    name: 'database',
                    type: 'tuple',
                },
            ],
            stateMutability: 'view',
            type: 'function',
        },
        {
            inputs: [
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
                {
                    internalType: 'bytes32',
                    name: 'description',
                    type: 'bytes32',
                },
            ],
            name: 'registerDataNetwork',
            outputs: [],
            stateMutability: 'nonpayable',
            type: 'function',
        },
        {
            inputs: [
                {
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    internalType: 'address',
                    name: 'db',
                    type: 'address',
                },
                {
                    internalType: 'address',
                    name: 'to',
                    type: 'address',
                },
            ],
            name: 'transferDatabase',
            outputs: [],
            stateMutability: 'nonpayable',
            type: 'function',
        },
        {
            inputs: [
                {
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    internalType: 'address',
                    name: 'to',
                    type: 'address',
                },
            ],
            name: 'transferNetwork',
            outputs: [],
            stateMutability: 'nonpayable',
            type: 'function',
        },
        {
            inputs: [
                {
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
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
            name: 'updateIndexNodes',
            outputs: [],
            stateMutability: 'nonpayable',
            type: 'function',
        },
        {
            inputs: [
                {
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
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
            ],
            name: 'updateRollupNode',
            outputs: [],
            stateMutability: 'nonpayable',
            type: 'function',
        },
        {
            inputs: [
                {
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    internalType: 'bytes32',
                    name: 'latestArweaveTx',
                    type: 'bytes32',
                },
            ],
            name: 'updateRollupSteps',
            outputs: [],
            stateMutability: 'nonpayable',
            type: 'function',
        },
        {
            anonymous: false,
            inputs: [
                {
                    indexed: false,
                    internalType: 'address',
                    name: 'sender',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    indexed: false,
                    internalType: 'address',
                    name: 'db',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'bytes32',
                    name: 'name',
                    type: 'bytes32',
                },
            ],
            name: 'CreateCollection',
            type: 'event',
        },
        {
            anonymous: false,
            inputs: [
                {
                    indexed: true,
                    internalType: 'address',
                    name: 'sender',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    indexed: false,
                    internalType: 'address',
                    name: 'databaseAddress',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'bytes32',
                    name: 'desc',
                    type: 'bytes32',
                },
            ],
            name: 'CreateDatabase',
            type: 'event',
        },
        {
            anonymous: false,
            inputs: [
                {
                    indexed: true,
                    internalType: 'address',
                    name: 'sender',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
            ],
            name: 'CreateNetwork',
            type: 'event',
        },
        {
            anonymous: false,
            inputs: [
                {
                    indexed: false,
                    internalType: 'address',
                    name: 'sender',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    indexed: false,
                    internalType: 'uint256',
                    name: 'forkedNetworkId',
                    type: 'uint256',
                },
            ],
            name: 'ForkNetwork',
            type: 'event',
        },
        {
            anonymous: false,
            inputs: [
                {
                    indexed: true,
                    internalType: 'address',
                    name: 'sender',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    indexed: false,
                    internalType: 'address',
                    name: 'db',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'address',
                    name: 'to',
                    type: 'address',
                },
            ],
            name: 'TransferDatabase',
            type: 'event',
        },
        {
            anonymous: false,
            inputs: [
                {
                    indexed: true,
                    internalType: 'address',
                    name: 'sender',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    indexed: false,
                    internalType: 'address',
                    name: 'to',
                    type: 'address',
                },
            ],
            name: 'TransferNetwork',
            type: 'event',
        },
        {
            anonymous: false,
            inputs: [
                {
                    indexed: false,
                    internalType: 'address',
                    name: 'sender',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    indexed: false,
                    internalType: 'address[]',
                    name: 'indexNodeAddresses',
                    type: 'address[]',
                },
                {
                    indexed: false,
                    internalType: 'string[]',
                    name: 'indexNodeUrls',
                    type: 'string[]',
                },
            ],
            name: 'UpdateIndexNode',
            type: 'event',
        },
        {
            anonymous: false,
            inputs: [
                {
                    indexed: false,
                    internalType: 'address',
                    name: 'sender',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    indexed: false,
                    internalType: 'address',
                    name: 'rollupNodeAddress',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'string',
                    name: 'rollupNodeUrl',
                    type: 'string',
                },
            ],
            name: 'UpdateRollupNode',
            type: 'event',
        },
        {
            anonymous: false,
            inputs: [
                {
                    indexed: false,
                    internalType: 'address',
                    name: 'sender',
                    type: 'address',
                },
                {
                    indexed: false,
                    internalType: 'uint256',
                    name: 'networkId',
                    type: 'uint256',
                },
                {
                    indexed: false,
                    internalType: 'bytes32',
                    name: 'arweaveTx',
                    type: 'bytes32',
                },
            ],
            name: 'UpdateRollupStep',
            type: 'event',
        },
    ],
} as const
