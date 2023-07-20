//
// sig_utils.ts
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
//@ts-nocheck
import { SystemConfig } from '../proto/db3_base'
import { signTypedData } from '../account/db3_account'
import type { DB3Account } from '../account/types'

export async function generate_config_sig(
    account: DB3Account,
    config: SystemConfig
) {
    const message = {
        types: {
            EIP712Domain: [],
            Message: [
                { name: 'rollupInterval', type: 'string' },
                { name: 'minRollupSize', type: 'string' },
                { name: 'networkId', type: 'string' },
                { name: 'chainId', type: 'string' },
                { name: 'contractAddr', type: 'address' },
                { name: 'rollupMaxInterval', type: 'string' },
                { name: 'evmNodeUrl', type: 'string' },
                { name: 'arNodeUrl', type: 'string' },
                { name: 'minGcOffset', type: 'string' },
            ],
        },
        domain: {},
        primaryType: 'Message',
        message: {
            rollupInterval: config.rollupInterval,
            minRollupSize: config.minRollupSize,
            networkId: config.networkId,
            chainId: config.chainId.toString(),
            contractAddr: config.contractAddr,
            rollupMaxInterval: config.rollupMaxInterval,
            evmNodeUrl: config.evmNodeUrl,
            arNodeUrl: config.arNodeUrl,
            minGcOffset: config.minGcOffset,
        },
    }

    const signature = await signTypedData(account, message)
    const payload = JSON.stringify(message)
    return [signature, payload]
}
