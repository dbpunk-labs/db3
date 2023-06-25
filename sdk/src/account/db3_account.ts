//
// db3_account.ts
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
    WalletClient,
    SignTypedDataParameters,
    Hex,
    SignTypedDataReturnType,
} from 'viem'
import type { DB3Account } from './types'
import * as secp from '@noble/secp256k1'
import { createWalletClient, http, Chain, custom } from 'viem'
import { privateKeyToAccount } from 'viem/accounts'
import { mainnet } from 'viem/chains'
import { toHEX } from '../crypto/crypto_utils'

/**
 *
 * Create a {@link DB3Account} from a hex format private key
 *
 * ```ts
 * const account = createFromPrivateKey("0x........")
 * ```
 * @param privatekey - a hex format private key string
 * @returns the instance of {@link DB3ACCOUNT}
 *
 **/
export function createFromPrivateKey(privateKey: Hex) {
    const account = privateKeyToAccount(privateKey)
    const client = createWalletClient({
        account,
        chain: mainnet,
        transport: http(),
    })
    const address = account.address
    return {
        client,
        address,
    } as DB3Account
}

/**
 *
 * Generate a {@link DB3Account} from a random private key
 *
 * ```ts
 * const account = createRandomAccount()
 * ```
 * @returns the instance of {@link DB3ACCOUNT}
 *
 **/
export function createRandomAccount() {
    const rawKey = '0x' + toHEX(secp.utils.randomPrivateKey())
    return createFromPrivateKey(rawKey as Hex)
}

export async function createFromExternal(chain: Chain) {
    const [account] = await window.ethereum.request({
        method: 'eth_requestAccounts',
    })
    const client = createWalletClient({
        account,
        chain,
        transport: custom(window.ethereum),
    })
    const [address] = await client.getAddresses()
    return {
        client,
        address,
    } as DB3Account
}

/**
 * Signs typed data and calculates an Ethereum-specific signature in [https://eips.ethereum.org/EIPS/eip-712](https://eips.ethereum.org/EIPS/eip-712): `sign(keccak256("\x19\x01" ‖ domainSeparator ‖ hashStruct(message)))`
 *
 * - JSON-RPC Methods:
 *   - JSON-RPC Accounts: [`eth_signTypedData_v4`](https://docs.metamask.io/guide/signing-data.html#signtypeddata-v4)
 *   - Local Accounts: Signs locally. No JSON-RPC request.
 *
 * ```ts
 *  const message = {
 *      types: {
 *          EIP712Domain: [],
 *          Message: [
 *              { name: 'payload', type: 'bytes' },
 *              { name: 'nonce', type: 'string' },
 *          ],
 *      },
 *      domain: {},
 *      primaryType: 'Message',
 *      message: {
 *          payload: '0x',
 *          nonce: nonce,
 *      },
 *  }
 *  const signature = await signTypedData(account, message)
 * ```
 * @param client - Client to use
 * @param parameters - {@link SignTypedDataParameters}
 * @returns The signed data. {@link SignTypedDataReturnType}
 *
 **/
export async function signTypedData(
    account: DB3Account,
    data: SignTypedDataParameters
): Promise<SignTypedDataReturnType> {
    return account.client.signTypedData(data)
}
