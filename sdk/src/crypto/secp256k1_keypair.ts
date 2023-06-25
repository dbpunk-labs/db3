//
// secp256k1_keypair.ts
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

import * as secp from '@noble/secp256k1'
import type { ExportedKeypair, Keypair } from './keypair'
import type { TypedData } from '../wallet/wallet'
import {
    SIGNATURE_SCHEME_TO_FLAG,
    PublicKey,
    SignatureScheme,
} from './publickey'
import { hmac } from '@noble/hashes/hmac'
import { sha256 } from '@noble/hashes/sha256'
import { Secp256k1PublicKey } from './secp256k1_publickey'
import { Signature } from '@noble/secp256k1'
import { isValidBIP32Path, mnemonicToSeed } from './mnemonics'
import { toB64 } from './crypto_utils'
import { HDKey } from '@scure/bip32'
import {
    TypedDataUtils,
    SignTypedDataVersion,
    TypedMessage,
    MessageTypes,
} from '@metamask/eth-sig-util'

export const DEFAULT_SECP256K1_DERIVATION_PATH = "m/54'/784'/0'/0/0"
const SECP256K1_SIGNATURE_LEN = 65
const SECP256K1_PUBLIC_LEN = 33
const DB3_SECP256K1_SIGNATURE_LEN =
    SECP256K1_SIGNATURE_LEN + SECP256K1_PUBLIC_LEN + 1

secp.utils.hmacSha256Sync = (key: Uint8Array, ...msgs: Uint8Array[]) => {
    const h = hmac.create(sha256, key)
    msgs.forEach((msg) => h.update(msg))
    return h.digest()
}

/**
 * Secp256k1 Keypair data
 */
export interface Secp256k1KeypairData {
    publicKey: Uint8Array
    secretKey: Uint8Array
}

/**
 * An Secp256k1 Keypair used for signing transactions.
 */
export class Secp256k1Keypair implements Keypair {
    private keypair: Secp256k1KeypairData

    /**
     * Create a new keypair instance.
     * Generate random keypair if no {@link Secp256k1Keypair} is provided.
     *
     * @param keypair secp256k1 keypair
     */
    constructor(keypair?: Secp256k1KeypairData) {
        if (keypair) {
            this.keypair = keypair
        } else {
            const secretKey: Uint8Array = secp.utils.randomPrivateKey()
            const publicKey: Uint8Array = secp.getPublicKey(secretKey, true)
            this.keypair = { publicKey, secretKey }
        }
    }

    /**
     * Get the key scheme of the keypair Secp256k1
     */
    getKeyScheme(): SignatureScheme {
        return 'Secp256k1'
    }

    /**
     * Generate a new random keypair
     */
    static generate(): Secp256k1Keypair {
        const secretKey = secp.utils.randomPrivateKey()
        const publicKey = secp.getPublicKey(secretKey, true)
        return new Secp256k1Keypair({ publicKey, secretKey })
    }

    /**
     * Create a keypair from a raw secret key byte array.
     *
     * This method should only be used to recreate a keypair from a previously
     * generated secret key. Generating keypairs from a random seed should be done
     * with the {@link Keypair.fromSeed} method.
     *
     * @throws error if the provided secret key is invalid and validation is not skipped.
     *
     * @param secretKey secret key byte array
     * @param options: skip secret key validation
     */

    static fromSecretKey(secretKey: Uint8Array): Secp256k1Keypair {
        const publicKey: Uint8Array = secp.getPublicKey(secretKey, true)
        return new Secp256k1Keypair({ publicKey, secretKey })
    }

    /**
     * Generate a keypair from a 32 byte seed.
     *
     * @param seed seed byte array
     */
    static fromSeed(seed: Uint8Array): Secp256k1Keypair {
        let publicKey = secp.getPublicKey(seed, true)
        return new Secp256k1Keypair({ publicKey, secretKey: seed })
    }

    /**
     * The public key for this keypair
     */
    getPublicKey(): PublicKey {
        return new Secp256k1PublicKey(this.keypair.publicKey)
    }

    /**
     * Return the signature for the provided data.
     */
    signData(data: Uint8Array | TypedData): Uint8Array {
        if (data instanceof Uint8Array) {
            const msgHash = sha256(data)
            const [sig, rec_id] = secp.signSync(
                msgHash,
                this.keypair.secretKey,
                {
                    canonical: true,
                    recovered: true,
                }
            )
            var buf = new Uint8Array(DB3_SECP256K1_SIGNATURE_LEN)
            buf[0] = SIGNATURE_SCHEME_TO_FLAG['Secp256k1']
            buf.set(Signature.fromDER(sig).toCompactRawBytes(), 1)
            buf.set([rec_id], 65)
            buf.set(this.keypair.publicKey, 66)
            return buf
        } else {
            const hashedmsg = TypedDataUtils.eip712Hash(
                data as TypedMessage<MessageTypes>,
                SignTypedDataVersion.V3
            )
            const [sig, rec_id] = secp.signSync(
                hashedmsg,
                this.keypair.secretKey,
                {
                    canonical: true,
                    recovered: true,
                }
            )
            var buf = new Uint8Array(DB3_SECP256K1_SIGNATURE_LEN)
            buf[0] = SIGNATURE_SCHEME_TO_FLAG['Secp256k1']
            buf.set(Signature.fromDER(sig).toCompactRawBytes(), 1)
            buf.set([rec_id], 65)
            buf.set(this.keypair.publicKey, 66)
            return buf
        }
    }

    /**
     * Derive Secp256k1 keypair from mnemonics and path. The mnemonics must be normalized
     * and validated against the english wordlist.
     *
     * If path is none, it will default to m/54'/784'/0'/0/0, otherwise the path must
     * be compliant to BIP-32 in form m/54'/784'/{account_index}'/{change_index}/{address_index}.
     */
    static deriveKeypair(mnemonics: string, path?: string): Secp256k1Keypair {
        if (path == null) {
            path = DEFAULT_SECP256K1_DERIVATION_PATH
        }

        if (!isValidBIP32Path(path)) {
            throw new Error('Invalid derivation path')
        }
        const key = HDKey.fromMasterSeed(mnemonicToSeed(mnemonics)).derive(path)
        if (key.publicKey == null || key.privateKey == null) {
            throw new Error('Invalid key')
        }
        return new Secp256k1Keypair({
            publicKey: key.publicKey,
            secretKey: key.privateKey,
        })
    }

    export(): ExportedKeypair {
        return {
            schema: 'Secp256k1',
            privateKey: toB64(this.keypair.secretKey),
        }
    }
}
