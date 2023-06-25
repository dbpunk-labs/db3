//
// secp256k1_publickey.ts
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

import sha3 from 'js-sha3'
import { fromB64, toB64 } from './crypto_utils'
import {
    bytesEqual,
    PublicKey,
    PublicKeyInitData,
    SIGNATURE_SCHEME_TO_FLAG,
} from './publickey'

const SECP256K1_PUBLIC_KEY_SIZE = 33

/**
 * A Secp256k1 public key
 */
export class Secp256k1PublicKey implements PublicKey {
    private data: Uint8Array

    /**
     * Create a new Secp256k1PublicKey object
     * @param value secp256k1 public key as buffer or base-64 encoded string
     */
    constructor(value: PublicKeyInitData) {
        if (typeof value === 'string') {
            this.data = fromB64(value)
        } else if (value instanceof Uint8Array) {
            this.data = value
        } else {
            this.data = Uint8Array.from(value)
        }

        if (this.data.length !== SECP256K1_PUBLIC_KEY_SIZE) {
            throw new Error(
                `Invalid public key input. Expected ${SECP256K1_PUBLIC_KEY_SIZE} bytes, got ${this.data.length}`
            )
        }
    }

    /**
     * Checks if two Secp256k1 public keys are equal
     */
    equals(publicKey: Secp256k1PublicKey): boolean {
        return bytesEqual(this.toBytes(), publicKey.toBytes())
    }

    /**
     * Return the base-64 representation of the Secp256k1 public key
     */
    toBase64(): string {
        return toB64(this.toBytes())
    }

    /**
     * Return the byte array representation of the Secp256k1 public key
     */
    toBytes(): Uint8Array {
        return this.data
    }

    /**
     * Return the base-64 representation of the Secp256k1 public key
     */
    toString(): string {
        return this.toBase64()
    }

    /**
     * Return the db3 address associated with this Secp256k1 public key
     */
    toAddress(): string {
        let tmp = new Uint8Array(SECP256K1_PUBLIC_KEY_SIZE + 1)
        tmp.set([SIGNATURE_SCHEME_TO_FLAG['Secp256k1']])
        tmp.set(this.toBytes(), 1)
        return '0x' + sha3.sha3_256(tmp).slice(0, 40)
    }
    /**
     * Return the evm address associated with this Secp256k1 public key
     */
    toEvmAddress(): string {
        return '0x' + sha3.sha3_256(this.toBytes()).slice(0, 40)
    }
}
