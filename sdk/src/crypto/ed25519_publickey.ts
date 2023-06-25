//
// ed25519_publickey.ts
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
    PublicKeyInitData,
    SIGNATURE_SCHEME_TO_FLAG,
} from './publickey'

const PUBLIC_KEY_SIZE = 32

/**
 * An Ed25519 public key
 */
export class Ed25519PublicKey {
    private data: Uint8Array

    /**
     * Create a new Ed25519PublicKey object
     * @param value ed25519 public key as buffer or base-64 encoded string
     */
    constructor(value: PublicKeyInitData) {
        if (typeof value === 'string') {
            this.data = fromB64(value)
        } else if (value instanceof Uint8Array) {
            this.data = value
        } else {
            this.data = Uint8Array.from(value)
        }

        if (this.data.length !== PUBLIC_KEY_SIZE) {
            throw new Error(
                `Invalid public key input. Expected ${PUBLIC_KEY_SIZE} bytes, got ${this.data.length}`
            )
        }
    }

    /**
     * Checks if two Ed25519 public keys are equal
     */
    equals(publicKey: Ed25519PublicKey): boolean {
        return bytesEqual(this.toBytes(), publicKey.toBytes())
    }

    /**
     * Return the base-64 representation of the Ed25519 public key
     */
    toBase64(): string {
        return toB64(this.toBytes())
    }

    /**
     * Return the byte array representation of the Ed25519 public key
     */
    toBytes(): Uint8Array {
        return this.data
    }

    /**
     * Return the db3 address associated with this Ed25519 public key
     */
    toAddress(): string {
        let tmp = new Uint8Array(PUBLIC_KEY_SIZE + 1)
        tmp.set([SIGNATURE_SCHEME_TO_FLAG['ED25519']])
        tmp.set(this.toBytes(), 1)
        return '0x' + sha3.sha3_256(tmp).slice(0, 40)
    }
    /**
     * Return the evm address associated with this Ed25519 public key
     */
    toEvmAddress(): string {
        return '0x' + sha3.sha3_256(this.toBytes()).slice(0, 40)
    }
}
