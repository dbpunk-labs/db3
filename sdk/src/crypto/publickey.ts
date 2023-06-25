//
// publickey.ts
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

export type SignatureScheme = 'ED25519' | 'Secp256k1'

export type PublicKeyInitData = string | Uint8Array | Iterable<number>

export const SIGNATURE_SCHEME_TO_FLAG = {
    ED25519: 0x00,
    Secp256k1: 0x01,
}

/**
 * A public key
 */
export interface PublicKey {
    /**
     * Checks if two public keys are equal
     */
    equals(publicKey: PublicKey): boolean

    /**
     * Return the base-64 representation of the public key
     */
    toBase64(): string

    /**
     * Return the byte array representation of the public key
     */
    toBytes(): Uint8Array

    /**
     * Return the db3 address associated with this public key
     */
    toAddress(): string

    /**
     * Return the evm address associated with this public key
     */
    toEvmAddress(): string
}

export function bytesEqual(a: Uint8Array, b: Uint8Array) {
    if (a === b) return true

    if (a.length !== b.length) {
        return false
    }

    for (let i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) {
            return false
        }
    }
    return true
}
