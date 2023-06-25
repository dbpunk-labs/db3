//
// keypair.ts
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

import { PublicKey, SignatureScheme } from './publickey'

export type ExportedKeypair = {
    schema: SignatureScheme
    privateKey: string
}

/**
 * A keypair used for signing transactions.
 */
export interface Keypair {
    /**
     * The public key for this keypair
     */
    getPublicKey(): PublicKey

    /**
     * Return the signature for the data
     */
    signData(data: Uint8Array): Uint8Array

    /**
     * Get the key scheme of the keypair: Secp256k1 or ED25519
     */
    getKeyScheme(): SignatureScheme

    export(): ExportedKeypair
}
