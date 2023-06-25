//
// id.ts
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
import { toB64, fromHEX } from './crypto_utils'
import * as numBufferPkg from 'int64-buffer'

const TX_ID_LENGTH = 32
const DB_ID_LENGTH = 20

export class TxId {
    data: Uint8Array
    constructor(data: Uint8Array) {
        const inputDataLength = data.length
        if (inputDataLength != TX_ID_LENGTH) {
            throw new Error(
                `Wrong data size. Expected 32 bytes, got ${inputDataLength}.`
            )
        }
        this.data = data
    }

    //
    // from the broadcast response
    //
    static from(hash: Uint8Array): TxId {
        return new TxId(hash)
    }

    getB64(): string {
        return toB64(this.data)
    }
}

export class DbId {
    addr: string
    constructor(sender: string, nonce: number) {
        const binary_addr = fromHEX(sender)
        const nonceBuf = new numBufferPkg.Uint64BE(nonce)
        let tmp = new Uint8Array(DB_ID_LENGTH + 8)
        tmp.set(nonceBuf.toArray(), 0)
        tmp.set(binary_addr, 8)
        this.addr = '0x' + sha3.sha3_256(tmp).slice(0, 40)
    }

    getHexAddr(): string {
        return this.addr
    }
}
