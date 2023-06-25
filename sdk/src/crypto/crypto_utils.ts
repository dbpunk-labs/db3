//
// util.ts
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

import { sha512 } from '@noble/hashes/sha512'
import { hmac } from '@noble/hashes/hmac'
import nacl from 'tweetnacl'
const ED25519_CURVE = 'ed25519 seed'
const HARDENED_OFFSET = 0x80000000

export const pathRegex = new RegExp("^m(\\/[0-9]+')+$")

export const replaceDerive = (val: string): string => val.replace("'", '')

interface Keys {
    key: Uint8Array
    chainCode: Uint8Array
}

export const getMasterKeyFromSeed = (seed: string): Keys => {
    const h = hmac.create(sha512, ED25519_CURVE)
    const I = h.update(fromHEX(seed)).digest()
    const IL = I.slice(0, 32)
    const IR = I.slice(32)
    return {
        key: IL,
        chainCode: IR,
    }
}

export function fromHEX(hexStr: string): Uint8Array {
    // @ts-ignore
    let intArr = hexStr
        .replace('0x', '')
        .match(/.{1,2}/g)
        .map((byte) => parseInt(byte, 16))

    if (intArr === null) {
        throw new Error(`Unable to parse HEX: ${hexStr}`)
    }

    return Uint8Array.from(intArr)
}

export function toHEX(bytes: Uint8Array): string {
    return bytes.reduce(
        (str, byte) => str + byte.toString(16).padStart(2, '0'),
        ''
    )
}

//
// Base64 / binary data / UTF-8 strings utilities
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Base64_encoding_and_decoding
//
//  Array of bytes to Base64 string decoding
//

function b64ToUint6(nChr: number) {
    return nChr > 64 && nChr < 91
        ? nChr - 65
        : nChr > 96 && nChr < 123
        ? nChr - 71
        : nChr > 47 && nChr < 58
        ? nChr + 4
        : nChr === 43
        ? 62
        : nChr === 47
        ? 63
        : 0
}

export function fromB64(sBase64: string, nBlocksSize?: number): Uint8Array {
    var sB64Enc = sBase64.replace(/[^A-Za-z0-9+/]/g, ''),
        nInLen = sB64Enc.length,
        nOutLen = nBlocksSize
            ? Math.ceil(((nInLen * 3 + 1) >> 2) / nBlocksSize) * nBlocksSize
            : (nInLen * 3 + 1) >> 2,
        taBytes = new Uint8Array(nOutLen)

    for (
        var nMod3, nMod4, nUint24 = 0, nOutIdx = 0, nInIdx = 0;
        nInIdx < nInLen;
        nInIdx++
    ) {
        nMod4 = nInIdx & 3
        nUint24 |= b64ToUint6(sB64Enc.charCodeAt(nInIdx)) << (6 * (3 - nMod4))
        if (nMod4 === 3 || nInLen - nInIdx === 1) {
            for (
                nMod3 = 0;
                nMod3 < 3 && nOutIdx < nOutLen;
                nMod3++, nOutIdx++
            ) {
                taBytes[nOutIdx] = (nUint24 >>> ((16 >>> nMod3) & 24)) & 255
            }
            nUint24 = 0
        }
    }

    return taBytes
}

/* Base64 string to array encoding */

function uint6ToB64(nUint6: number) {
    return nUint6 < 26
        ? nUint6 + 65
        : nUint6 < 52
        ? nUint6 + 71
        : nUint6 < 62
        ? nUint6 - 4
        : nUint6 === 62
        ? 43
        : nUint6 === 63
        ? 47
        : 65
}

export function toB64(aBytes: Uint8Array): string {
    var nMod3 = 2,
        sB64Enc = ''

    for (var nLen = aBytes.length, nUint24 = 0, nIdx = 0; nIdx < nLen; nIdx++) {
        nMod3 = nIdx % 3
        if (nIdx > 0 && ((nIdx * 4) / 3) % 76 === 0) {
            sB64Enc += ''
        }
        nUint24 |= aBytes[nIdx] << ((16 >>> nMod3) & 24)
        if (nMod3 === 2 || aBytes.length - nIdx === 1) {
            sB64Enc += String.fromCodePoint(
                uint6ToB64((nUint24 >>> 18) & 63),
                uint6ToB64((nUint24 >>> 12) & 63),
                uint6ToB64((nUint24 >>> 6) & 63),
                uint6ToB64(nUint24 & 63)
            )
            nUint24 = 0
        }
    }

    return (
        sB64Enc.slice(0, sB64Enc.length - 2 + nMod3) +
        (nMod3 === 2 ? '' : nMod3 === 1 ? '=' : '==')
    )
}

export const derivePath = (
    path: string,
    seed: string,
    offset = HARDENED_OFFSET
) => {
    if (!isValidPath(path)) {
        throw new Error('Invalid derivation path')
    }

    const { key, chainCode } = getMasterKeyFromSeed(seed)
    const segments = path
        .split('/')
        .slice(1)
        .map(replaceDerive)
        .map((el) => parseInt(el, 10))

    return segments.reduce(
        (parentKeys, segment) => CKDPriv(parentKeys, segment + offset),
        { key, chainCode }
    )
}
const CKDPriv = ({ key, chainCode }: Keys, index: number): Keys => {
    const indexBuffer = new ArrayBuffer(4)
    const cv = new DataView(indexBuffer)
    cv.setUint32(0, index)

    const data = new Uint8Array(1 + key.length + indexBuffer.byteLength)
    data.set(new Uint8Array(1).fill(0))
    data.set(key, 1)
    data.set(
        new Uint8Array(indexBuffer, 0, indexBuffer.byteLength),
        key.length + 1
    )

    const I = hmac.create(sha512, chainCode).update(data).digest()
    const IL = I.slice(0, 32)
    const IR = I.slice(32)
    return {
        key: IL,
        chainCode: IR,
    }
}

export const getPublicKey = (
    privateKey: Uint8Array,
    withZeroByte = true
): Uint8Array => {
    const keyPair = nacl.sign.keyPair.fromSeed(privateKey)
    const signPk = keyPair.secretKey.subarray(32)
    const newArr = new Uint8Array(signPk.length + 1)
    newArr.set([0])
    newArr.set(signPk, 1)
    return withZeroByte ? newArr : signPk
}

export const isValidPath = (path: string): boolean => {
    if (!pathRegex.test(path)) {
        return false
    }
    return !path
        .split('/')
        .slice(1)
        .map(replaceDerive)
        .some(isNaN as any /* ts T_T*/)
}
export const isHexString = (value: string): boolean => {
    if (typeof value !== 'string' || !value.match(/^0x[0-9A-Fa-f]*$/)) {
        return false
    }

    return true
}
