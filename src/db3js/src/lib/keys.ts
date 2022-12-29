import dcrypto from '@deliberative/crypto'
import sha3 from 'js-sha3'

export async function generateKey() {
    const mnemonic = await dcrypto.generateMnemonic()
    const keypair = await dcrypto.keyPairFromMnemonic(mnemonic)
    return [keypair.secretKey, keypair.publicKey]
}

export async function getATestStaticKeypair() {
    const mnemonic =
        'prefer name genius napkin pig tree twelve blame meat market market soda'
    const keypair = await dcrypto.keyPairFromMnemonic(mnemonic)
    return [keypair.secretKey, keypair.publicKey]
}

export async function sign(data: Uint8Array, privateKey: Uint8Array) {
    const signature = await dcrypto.sign(data, privateKey)
    return signature
}

export async function getAddress(publicKey: Uint8Array) {
    return '0x' + sha3.keccak_256(publicKey.subarray(1)).substring(24)
}
