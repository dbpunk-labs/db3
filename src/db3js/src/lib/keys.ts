import * as dcrypto from "@deliberative/crypto";

export async function generateKey() {
    const mnemonic = await dcrypto.generateMnemonic();
    const keypair = await dcrypto.keyPairFromMnemonic(mnemonic);
	return [keypair.secretKey, keypair.publicKey];
}

export async function getATestKeyPair() {
    const sk = Buffer.from('833fe62409237b9d62ec77587520911e9a759cec1d19755b7da901b96dca3d42','hex');
    const pk = Buffer.from('ec172b93ad5e563bf4932c70e1245034c35467ef2efd4d64ebf819683467e2bf','hex');
    return [sk, pk];
}

export async function sign(data: Uint8Array, privateKey: Uint8Array) {
    const signature = await dcrypto.sign(data, privateKey);
	return signature;
}
