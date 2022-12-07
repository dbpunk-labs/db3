import * as dcrypto from "@deliberative/crypto";

export async function generateKey() {
    const mnemonic = await dcrypto.generateMnemonic();
    const keypair = await dcrypto.keyPairFromMnemonic(mnemonic);
	  return [keypair.secretKey, keypair.publicKey];
}

export async function getATestStaticKeypair() {
   const mnemonic = "prefer name genius napkin pig tree twelve blame meat market market soda";
    const keypair = await dcrypto.keyPairFromMnemonic(mnemonic);
	return [keypair.secretKey, keypair.publicKey];
}

export async function sign(data: Uint8Array, privateKey: Uint8Array) {
    const signature = await dcrypto.sign(data, privateKey);
	return signature;
}
