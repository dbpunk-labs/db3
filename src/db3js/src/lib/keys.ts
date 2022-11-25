import * as secp from "@noble/secp256k1";
import * as secp256k1 from "secp256k1";
import { hmac } from "@noble/hashes/hmac";
import { sha256 } from "@noble/hashes/sha256";

secp.utils.hmacSha256Sync = (key, ...msgs) =>
	hmac(sha256, key, secp.utils.concatBytes(...msgs));
secp.utils.sha256Sync = (...msgs) => sha256(secp.utils.concatBytes(...msgs));

export function generateKey() {
	const privKey = window.crypto.getRandomValues(new Uint8Array(32));
	const pubKey = secp256k1.publicKeyCreate(privKey);
	return [privKey, pubKey];
}

export function sign(data: Uint8Array, privateKey: Uint8Array) {
	const [signature] = secp.signSync(data, privateKey, { recovered: true });
	return signature;
}
