use hmac::Mac;
use secp256k1::hashes::sha256;
use secp256k1::{constants, Message, PublicKey, SecretKey, SECP256K1};
use wasm_bindgen::prelude::*;

/// Derivation domain separator for BIP39 keys.
const BIP39_DOMAIN_SEPARATOR: [u8; 12] = [
    0x42, 0x69, 0x74, 0x63, 0x6f, 0x69, 0x6e, 0x20, 0x73, 0x65, 0x65, 0x64,
];
// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;
const RECOVERABLE_SIGNATURE_SIZE: usize = constants::COMPACT_SIGNATURE_SIZE + 1;
type HmacSha512 = hmac::Hmac<sha2::Sha512>;


#[wasm_bindgen]
pub fn gen_key(seed: &[u8]) -> Result<js_sys::Array, js_sys::Error> {
    let mut hmac = HmacSha512::new_from_slice(&BIP39_DOMAIN_SEPARATOR)
        .map_err(|_| js_sys::Error::new("fail to init hmac"))?;
    hmac.update(seed);
    let result = hmac.finalize().into_bytes();
    let (secret_key, _) = result.split_at(constants::SECRET_KEY_SIZE);
    let sk = SecretKey::from_slice(secret_key)
        .map_err(|_| js_sys::Error::new("fail to decode secret key from input"))?;
    let pk = PublicKey::from_secret_key(&SECP256K1, &sk);
    let array = js_sys::Array::new_with_length(2);
    array.set(0, js_sys::Uint8Array::from(pk.serialize().as_ref()).into());
    array.set(
        1,
        js_sys::Uint8Array::from(sk.secret_bytes().as_ref()).into(),
    );
    Ok(array)
}

#[wasm_bindgen]
pub fn sign(msg: &[u8], sk: &[u8]) -> Result<js_sys::Uint8Array, js_sys::Error> {
    let message = Message::from_hashed_data::<sha256::Hash>(msg);
    let secret_key = SecretKey::from_slice(sk)
        .map_err(|_| js_sys::Error::new("fail to decode secret key from input"))?;
    let mut bytes = [0u8; RECOVERABLE_SIGNATURE_SIZE];
    let signature = SECP256K1.sign_ecdsa_recoverable(&message, &secret_key);
    let (recovery_id, sig) = signature.serialize_compact();
    bytes[..64].copy_from_slice(&sig);
    bytes[64] = recovery_id.to_i32() as u8;
    Ok(js_sys::Uint8Array::from(bytes.as_ref()))
}
