//
//
// Ed25519Signer.java
// Copyright (C) 2022 db3.network Author imotai <codego.me@gmail.com>
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

package network.db3.crypto;
import cafe.cryptography.ed25519.Ed25519ExpandedPrivateKey;
import cafe.cryptography.ed25519.Ed25519PrivateKey;
import cafe.cryptography.ed25519.Ed25519PublicKey;
import cafe.cryptography.ed25519.Ed25519Signature;

public class Ed25519Signer {
    private final Ed25519PublicKey publicKey;
    private final Ed25519ExpandedPrivateKey extendPrivateKey;


    public Ed25519Signer(Ed25519PrivateKey privateKey) {
        this.publicKey = privateKey.derivePublic();
        this.extendPrivateKey = privateKey.expand();
    }

    public Ed25519PublicKey getPublicKey() {
        return this.publicKey;
    }

    public Ed25519Signature sign(byte[] message) {
        return  this.extendPrivateKey.sign(message, this.publicKey);
    }
}
