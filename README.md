
<p align="center" width="100%">
    <img width="33%" src="https://github.com/dbpunk-labs/db3/blob/main/docs/images/db3_logo.png">
</p>

<p align="center" width="100%">
A star ⭐️ is welcome.
</p>

![GitHub Workflow Status (with event)](https://img.shields.io/github/actions/workflow/status/dbpunk-labs/db3/ci.yml?branch=main&style=flat-square)
![coverage](https://img.shields.io/codecov/c/github/dbpunk-labs/db3?style=flat-square)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/db3-teams/db3?style=flat-square)
![contribution](https://img.shields.io/github/contributors/dbpunk-labs/db3?style=flat-square)
![GitHub issues](https://img.shields.io/github/issues/db3-teams/db3?style=flat-square)
[![GitHub issues by-label](https://img.shields.io/github/issues/dbpunk-labs/db3/good%20first%20issue?style=flat-square)](https://github.com/dbpunk-labs/db3/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/dbpunk-labs/db3?style=flat-square)
[![Twitter Follow](https://img.shields.io/twitter/follow/Db3Network?style=flat-square)](https://twitter.com/Db3Network)
![Discord](https://img.shields.io/discord/1025017851179962408?style=flat-square)
[![GitPOAP Badge](https://public-api.gitpoap.io/v1/repo/dbpunk-labs/db3/badge)](https://www.gitpoap.io/gh/dbpunk-labs/db3)


# What's DB3 Network?

DB3 is a community-driven layer2 decentralized database network. It provides developers with an alternative to firebase firestore.

## Why DB3 Network

DB3 network serves as the data management for web3 applications; you can store and query documents into the db3 network with a firestore-like [SDK](https://github.com/dbpunk-labs/db3.js).

![position](./docs/images/position_web3.jpg)

Doc is here: https://docs.db3.network/

## Features

**Infinite Storage Space**

Scalability is critical in the web3 era; DB3 will use the following strategies to achieve dApp's scalability:
  * Personal computers is enough to maintain the minimum network node requirements so everyone can join the db3 network to provide storage space.
  * Using dynamic sharding to scale out: when a storage shard chain does not have enough space to store [mutation](./docs/mutation.md), the original shard chain will split itself into two subchains.
  * Archiving the cold data to recycle storage space: historical cold blocks and cold state data will be archived to FileCoin, so new incoming data can always be stored.

**Blazed Fast and Provable On-chain Query**

Currently,decentralization means terrible performance, but DB3 is trying to improve significantly:
* [Merkdb](https://github.com/dbpunk-labs/db3/issues/100) is the storage engine of the DB3 network, and it has high performance and also fast-proof generation. 
* Geo distribution: the nodes in every storage shard are geo-distributed, and the clients can execute queries against the nearest storage node.
* [Query session](./docs/query.md), the first decentralized query protocol to resolve performance and incentive perfectly. 

**Crypto Native Data Ownership**

We proposed [the document level ownership](https://github.com/dbpunk-labs/db3/issues/271), and every document has its owner, while only the owner who holds the private key can update/delete the record. DB3 network generates the proofs and provides signatures to prove the membership (db3 has the specific document) and ownership.

**Programming on-chain in web2 manner**

Dapp developers can develop data processing contracts and deploy them to the DB3 network just like developing data backend in web2.

**Ethereum Guarded Security**

DB3 network is a layer2 network on Ethereum and Ethereum guards all the assets.

Looking for more details?go to the [background introduction](./docs/background.md)

# Getting Started

### Start A Local Testnet

```shell
git clone https://github.com/dbpunk-labs/db3.git
cd db3 && bash install_env.sh && cargo build
# start localnet
cd tools &&  sh start_localnet.sh
```

### Developers Friendly SDK

```typescript
// connect to db3 node
const db3_instance = new DB3("http://127.0.0.1:26659");
const doc_store = new DocStore(db3_instance);
const doc_index = {
    keys: [
        {
            name: 'address',
            keyType: DocKeyType.STRING,
        },
        {
            name: 'ts',
            keyType: DocKeyType.NUMBER,
        },
    ],
    ns: 'ns1',
    docName: 'transaction',
};
const transaction = {
    address: '0x11111',
    ts: 9527,
    amount: 10,
};
// insert a document
const result = await doc_store.insertDocs(doc_index, [transaction], _sign, 1);
// query a document
const query = {
    address: '0x11111',
    ts: 9527,
};
const docs = await doc_store.getDocs(doc_index, [query], _sign);
```

more examples

* [helloworld in typescript](./examples/helloworld)

# Project assistance

* Add a GitHub Star⭐️ to the project.
* Tweet about how to use DB3 network.
* Write blogs about the project on [Dev.to](https://dev.to/), [Medium](https://medium.com/) or your personal blog.

Together, we can make db3 network better!


# The Relationship Between Roles

![relationship](./docs/images/db3-overview.svg)

# The Architecture

![arch](./docs/images/db3-architecture.svg)

more technical details
* [mutation](./docs/mutation.md)
* [query session](./docs/query.md)
* [dvm](./docs/dvm.md)
* [merkdb](https://github.com/dbpunk-labs/db3/issues/100)


# Other Decentralized Database

* [the graph](https://github.com/graphprotocol/graph-node), a decentralized on-chain indexer
* [Locutus](https://github.com/freenet/locutus), a decentralized key-value database
* [ceramic network](https://github.com/ceramicnetwork/ceramic), a decentralized data network that brings unlimited data composability to Web3 applications
* [kwil](https://github.com/kwilteam), the first permissionless SQL database for the decentralized internet
* [spaceandtime](https://www.spaceandtime.io/), a decentralized data Warehouse
* [OrbitDB](https://github.com/orbitdb/orbit-db) is a serverless, distributed, peer-to-peer database

# License
Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

# Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
See [CONTRIBUTING.md](CONTRIBUTING.md).
