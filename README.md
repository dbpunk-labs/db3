![db3_logo](./docs/images/db3_logo.png)

![GitHub Workflow Status (with event)](https://img.shields.io/github/actions/workflow/status/dbpunk-labs/db3/ci.yml?branch=main&style=flat-square)
![coverage](https://img.shields.io/codecov/c/github/dbpunk-labs/db3?style=flat-square)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/db3-teams/db3?style=flat-square)
![contribution](https://img.shields.io/github/contributors/dbpunk-labs/db3?style=flat-square)
![GitHub issues](https://img.shields.io/github/issues/db3-teams/db3?style=flat-square)
[![GitHub issues by-label](https://img.shields.io/github/issues/dbpunk-labs/db3/good%20first%20issue?style=flat-square)](https://github.com/dbpunk-labs/db3/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/dbpunk-labs/db3?style=flat-square)
[![Twitter Follow](https://img.shields.io/twitter/follow/Db3Network?style=flat-square)](https://twitter.com/Db3Network)
![Discord](https://img.shields.io/discord/1025017851179962408?style=flat-square)


# What's DB3 Network?

DB3 is a community-driven layer2 decentralized database network. and if you are a developer, you can think db3 is a decentralized firebase firestore alternative.

## Why DB3 Network

db3 network will provide data management for web3 applications, you can store and query documents against the db3 network with a firestore-like SDK

![position](./docs/images/position_web3.jpg)

## Features

1. **Infinite Storage Space**

   Scalability is the key to the web3 explosion, db3 will use the following strategies to achieve web3 scale

    * PC can meet the minimum system requirements so everyone can join the db3 network to provide storage space.
    * Using dynamic sharding to achieve scale out. when a storage shard chain has not enough space to store [mutation](./docs/mutation.md), it will split itself into two subchains.
    * Using cold data archive to recycle storage space. history cold blocks and cold state data will be archived to FileCoin and the storage node will always has storage space to store new data.

2. **Blazed Fast and Provable On-chain Query**

   Currently, decentralization means bad performance but db3 is trying to make a big improvement in performance

	* [Merkdb](https://github.com/dbpunk-labs/db3/issues/100) is the storage engine of db3 network and it not only has high performance but also the fast-proof generation
	* Geo distribution, the nodes in every storage shard are geo-distributed and the clients can execute queries against the nearest storage node

    * [Query session](./docs/query.md), the first decentralized query protocol to resolve performance and incentive perfectly

3. **Crypto Native Data Ownership**

    In the decentralized network, only the private key owners can update their data and they can keep privacy by encrypting their data with the public key

4. **On-chain Programmable**

    Dapp developers can develop data processing contracts and deploy them to the db3 network just like developing data backend in web2

5. **Ethereum Guarded Security**

    DB3 network is a layer2 network on Ethereum and all the assets are guarded by Ethereum

If you want to know what these features exactly mean? go to the [background introduction](./docs/background.md)

# Getting Started

### Start A Local Testnet

```shell
git clone https://github.com/dbpunk-labs/db3.git
cd db3 && bash install_env.sh && cargo build
# start localnet
cd tools &&  sh start_localnet.sh
# open another terminal , enter db3 dir and run db3 shell
./target/debug/db3 shell
>get ns1 k1
>put ns1 k1 v1
submit mutation to mempool done!
>get ns1 k1
k1 -> v1
>account
 total bills  | storage used | mutation | queries | credits
 0.000000 db3 | 38.00        | 2        | 0      | 10 db3
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

If you want to say thank you or/and support active development of DB3 Network

* Add a GitHub Star to the project.
* Tweet about how to use db3 network.
* Write interesting articles about the project on Dev.to, Medium or your personal blog.

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

# License
Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

# Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
See [CONTRIBUTING.md](CONTRIBUTING.md).
