
<p align="center">
 <img width="300px" src="./docs/images/db3_logo.svg" align="center"/>
<p align="center"> A ⭐️ is welcome!
  
<p align="center">

![GitHub Workflow Status (with event)](https://img.shields.io/github/actions/workflow/status/dbpunk-labs/db3/ci.yml?branch=main&style=flat-square)
![coverage](https://img.shields.io/codecov/c/github/dbpunk-labs/db3?style=flat-square)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/db3-teams/db3?style=flat-square)
![contribution](https://img.shields.io/github/contributors/dbpunk-labs/db3?style=flat-square)
![GitHub issues](https://img.shields.io/github/issues/db3-teams/db3?style=flat-square)
[![GitHub issues by-label](https://img.shields.io/github/issues/dbpunk-labs/db3/good%20first%20issue?style=flat-square)](https://github.com/dbpunk-labs/db3/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/dbpunk-labs/db3?style=flat-square)
[![Twitter Follow](https://img.shields.io/twitter/follow/Db3Network?style=flat-square)](https://twitter.com/Db3Network)
[![GitPOAP Badge](https://public-api.gitpoap.io/v1/repo/dbpunk-labs/db3/badge)](https://www.gitpoap.io/gh/dbpunk-labs/db3)
[![Discord](https://badgen.net/badge/icon/discord?icon=discord&label)](https://discord.gg/sz3bmZx2uh)

**English | [中文](./README_zh_cn.md)**

## DB3 Network

DB3 Network is an open-source and decentralized firebase firestore alternative for building fully decentralized dApps quickly with  minimal engineering effort.

<p align="center">
 <img width="600px" src="./docs/images/position_web3.svg" align="center"/>

## Demo

[DB3 Network CRUD TodoMVC demo](https://replit.com/@imotai/DB3-Network-CRUD-TodoMVC-Demo?v=1)

![todo_demo](./docs/images/todo_mvc_db3.png)

[DB3 Network Metamask Support Demo](https://replit.com/@imotai/db3-playground-with-metamask?v=1)

![playground](./docs//images/playground.png)

## Getting Started

### Build

```shell
git clone https://github.com/dbpunk-labs/db3.git
cd db3 && bash install_env.sh && cargo build
# start localnet
cd tools &&  sh start_localnet.sh
```

## Why DB3 Network
![why db3](./docs/images/why_db3.svg)

Currently, there are two types of Data architecture for dApp(decentralized application): centralized vs. decentralized.
  
**Centralized**: use [Firebase](https://firebase.google.com)<img height="20" width="20" src="https://cdn.jsdelivr.net/npm/simple-icons@v8/icons/firebase.svg" />
 or [MongoDB](https://github.com/mongodb/mongo)<img height="20" width="20" src="https://cdn.jsdelivr.net/npm/simple-icons@v8/icons/mongodb.svg" />to store the data), both of which are developer-friendly. However, dApps would be regarded as less secure based on a central database.
  
**Decentralized**: use Ethereum<img height="20" width="20" src="https://cdn.jsdelivr.net/npm/simple-icons@v8/icons/ethereum.svg" /> or other blockchains to store the data and use [the Graph](https://thegraph.com/en/) to index data from it. The separation of the storage and the indexer would cost a lot of engineering efforts in future development.
  
With Db3 network, you can get both advantages of the above two choices.


# Features

**Schemaless**

You can store data on DB3 Network without any change.

**High Performance**

Currently, decentralization means terrible performance, but DB3 is trying to improve it significantly:
* [Merkdb](https://github.com/dbpunk-labs/db3/issues/100) is the storage engine of the DB3 Network, and it has high Performance and fast-proof generation. 
* Geo distribution: the nodes in every storage shard are geo-distributed, and the clients can execute queries against the nearest storage node.
* [Query session](./docs/query.md), the first decentralized query protocol to resolve Performance and incentive perfectly.

**Data Ownership**

We proposed [the document level ownership](https://github.com/dbpunk-labs/db3/issues/271), and every document has its owner, while only the owner holds the private key can update/delete the record. DB3 network generates the proofs and provides signatures to prove the membership (db3 has the specific document) and ownership.

**Programmable**

Dapp developers can develop data processing contracts and deploy them to the DB3 Network, just like the data backend in web2.

**Ethereum Guarded Security**

DB3 Network is a layer2 network on Ethereum and Ethereum guards all the assets.

# Getting Started

### Build

```shell
git clone https://github.com/dbpunk-labs/db3.git
cd db3 && bash install_env.sh && cargo build
# start localnet
cd tools &&  sh start_localnet.sh
```

### Use Console

 * [x] Start db3 console

```shell
./target/debug/db3 console
db3>-$ new-db
database address                           | transaction id
--------------------------------------------+----------------------------------------------
0xa9f5c8170aad7a0f924d89c6edacae6db24ef57d | 0ALy/hH7CQe9lv294K6dOxGP14xWHsbRs+/pXBZa8oU=
```

 * [x] Show database

```shell
db3>-$ show-db --addr 0x7e16cb6524e2fc21ae9bf2d7ee18b05767b9dc33
 database address                           | sender address                             | related transactions                        | collections
--------------------------------------------+--------------------------------------------+----------------------------------------------+-------------
 0x7e16cb6524e2fc21ae9bf2d7ee18b05767b9dc33 | 0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b | EMYw64xlI2q4v1MShoKw3T60asNbWJ9//ca75M3JO3Q= |
```

 * [x] Add a collection to database

```shell
db3>$ new-collection --addr 0xcfb524677673af15edebbec018b16d42d87b1251 --name books --index '{"name":"idx1","fields":[{"field_path":"test1","value_mode":{"Order":1}}]}'
send add collection done with tx
3V7r7VRg+9zUXeGNmqRR0YdVXWtBSl4sk+Z50h9BrOc=

```

 * [x] Show collections in database

```shell
db3>-$ show-collection --addr 0xcfb524677673af15edebbec018b16d42d87b1251
 name  | index
-------+----------------------------------------------------------------------------
 books | {"name":"idx1","fields":[{"field_path":"test1","value_mode":{"Order":1}}]}
```
 * [x] Create a document

```
db3>-$ new-doc --addr 0x997f631fcafeed5ee319c83683ae16e64783602b --collection-name books --documents '{"name": "John Doe","age": 43,"phones": ["+44 1234567","+44 2345678"]}'
send add document done with tx
+O9cK2cHLexZQvIITk4OTm8SxBhq7Yz7g+xZYiionWo=
```

 * [x] list documents
 
 ```
db3>-$ show-doc --addr 0x22fb51848e26b34e242dd16a1224e8f23ee9b42e  --collection-name books
 id_base64                                    | owner                                      | document
----------------------------------------------+--------------------------------------------+---------------------------------------------------------------------------------------------------------------------------
 AQAAAAAAAAAyAAAAAQAAAAEAAAAAAAAAfAAAAAEAAAAA | 0x84b0bd55e7ad979b7cb92a56f561190de8f68403 | Document({"name": String("John Doe"), "age": Int64(43), "phones": Array([String("+44 1234567"), String("+44 2345678")])})
 AQAAAAAAAAAyAAAAAQAAAAEAAAAAAAABLAAAAAEAAAAA | 0x84b0bd55e7ad979b7cb92a56f561190de8f68403 | Document({"name": String("John Doe"), "age": Int64(44), "phones": Array([String("+44 1234567"), String("+44 2345678")])})
 AQAAAAAAAAAyAAAAAQAAAAEAAAAAAAABPgAAAAEAAAAA | 0x84b0bd55e7ad979b7cb92a56f561190de8f68403 | Document({"name": String("John Doe"), "age": Int64(45), "phones": Array([String("+44 1234567"), String("+44 2345678")])})
 ```

* [x] get a document

 ```
 db3>-$ get-doc --id AQAAAAAAAAAVAAAAAQAAAAEAAAAAAAAAOQAAAAEAAAAA
 id_base64                                    | owner                                      | document
----------------------------------------------+--------------------------------------------+-----------------------------------------------------------------------------------------------------------------------
 AQAAAAAAAAAVAAAAAQAAAAEAAAAAAAAAOQAAAAEAAAAA | 0x84b0bd55e7ad979b7cb92a56f561190de8f68403 | Document({"name": String("Mike"), "age": Int64(43), "phones": Array([String("+44 1234567"), String("+44 2345678")])})
 ```
 * [x] show network state

```
db3>-$ show-state
 name       | state
------------+---------
 database   | 1
 collection | 0
 documemt   | 0
 account    | 1
 mutation   | 1
 session    | 0
 storage    | 102.00
```
 * [ ] query documents by index

### Build a dapp with db3.js

#### Build db3 client

```typescript
// the key seed
const mnemonic ='...'
// create a wallet
const wallet = DB3BrowserWallet.createNew(mnemonic, 'DB3_SECP259K1')
// build db3 client
const client = new DB3Client('http://127.0.0.1:26659', wallet)
```
#### Create a database

```typescript
const [dbId, txId] = await client.createDatabase()
const db = initializeDB3('http://127.0.0.1:26659', dbId, wallet)
```

#### Create a collection

```typescript
// add an index to collection
const indexList: Index[] = [
            {
                name: 'idx1',
                id: 1,
                fields: [
                    {
                        fieldPath: 'name',
                        valueMode: {
                            oneofKind: 'order',
                            order: Index_IndexField_Order.ASCENDING,
                        },
                    },
                ],
            },
]
// create a collecion
const collectionRef = await collection(db, 'cities', indexList)
// add a doc to collection
const result = await addDoc(collectionRef, {
    name: 'beijing',
    address: 'north',
})
// get all docs from collection                                                                                                                                                                  
const docs = await getDocs(collectionRef)
```
for more please go to [db3.js](https://github.com/dbpunk-labs/db3.js)

# Project assistance

* Add a GitHub Star⭐️ to the project.
* Tweet about how to use DB3 network.
* Write blogs about the project on [Dev.to](https://dev.to/), [Medium](https://medium.com/) or your personal blog.

Together, we can make db3 network better!


# The internal of db3

![relationship](./docs/images/db3-overview.svg)

# The Architecture

![arch](./docs/images/db3-architecture.svg)

# How it works

* [mutation](./docs/old/mutation.md)
* [query session](./docs/old/query.md)
* [dvm](./docs/old/dvm.md)
* [merkdb](https://github.com/dbpunk-labs/db3/issues/100)


# Other Decentralized Database

* [the graph](https://github.com/graphprotocol/graph-node), a decentralized on-chain indexer
* [Locutus](https://github.com/freenet/locutus), a decentralized key-value database
* [ceramic network](https://github.com/ceramicnetwork/ceramic), a decentralized data network that brings unlimited data composability to Web3 applications
* [kwil](https://github.com/kwilteam), the first permissionless SQL database for the decentralized internet
* [spaceandtime](https://www.spaceandtime.io/), a decentralized data Warehouse
* [OrbitDB](https://github.com/orbitdb/orbit-db) is a serverless, distributed, peer-to-peer database


# Thanks support

 **I stood on the shoulders of giants and did only simple things. Thank you for your attention.**
<table>
  <tr>
    <td align="center"><a href="https://protocol.ai/"><img src="https://user-images.githubusercontent.com/34047788/188373221-4819fd05-ef2f-4e53-b784-dcfffe9c018c.png" width="100px;" alt=""/><br /><sub><b>Protocol Labs</b></sub></a></td>
    <td align="center"><a href="https://filecoin.io/"><img src="https://user-images.githubusercontent.com/34047788/188373584-e245e0bb-8a3c-4773-a741-17e4023bde65.png" width="100px;" alt=""/><br /><sub><b>Filecoin</b></sub></a></td>
  </tr>
</table>

# License
Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

# Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
See [CONTRIBUTING.md](CONTRIBUTING.md).

