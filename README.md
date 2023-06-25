<p align="center">
 <img width="300px" src="./docs/images/db3_logo.svg" align="center"/>

![GitHub Workflow Status (with event)](https://img.shields.io/github/actions/workflow/status/dbpunk-labs/db3/ci.yml?branch=main&style=flat-square)
![coverage](https://img.shields.io/codecov/c/github/dbpunk-labs/db3?style=flat-square)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/db3-teams/db3?style=flat-square)
![contribution](https://img.shields.io/github/contributors/dbpunk-labs/db3?style=flat-square)
![GitHub issues](https://img.shields.io/github/issues/db3-teams/db3?style=flat-square)
[![GitHub issues by-label](https://img.shields.io/github/issues/dbpunk-labs/db3/good%20first%20issue?style=flat-square)](https://github.com/dbpunk-labs/db3/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/dbpunk-labs/db3?color=green&display_name=tag&label=db3&logo=db3&logoColor=https%3A%2F%2Favatars.githubusercontent.com%2Fu%2F102341693%3Fs%3D96%26v%3D4&style=flat-square)[![Twitter Follow](https://img.shields.io/twitter/follow/Db3Network?style=flat-square)](https://twitter.com/Db3Network)
[![GitPOAP Badge](https://public-api.gitpoap.io/v1/repo/dbpunk-labs/db3/badge)](https://www.gitpoap.io/gh/dbpunk-labs/db3)
[![Discord](https://badgen.net/badge/icon/discord?icon=discord&label)](https://discord.gg/sz3bmZx2uh)

**English**

# DB3 Network

DB3 Network is a lightweight, permanent JSON document database for Web3. It is designed to store and retrieve data for decentralized applications built on blockchain technology. DB3 Network has two core features:

1. Using DB3 Network as a JSON document database.
2. Periodically rolling up the JSON document to the Arweave.

# Have a try

## Set up self-hosted Node

```shell
sudo docker run -p 26639:26639 -p 26619:26619 -p 26629:26629 \
                -e ADMIN_ADDR=0xF78c...29679 \ 
                -it imotai/db3:v0.3.12
```

## Build

```shell
git clone https://github.com/dbpunk-labs/db3.git
cd db3 && bash install_env.sh
cargo build
```
If you encounter any problems, you can check the environment by referring to [BUILD](./BUILD.md).

## Write And Query the Document

```typescript
// create a account
const account = createRandomAccount()
// create the client
const client = createClient('http://127.0.0.1:26619',
                            'http://127.0.0.1:26639', 
                             account)

// get the collection
const collection = await getCollection("0xF7..79", "book", client)

// add a document
const {id} = await addDoc(collection, {
                name:"The Three-Body Problem"，
                author:"Cixin-Liu",
                rate:"4.8"} as Book)
// query the document
const resultSet = await queryDoc<Book>(collection, "/[author=Cixin-Liu]")
```
you can go to [sdk](https://docs.db3.network/) for more

# How it works

The DB3 Network has two roles:

1. The **Data Rollup Node** accepts write operations, compresses the data, rolls it up to Arweave, and stores the metadata in the smart contract.
2. The **Data Index Node** synchronizes data from the Data Rollup Node in real-time or recovers the index with metadata in the smart contract and files in Arweave. It serves the index as a queryable API.

To prevent tampering, every 
JSON document must be signed by its owner. Only the owner can update or delete the JSON document.
<p align="center">
<img width="500px" src="./docs/images/db3_arch.png" align="center"/>


# What can we build with the db3 network?

1. A fully on-chain game.
2. A fully on-chain social network.

All user data will be stored permanently on the DB3 network.

# FAQ

Q: Is the DB3 Network a blockchain?

A: No, the DB3 Network is not a blockchain. It is simply a developer tool that can be hosted locally or used through our cloud service.

Q: What are the differences between MongoDB and DB3 Network?

A: MongoDB uses centralized data storage, whereas DB3 Network uses decentralized data storage. Additionally, DB3 Network ensures that data is permanently available.

Q: Will my data be lost if the Data Rollup Node or Data Index Node is not available?

A: No, you can set up your data index node and recover your data from the blockchain.
# License

Apache License, Version 2.0
([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

# Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
See [CONTRIBUTING.md](CONTRIBUTING.md).
