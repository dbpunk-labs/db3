![db3_logo](./docs/images/db3_logo.png)

![CI](https://img.shields.io/github/workflow/status/dbpunk-labs/db3/ci?style=flat-square)
![coverage](https://img.shields.io/codecov/c/github/dbpunk-labs/db3?style=flat-square)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/db3-teams/db3?style=flat-square)
![contribution](https://img.shields.io/github/contributors/dbpunk-labs/db3?style=flat-square)
![GitHub issues](https://img.shields.io/github/issues/db3-teams/db3?style=flat-square)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/dbpunk-labs/db3?style=flat-square)
![npm](https://img.shields.io/npm/v/db3js?style=flat-square)
![Twitter Follow](https://img.shields.io/twitter/follow/Db3Network?style=flat-square)
![Discord](https://img.shields.io/discord/1025017851179962408?style=flat-square)

# What's DB3?

DB3 is a community-driven decentralized layer two database network.

# Features

1. **Infinite Storage Space**

   Scalability is the key for the web3 explosion, db3 will use the following strategies to achieve web3 scale

    * PC can neet minimum system requirements so everyone can join the db3 network to provide storage space.
    * Using dynamic sharding to achieve scale out. when a storage shard chain has not enough space to store [mutation](), it will split itself into two subchains.
    * Using cold data archive to recycle storage space. history cold blocks and cold state data will be archived to FileCoin and the storage node will always has storage space to store new data.

2. **Blazed Fast and Provable On-chain Query**

   Currently decentralization means bad performance but db3 is trying to make a big improvement on performance with the following technology

	* [merkdb], db3 uses merkdb as its storage to provide high performance query and fast query proof generation
	* geo distribution, the nodes in every storage shard are geo distributed and the clients can execute querys against the nearest storage node
    * [query session], the first query protocol to resolve performance and incentive perfectly

3. **Crypto Native Data Ownership**

    In the decentralized network only the private key owners can update their data and they can keep privacy by encrypting their data with public key

4. **On-chain Programable**


5. **Ethereum Guarded Security **



What these features exactly mean? Let’s explain in detail. 

# License
Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

# Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
See [CONTRIBUTING.md](CONTRIBUTING.md).
