![db3_logo](./docs/images/db3_logo.png)

[![CI](https://github.com/db3-teams/db3/workflows/CI/badge.svg)](https://github.com/db3-teams/db3/actions)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/db3-teams/db3)
![GitHub issues](https://img.shields.io/github/issues/db3-teams/db3)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/dbpunk-labs/db3)
![Twitter Follow](https://img.shields.io/twitter/follow/Db3Network?style=social)
![Discord](https://img.shields.io/discord/1025017851179962408)

# What's DB3

DB3 is a SQL compatible and data processing programmable decentralized database network

# Getting Started

## Build

```
# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# change toolchain to nightly
rustup default nightly
# install cmake on mac os
brew install cmake
# or
# install cmake on ubuntu
sudo apt install cmake

# build db3
git clone https://github.com/dbpunk-labs/db3.git
cd db3 & git submodule init & cargo build
```
## Run Localnet

```
# run localnet
cd tools && bash start_localnet.sh
# open another terminal , enter db3 dir and run db3 shell
./target/debug/db3 shell

██████╗ ██████╗ ██████╗
██╔══██╗██╔══██╗╚════██╗
██║  ██║██████╔╝ █████╔╝
██║  ██║██╔══██╗ ╚═══██╗
██████╔╝██████╔╝██████╔╝
╚═════╝ ╚═════╝ ╚═════╝
@db3.network🚀🚀🚀
WARNING, db3 will generate private key and save it to ~/.db3/key
restore the key with addr 0x0dce49e41905e6c0c5091adcedee2dee524a3b06
>put ns1 k1 v1 k2 v2
submit mutation to mempool done!
>get ns1 k1 k2
k1 -> v1
k2 -> v2
>account
 total bills | storage used | mutation | querys | credits
  3400 tai    | 76.00        | 1        | 0      | 10 db3
```

# Roadmap

db3 has three phases to reach it's vison

* phase 1 decentralized kv storage and data ownership
* phase 2 programable data virtual machine and permission control
* phase 3 data privacy

# Archtecture
![db3_arch](./docs/images/db3_arch.png)
# Media
* [all in web3探索个人数据主权](https://www.muran.me/%E7%A6%BB%E8%81%8C%E9%98%BF%E9%87%8Call-in-web3%E6%8E%A2%E7%B4%A2%E4%B8%AA%E4%BA%BA%E6%95%B0%E6%8D%AE%E4%B8%BB%E6%9D%83)
# License
Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

# Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
See [CONTRIBUTING.md](CONTRIBUTING.md).
