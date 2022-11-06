
[![CI](https://github.com/db3-teams/db3/workflows/CI/badge.svg)](https://github.com/db3-teams/db3/actions)
[![codecov](https://codecov.io/gh/db3-teams/db3/branch/main/graph/badge.svg?token=A2P47OWC5H)](https://codecov.io/gh/db3-teams/db3)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/db3-teams/db3)
![GitHub issues](https://img.shields.io/github/issues/db3-teams/db3)
[![Discord](https://img.shields.io/badge/Discord-5865F2?style=for-the-badge&logo=discord&logoColor=white)](https://discord.gg/9JfH4UXyQR)

# Quick Start 

```
# build db3
git clone https://github.com/dbpunk-labs/db3.git
cd db3 & cargo build

# run localnet
cd tool && sh start_localnet.sh

# open another terminal , enter db3 dir and run db3 shell
./target/debug/db3 shell  --public-grpc-url http://127.0.0.1:26659

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

# License
Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

# Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
See [CONTRIBUTING.md](CONTRIBUTING.md).
