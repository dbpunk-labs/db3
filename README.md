
[![CI](https://github.com/db3-teams/db3/workflows/CI/badge.svg)](https://github.com/db3-teams/db3/actions)
[![codecov](https://codecov.io/gh/db3-teams/db3/branch/main/graph/badge.svg?token=A2P47OWC5H)](https://codecov.io/gh/db3-teams/db3)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/db3-teams/db3)
![GitHub issues](https://img.shields.io/github/issues/db3-teams/db3)

# What is DB3?

db3 is a fully decentralized database network which has the following key features

* Account Based Data Permission Model
* ANSI SQL Compatible
* Programable Data Virtual Machine(DVM)
* Horizontally scaling out
* Global Replication

# Status

It's under a very early stage. if you are interested in this project, Issues, Discussions or PRs are welcome.

# Compile

- Upgrade cargo

```bash
rustup default nightly
rustup update 
```

- Check cargo version >= `cargo 1.66.0-nightly` 


```bash
cargo --version
```

- Compile

```bash
cargo build --release
```

- Run

```bash
./target/release/db3 --dev
```

# Architecture

![arch](./docs/arch.svg)

# License
Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

# Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
See [CONTRIBUTING.md](CONTRIBUTING.md).
