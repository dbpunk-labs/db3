# Contribution guidelines

First off, thank you for considering contributing to db3.

If your contribution is not straightforward, please first discuss the change you
wish to make by creating a new issue before making the change.

## Reporting issues

Before reporting an issue on the
[issue tracker](https://github.com/dbpunk-labs/db3/issues),
please check that it has not already been reported by searching for some related
keywords.

## Developing

### Build

1. clone the source and init the building environment
```shell
git clone https://github.com/dbpunk-labs/db3.git
cd db3 & bash install_env.sh
```

2. compile the bridge contract
```shell
cd  bridge && yarn && npx hardhat test
```
3. build the db3 binary
```shell
cargo build
```

### Start local testnet

```
cd tools && bash start_localtestnet.sh
```

### Run test cases

```
cargo test
```

## Update Documents

if you want update db3 documents , you can follow the steps

### Install Mkdocs

```shell
pip install mkdocs
```
### Document Template

db3 uses https://squidfunk.github.io/mkdocs-material/ as its document framework and you can get started from [here](https://squidfunk.github.io/mkdocs-material/getting-started/)

### Serve the docs

```shell
git clone https://github.com/dbpunk-labs/db3.git
mkdocs serve
```
