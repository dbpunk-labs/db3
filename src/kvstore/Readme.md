
### Install Tendermint

Install Tendermint-Core v0.34
https://github.com/tendermint/tendermint/releases/tag/v0.34.22

> v0.34.22
Special thanks to external contributors on this release: @RiccardoM
This release includes several bug fixes, https://github.com/tendermint/tendermint/pull/9518 we discovered while building up a baseline for v0.34 against which to compare our upcoming v0.37 release during our [QA process](https://github.com/tendermint/tendermint/blob/v0.34.22/docs/qa).

MacOS:
```shell
wget https://github.com/tendermint/tendermint/releases/download/v0.34.22/tendermint_0.34.22_darwin_amd64.tar.gz
```

Linux:
```shell
wget https://github.com/tendermint/tendermint/releases/download/v0.34.22/tendermint_0.34.22_linux_amd64.tar.gz
```

### Run db3-kvstore

To run the key/value

```bash
# Set your logging level through RUST_LOG (e.g. RUST_LOG=info)
# Binds to 127.0.0.1:26658

> cd ${PATH_TO_DB3}
> RUST_LOG=debug cargo run --bin db3-kvstore --features binary
```
Output 
```
RUST_LOG=debug cargo run --bin db3-kvstore --features binary                                                                                                                 ─╯
    Finished dev [unoptimized + debuginfo] target(s) in 0.86s
     Running `target/debug/db3-kvstore`
Oct 23 21:03:09.769  INFO tendermint_abci::server: ABCI server running at 127.0.0.1:26658
```

### Start and run Tendermint node

Reset and run your Tendermint node (binds RPC to 127.0.0.1:26657 by default)
```bash
cd ${PATH_TO_TENDERMINT}

> ./tendermint init && ./tendermint unsafe_reset_all && ./tendermint start
```
Output
```shell
❯ ./tendermint init && ./tendermint unsafe_reset_all && ./tendermint start                                                                                                     ─╯
I[2022-10-23|21:03:47.711] Found private validator                      module=main keyFile=/Users/chenjing/.tendermint/config/priv_validator_key.json stateFile=/Users/chenjing/.tendermint/data/priv_validator_state.json
I[2022-10-23|21:03:47.711] Found node key                               module=main path=/Users/chenjing/.tendermint/config/node_key.json
I[2022-10-23|21:03:47.711] Found genesis file                           module=main path=/Users/chenjing/.tendermint/config/genesis.json
Deprecated: snake_case commands will be replaced by hyphen-case commands in the next major release
I[2022-10-23|21:03:47.751] Removed existing address book                module=main file=/Users/chenjing/.tendermint/config/addrbook.json
I[2022-10-23|21:03:47.754] Removed all blockchain history               module=main dir=/Users/chenjing/.tendermint/data
I[2022-10-23|21:03:47.755] Reset private validator file to genesis state module=main keyFile=/Users/chenjing/.tendermint/config/priv_validator_key.json stateFile=/Users/chenjing/.tendermint/data/priv_validator_state.json
```

### Application Test
Submit a key/value pair (set "somekey" to "somevalue")
```bash
curl 'http://127.0.0.1:26657/broadcast_tx_async?tx="somekey=somevalue"'
```

Output
```shell
{
  "jsonrpc": "2.0",
  "id": -1,
  "result": {
    "code": 0,
    "data": "",
    "log": "",
    "codespace": "",
    "hash": "17ED61261A5357FEE7ACDE4FAB154882A346E479AC236CFB2F22A2E8870A9C3D"
  }
}
```


```bash
curl 'http://127.0.0.1:26657/abci_query?data=0x736f6d656b6579'
```

```bash
{
  "jsonrpc": "2.0",
  "id": -1,
  "result": {
    "response": {
      "code": 0,
      "log": "exists",
      "info": "",
      "index": "0",
      "key": "c29tZWtleQ==",
      "value": "c29tZXZhbHVl",
      "proofOps": null,
      "height": "11",
      "codespace": ""
    }
  }
}
```

### Run test

```shell
cargo test --package db3-kvstore
```
