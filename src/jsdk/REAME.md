# Javascrpt Library for DB3

## Prepare the development environment

### install protobuf

use https://github.com/protocolbuffers/protobuf/releases/download/v21.9/protobuf-all-21.9.tar.gz

### install wasm-pack

go to https://rustwasm.github.io/wasm-pack/installer


## Build

### Generate from Protobuf

```
bash gen_proto.sh
```

### Buils wasm

```
wasm-pack build
```


