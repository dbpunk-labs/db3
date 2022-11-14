#! /bin/bash
#
# install_env.sh

# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# change toolchain to nightly
rustup default nightly
rustup target add wasm32-unknown-unknown --toolchain nightly
# install cmake on mac os
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    sudo apt install cmake
elif [[ "$OSTYPE" == "darwin"* ]]; then
    brew install cmake
else
    echo "$OSTYPE is not supported, please give us a issue https://github.com/dbpunk-labs/db3/issues/new/choose"
    exit 1
fi
git submodule init
