#! /bin/bash
#
# install_env.sh

echo "install rust"
# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# change toolchain to nightly
rustup default nightly
echo "install cmake protobuf nodejs"
# install cmake on mac os
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    sudo apt install cmake protobuf-compiler
elif [[ "$OSTYPE" == "darwin"* ]]; then
    brew install cmake protobuf node@18
else
    echo "$OSTYPE is not supported, please give us a issue https://github.com/dbpunk-labs/db3/issues/new/choose"
    exit 1
fi

echo "fetch submodule"
git submodule init
git submodule update

echo "install yarn "
corepack enable
corepack prepare yarn@stable --activate
echo "install ar local"
yarn global add arlocal
