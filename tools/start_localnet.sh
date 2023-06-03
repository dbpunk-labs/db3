#! /bin/base
#
# start_localnet.sh
killall db3 tendermint
test_dir=`pwd`
BUILD_MODE='debug'
RUN_L1_CHAIN=""
if [[ $1 == 'release' ]] ; then
  BUILD_MODE='release'
fi

echo "BUILD MODE: ${BUILD_MODE}"
if [ -e ./tendermint ]
then
    echo "tendermint exist"
else
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        wget https://github.com/tendermint/tendermint/releases/download/v0.37.0-rc2/tendermint_0.37.0-rc2_linux_amd64.tar.gz
        mv tendermint_0.37.0-rc2_linux_amd64.tar.gz tendermint.tar.gz
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        wget https://github.com/tendermint/tendermint/releases/download/v0.37.0-rc2/tendermint_0.37.0-rc2_darwin_amd64.tar.gz
        mv tendermint_0.37.0-rc2_darwin_amd64.tar.gz tendermint.tar.gz
    else
        echo "$OSTYPE is not supported, please give us a issue https://github.com/dbpunk-labs/db3/issues/new/choose"
        exit 1
    fi
    tar -zxf tendermint.tar.gz
fi

# clean db3
killall  db3 ganache 
if [ -e ./db ]
then
    rm -rf db
fi
if [ -e ./bridge.db ]
then
    rm bridge.db
fi
# clean indexer
if [ -e ./indexer ]
then
    rm -rf indexer
fi
echo "start db3 node..."
./tendermint init > tm.log 2>&1 
export RUST_BACKTRACE=1
../target/${BUILD_MODE}/db3 start >db3.log 2>&1  &
sleep 1
echo "start tendermint node..."
./tendermint unsafe_reset_all >> tm.log 2>&1  && ./tendermint start >> tm.log 2>&1 &
sleep 1
echo "start db3 indexer..."
../target/${BUILD_MODE}/db3 indexer >indexer.log 2>&1  &
sleep 1


if [[ $RUN_L1_CHAIN == 'OK' ]]; then
    echo "start evm chain network..."
    ganache --chain.chainId 1 -m 'road entire survey elevator employ toward city flee pupil vessel flock point' > evm.log 2>&1 &
    sleep 2
    echo "deploy rollup contract to evm chain"
    cd ${test_dir}/../bridge && bash deploy_to_local.sh > address.log
    export TOKEN_ADDRESS=`less address.log | awk '{print $3}'`
    export ADDRESS=`less address.log | awk '{print $6}'`
    echo "rollup address ${ADDRESS}"
    echo "erc20 address ${TOKEN_ADDRESS}"
    echo "start db3 bridge node ..."
    cd ${test_dir} && ../target/${BUILD_MODE}/db3 bridge --evm-chain-ws ws://127.0.0.1:8545 --evm-chain-id 1 --contract-address ${ADDRESS} --db_path ${test_dir}/bridge.db > bridge.log 2>&1 &
    sleep 1
    echo "start db3 faucet node ..."
    cd ${test_dir} && ../target/${BUILD_MODE}/db3 faucet --evm-chain-ws ws://127.0.0.1:8545  --token-address ${TOKEN_ADDRESS} --db_path ${test_dir}/faucet.db > faucet.log 2>&1 &
    echo "start local development done!"
fi
while true; do sleep 1 ; done
