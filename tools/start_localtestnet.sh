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
        wget https://github.com/tendermint/tendermint/releases/download/v0.34.22/tendermint_0.34.22_linux_amd64.tar.gz
        mv tendermint_0.34.22_linux_amd64.tar.gz tendermint.tar.gz
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        wget https://github.com/tendermint/tendermint/releases/download/v0.34.22/tendermint_0.34.22_darwin_amd64.tar.gz
        mv tendermint_0.34.22_darwin_amd64.tar.gz tendermint.tar.gz
    else
        echo "$OSTYPE is not supported, please give us a issue https://github.com/dbpunk-labs/db3/issues/new/choose"
        exit 1
    fi
    tar -zxf tendermint.tar.gz
fi

# clean db3
killall  db3 ganache 
if [ -e ./db0 ]
then
    rm -rf db0 db1 db2 db3
fi
if [ -e ./bridge.db ]
then
    rm bridge.db
fi
if [ -e ./node0 ]
then
    rm -rf node0 node1 node2 node3
fi

# the local testnet config
tar -zxf local_testnet.tar.gz
echo "start db3 validator node0 with grpc address http://127.0.0.1:16659"
export RUST_BACKTRACE=1
../target/${BUILD_MODE}/db3 start --public-grpc-port 16659 --public-json-rpc-port 16670 --abci-port 16658 --tendermint-port 16657 --db-path db0 >db30.log 2>&1  &
sleep 1
./tendermint start --home node0 > tm0.log 2>&1 &
sleep 1

echo "start db3 validator node1 with grpc address http://127.0.0.1:26659"
export RUST_BACKTRACE=1
../target/${BUILD_MODE}/db3 start --public-grpc-port 26659 --public-json-rpc-port 26670 --abci-port 26658 --tendermint-port 26657 --db-path db1 >db31.log 2>&1  &
sleep 1
./tendermint start --home node1 > tm1.log 2>&1 &
sleep 1

echo "start db3 validator node2 with grpc address http://127.0.0.1:36659"
export RUST_BACKTRACE=1
../target/${BUILD_MODE}/db3 start --public-grpc-port 36659 --public-json-rpc-port 36670 --abci-port 36658 --tendermint-port 36657 --db-path db2 >db32.log 2>&1  &
sleep 1
./tendermint start --home node2 > tm2.log 2>&1 &
sleep 1

echo "start db3 normal node3 with grpc address http://127.0.0.1:46659"
export RUST_BACKTRACE=1
../target/${BUILD_MODE}/db3 start --disable-query-session --public-grpc-port 46659 --public-json-rpc-port 46670 --abci-port 46658 --tendermint-port 46657 --db-path db3 >db33.log 2>&1  &
sleep 1
./tendermint start --home node3 > tm3.log 2>&1 &
sleep 1
echo "start db3 indexer..."
../target/${BUILD_MODE}/db3 indexer --public-grpc-port 46639 --db3_storage_grpc_url 46659 >indexer.log 2>&1  &
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
