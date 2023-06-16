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
ps -ef | grep ar_miner | grep -v grep | awk '{print $2}' | while read line; do kill $line;done
if [ -e ./db ]
then
    rm -rf db
fi
if [ -e ./mutation_db ]
then
    rm -rf ./mutation_db
fi

if [ -e ./state_db ]
then
    rm -rf ./state_db
fi
if [ -e ./doc_db ]
then
    rm -rf ./doc_db
fi

# clean indexer
if [ -e ./indexer_doc_db ]
then
    rm -rf indexer_doc_db
fi

if [ -e ./indexer_meta_db ]
then
    rm -rf indexer_meta_db
fi

echo "start ar miner..."
bash ./ar_miner.sh > miner.log 2>&1 &
echo "start db3 node..."
./tendermint init > tm.log 2>&1 
export RUST_BACKTRACE=1
../target/${BUILD_MODE}/db3 start >db3.log 2>&1  &
sleep 1
echo "start tendermint node..."
./tendermint unsafe_reset_all >> tm.log 2>&1  && ./tendermint start >> tm.log 2>&1 &
sleep 1

echo "start db3 store..."
../target/${BUILD_MODE}/db3 store --rollup-interval 60000 --block-interval=500 >store.log 2>&1  &
sleep 1

echo "start db3 indexer..."
../target/${BUILD_MODE}/db3 indexer >indexer.log 2>&1  &
sleep 1

while true; do sleep 1 ; done
