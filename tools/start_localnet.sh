#! /bin/base
#
# start_localnet.sh
test_dir=`pwd`
BUILD_MODE='debug'
RUN_L1_CHAIN=""
export RUST_BACKTRACE=1
# the hardhat node rpc url
EVM_NODE_URL='ws://127.0.0.1:8545'
ADMIN_ADDR='0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'
echo "start to clean"

## clean local process
ps -ef | grep db3 | grep rollup | grep -v grep | awk '{print $2}' | while read line; do kill $line;done
ps -ef | grep db3 | grep index | grep -v grep | awk '{print $2}' | while read line; do kill $line;done
ps -ef | grep arlocal | grep -v grep | awk '{print $2}' | while read line; do kill $line;done
ps -ef | grep ar_miner | grep -v grep | awk '{print $2}' | while read line; do kill $line;done
ps -ef | grep arlocal_db3 | grep node | grep -v grep | awk '{print $2}' | while read line; do kill $line;done
ps -ef | grep hardhat | grep -v grep | awk '{print $2}' | while read line; do kill $line;done
echo "start the all process"

cd ${test_dir}/../metadata/ && npx hardhat node >${test_dir}/evm.log 2>&1 &
sleep 2
cd ${test_dir}/../metadata/ && bash deploy_to_local.sh >${test_dir}/contract.log
sleep 2
CONTRACT_ADDR=`cat ${test_dir}/contract.log | grep 'store address' | awk '{print $3}'`
cd ${test_dir}

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
if [ -e ./index_doc_db ]
then
    rm -rf index_doc_db
fi

if [ -e ./index_meta_db ]
then
    rm -rf index_meta_db
fi
if [ -e ./index_state_db ]
then
    rm -rf index_state_db
fi
mkdir -p ./keys
echo "start data rollup node..."
../target/${BUILD_MODE}/db3 rollup --block-interval=500 --admin-addr=${ADMIN_ADDR}>rollup.log 2>&1 &
sleep 1
AR_ADDRESS=`less rollup.log | grep Arweave | awk '{print $NF}'`
STORE_EVM_ADDRESS=`less rollup.log | grep Evm | grep address | awk '{print $NF}'`
echo "start ar miner..."
bash ./ar_miner.sh> miner.log 2>&1 &
sleep 5
echo "start data index node..."
../target/${BUILD_MODE}/db3 index  --admin-addr=${ADMIN_ADDR} > index.log 2>&1  &
curl --connect-timeout 5 http://127.0.0.1:1984/mint/gXJVsUCAmDqv9XeZui0MB2EdGPQEhN86QEnKY0_7vPc/10000000000000000
sleep 1
echo "\n===========the account information=============="
echo "the AR address ${AR_ADDRESS}"
echo "the Admin address ${ADMIN_ADDR}"
echo "the Contract address ${CONTRACT_ADDR}"
echo "the Rollup Evm address ${STORE_EVM_ADDRESS}"

echo "===========the node information=============="
echo "data rollup node http://127.0.0.1:26619"
echo "data index node http://127.0.0.1:26639"
echo "ar mock server http://127.0.0.1:1984"
echo "evm node ${EVM_NODE_URL}"

