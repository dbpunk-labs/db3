#! /bin/base
#
# start_localnet.sh
test_dir=`pwd`
BUILD_MODE='debug'
RUN_L1_CHAIN=""
export RUST_BACKTRACE=1
# the hardhat node rpc url
EVM_NODE_URL='http://127.0.0.1:8545'
ADMIN_ADDR='0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266'
## clean local process
ps -ef | grep db3 | grep store | grep -v grep | awk '{print $2}' | while read line; do kill $line;done
ps -ef | grep db3 | grep indexer | grep -v grep | awk '{print $2}' | while read line; do kill $line;done
ps -ef | grep ar_miner | grep -v grep | awk '{print $2}' | while read line; do kill $line;done
ps -ef | grep hardhat | grep -v grep | awk '{print $2}' | while read line; do kill $line;done

cd ${test_dir}/../metadata/ && npx hardhat node >${test_dir}/evm.log 2>&1 &
sleep 1
cd ${test_dir}/../metadata/ && bash deploy_to_local.sh >${test_dir}/contract.log
sleep 1
CONTRACT_ADDR=`cat ${test_dir}/contract.log | awk '{print $3}'`
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
mkdir -p ./keys
echo "start db3 store..."
../target/${BUILD_MODE}/db3 store --admin-addr=${ADMIN_ADDR}\
            --rollup-interval 60000 --block-interval=500\
            --contract-addr=${CONTRACT_ADDR} --evm-node-url=${EVM_NODE_URL}>store.log 2>&1 &
sleep 1
AR_ADDRESS=`less store.log | grep filestore | awk '{print $NF}'`
STORE_EVM_ADDRESS=`less store.log | grep evm | grep address | awk '{print $NF}'`
echo "start ar miner..."
bash ./ar_miner.sh> miner.log 2>&1 &
sleep 1
echo "request ar token to rollup node"
curl http://127.0.0.1:1984/mint/${AR_ADDRESS}/10000000000000000
echo "done!"
sleep 1

echo "start db3 indexer..."
../target/${BUILD_MODE}/db3 indexer  --admin-addr=${ADMIN_ADDR}\
    --contract-addr=${CONTRACT_ADDR}\
    --evm-node-url=${EVM_NODE_URL}> indexer.log 2>&1  &
sleep 1

echo "===========the account information=============="
echo "the AR address ${AR_ADDRESS}"
echo "the Admin address ${ADMIN_ADDR}"
echo "the Contract address ${CONTRACT_ADDR}"
echo "the Store Evm address ${STORE_EVM_ADDRESS}"

echo "===========the node information=============="
echo "rollup node http://127.0.0.1:26619"
echo "index node http://127.0.0.1:26639"
echo "ar mock server http://127.0.0.1:1984"
echo "evm node ${EVM_NODE_URL}"
