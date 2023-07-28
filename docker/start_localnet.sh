#! /bin/bash
#
echo "ADMIN ADDR ${ADMIN_ADDR}"
mkdir -p ./mutation_db ./state_db ./doc_db ./keys ./index_meta_db ./index_doc_db
echo "start store node..."
/usr/bin/db3 rollup --admin-addr=${ADMIN_ADDR} --bind-host 0.0.0.0 > rollup.log 2>&1 &
sleep 3
echo "start index node..."
/usr/bin/db3 index --admin-addr=${ADMIN_ADDR} --bind-host 0.0.0.0 > index.log 2>&1 &
sleep 3
npx serve -l 26629 -s /pages > pages.log 2>&1 &

AR_ADDRESS=`less rollup.log | grep Arweave | awk '{print $NF}'`
STORE_EVM_ADDRESS=`less rollup.log | grep Evm | grep address | awk '{print $NF}'`
echo "the ar account address ${AR_ADDRESS}"
echo "start ar testnet ..."
bash /usr/bin/ar_miner.sh > miner.log 2>&1 &
sleep 2
curl http://127.0.0.1:1984/mint/${AR_ADDRESS}/10000000000000
echo "Start the local db3 nodes successfully"
echo "The rollup node url: http://127.0.0.1:26619"
echo "The index node url: http://127.0.0.1:26639"
echo "The setup url: http://127.0.0.1:26629"
while true; do sleep 10 ; done
