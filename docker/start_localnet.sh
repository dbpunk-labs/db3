#! /bin/bash
#
echo "ADMIN ADDR ${ADMIN_ADDR}"
mkdir -p ./mutation_db ./state_db ./doc_db ./keys ./index_meta_db ./index_doc_db
echo "start store node..."
/usr/bin/db3 store --admin-addr=${ADMIN_ADDR} --public-host 0.0.0.0 --rollup-interval 30000 --contract-addr=0xb9709ce5e749b80978182db1bedfb8c7340039a9 --evm-node-url=https://polygon-mumbai.g.alchemy.com/v2/kiuid-hlfzpnletzqdvwo38iqn0giefr > store.log 2>&1 &
sleep 3
echo "start index node..."
/usr/bin/db3 indexer --admin-addr=${ADMIN_ADDR} --public-host 0.0.0.0 --contract-addr=0xb9709ce5e749b80978182db1bedfb8c7340039a9 --evm-node-url=https://polygon-mumbai.g.alchemy.com/v2/kiuid-hlfzpnletzqdvwo38iqn0giefr> indexer.log 2>&1 &
sleep 3

npx serve -l 26629 -s /pages > pages.log 2>&1 &

AR_ADDRESS=`cat /store.log | grep filestore | awk '{print $NF}'`
echo "the ar account address ${AR_ADDRESS}"
echo "start ar testnet ..."
bash /usr/bin/ar_miner.sh > miner.log 2>&1 &
sleep 2
curl http://127.0.0.1:1984/mint/${AR_ADDRESS}/10000000000000
echo "Start the local db3 nodes successfully"
echo "The storage node url: http://127.0.0.1:26619"
echo "The index node url: http://127.0.0.1:26639"
echo "The console node url: http://127.0.0.1:26629"
while true; do sleep 10 ; done
