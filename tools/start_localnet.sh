#! /bin/sh
#
# start_localnet.sh

test_dir=`pwd`

killall db3
test -d db && rm -rf db
../target/debug/db3 node -r 1000000 >db3.log 2>&1  &
sleep 1
tendermint init && tendermint unsafe_reset_all && tendermint start
