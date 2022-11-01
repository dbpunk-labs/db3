#! /bin/sh
#
# start_localnet.sh

test_dir=`pwd`
../target/debug/db3 >db3.log 2>&1  &
sleep 1
tendermint init && tendermint unsafe_reset_all && tendermint start
