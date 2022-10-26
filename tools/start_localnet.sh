#! /bin/sh
#
# start_localnet.sh

test_dir=`pwd`
../target/debug/db3-kvstore &
sleep 1
tendermint init && tendermint unsafe_reset_all && tendermint start
