#! /bin/sh
#
# start_localnet.sh

test_dir=`pwd`
if [ -e ./tendermint ]
then
    echo "tendermint_0.34.22_linux_amd64.tar.gz exist"
else
    wget https://github.com/tendermint/tendermint/releases/download/v0.34.22/tendermint_0.34.22_linux_amd64.tar.gz
    tar -zxf tendermint_0.34.22_linux_amd64.tar.gz
fi

killall -s 9 db3
test -d db && rm -rf db
../target/debug/db3 node -r 1000000 >db3.log 2>&1  &
sleep 1
tendermint init && tendermint unsafe_reset_all && tendermint start
