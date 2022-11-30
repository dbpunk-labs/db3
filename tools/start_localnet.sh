#! /bin/base
#
# start_localnet.sh
test_dir=`pwd`
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
    ./tendermint init
fi

# clean db3
killall -s 9 db3
if [ -e ./db ]
then
    rm -rf db
fi
../target/debug/db3 node >>db3.log 2>&1  &
sleep 1
./tendermint init && ./tendermint unsafe_reset_all && ./tendermint start
sleep 1

