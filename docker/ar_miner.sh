#! /bin/sh
#
# ar_miner.sh
# Copyright (C) 2023 jackwang <jackwang@jackwang-ub>
#
# Distributed under terms of the MIT license.
#

npx arlocal >arlocal.log 2>&1 &
sleep 2
curl http://127.0.0.1:1984/mint/$1/10000000000000
while true
do
    curl http://127.0.0.1:1984/mine
    sleep 2
done
