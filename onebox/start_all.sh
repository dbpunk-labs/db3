#! /bin/sh
#
# start_all.sh

export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
RUST_LOG=info ../target/debug/rtstore memory-node 9791 /tmp/binlog /tmp/test http://127.0.0.1:2379 /rtstore 127.0.0.1 > mem.log 2>&1 &
echo "start memory node"
sleep 2
RUST_LOG=info ../target/debug/rtstore meta 9191 http://127.0.0.1:2379 /rtstore 127.0.0.1 http://127.0.0.1:9000 > meta.log 2>&1 &
echo "start meta node"
sleep 2
RUST_LOG=info ../target/debug/rtstore compute-node 9193 http://127.0.0.1:2379 /rtstore 127.0.0.1 http://127.0.0.1:9000 > compute.log 2>&1 &
echo "start compute node"
sleep 2
RUST_LOG=info ../target/debug/rtstore frontend-node 9292 http://127.0.0.1:2379 /rtstore 127.0.0.1 ../static/vars.txt > fe.log 2>&1 &
echo "start frontend node"




