#! /bin/sh
#
# start_all.sh

echo "start etcd ..."
cd /etcd_dir && ./etcd > etcd.log 2>&1 &
sleep  2

export MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE
export MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
echo "start minio ..."
mkdir -p /data
cd /minio_dir && chmod +x minio && ./minio server /data  --console-address ":9001" > minio.log 2>&1 &
sleep 2

echo "start rtstore "
cd /rtstore_dir && chmod +x rtstore
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
RUST_LOG=info ./rtstore meta 9191 http://127.0.0.1:2379 /rtstore 127.0.0.1 http://127.0.0.1:9000 > meta.log 2>&1 &
sleep 1
tail -n 100 meta.log
RUST_LOG=info ./rtstore memory-node 9791 /tmp/binlog /tmp/test http://127.0.0.1:2379 /rtstore 127.0.0.1 > mem.log 2>&1 &
sleep 1
tail -n 100 mem.log
RUST_LOG=info ./rtstore compute-node 9193 http://127.0.0.1:2379 /rtstore 127.0.0.1 http://127.0.0.1:9000 > compute.log 2>&1 &
sleep 1
tail -n 100 compute.log
RUST_LOG=info ./rtstore frontend-node 9292 http://127.0.0.1:2379 /rtstore 0.0.0.0 ./vars.txt > fe.log 2>&1 &
sleep 1
tail -n 100 fe.log

echo "start rtstore done"

echo "You can use 'mysql -u root -h 127.0.0.1 -P 9292 ' to connect to rtstore"
while true; do sleep 1; done





