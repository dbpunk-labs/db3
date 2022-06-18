# rtstore

[![CI](https://github.com/rtstore/rtstore/workflows/CI/badge.svg)](https://github.com/rtstore/rtstore/actions)
[![codecov](https://codecov.io/gh/rtstore/rtstore/branch/main/graph/badge.svg?token=A2P47OWC5H)](https://codecov.io/gh/rtstore/rtstore)

rtstore is a postgres + mysql compatible and (cloud + blockchain)-native timeseries database for web3 data analytics aiming to help developers use blockchain data out of box

## How to Build

```commandline
git clone https://github.com/rtstore/rtstore.git
cd rstore && cargo build
```

## How to use

```commandline
docker pull ghcr.io/rtstore/rtstore:0.1.0
docker run -p 9292:9292 -dt ghcr.io/rtstore/rtstore:0.1.0
74aed461005d57a0c9184bf1734066a66bcd0054dbeade49ee0b324dc0f94def
# use your own docker container id
docker logs 74aed461005d57a0c9184bf1734066a66bcd0054dbeade49ee0b324dc0f94def
start etcd ...
start minio ...
start rtstore
 2022-06-18T04:21:50.512Z INFO  rtstore::store > connect to etcd http://127.0.0.1:2379 done
 2022-06-18T04:21:50.516Z INFO  rtstore        > start metaserver on addr 127.0.0.1:9191
 2022-06-18T04:21:51.519Z INFO  rtstore::store > connect to etcd http://127.0.0.1:2379 done
 2022-06-18T04:21:51.521Z INFO  rtstore        > start memory node server on addr 127.0.0.1:9791
 2022-06-18T04:21:52.526Z INFO  rtstore::store > connect to etcd http://127.0.0.1:2379 done
 2022-06-18T04:21:52.529Z INFO  rtstore        > start compute node server on addr 127.0.0.1:9193
 2022-06-18T04:21:53.531Z INFO  rtstore > start frontend node ...
 2022-06-18T04:21:53.531Z INFO  rtstore::store > connect to etcd http://127.0.0.1:2379 done
 2022-06-18T04:21:53.533Z INFO  rtstore::sdk   > connect meta node http://127.0.0.1:9191
 2022-06-18T04:21:53.533Z INFO  rtstore::sdk   > connect memory node http://127.0.0.1:9791
 2022-06-18T04:21:53.534Z INFO  rtstore::sdk   > connect compute node http://127.0.0.1:9193
 2022-06-18T04:21:53.534Z INFO  rtstore        > start frontend node on addr 0.0.0.0:9292
start rtstore done
You can use 'mysql -u root -h 127.0.0.1 -P 9292 ' to connect to rtstore

mysql -u root -h 127.0.0.1 -P 9292
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show databases;
Query OK, 0 rows affected (0.00 sec)

mysql> create database db1;
Query OK, 1 row affected (0.01 sec)

mysql> use db1;


Database changed
mysql> create table t1( col1 int, col2 varchar(255));
Query OK, 1 row affected (0.03 sec)

mysql> insert into t1 values(10, 'hello world');
Query OK, 1 row affected (0.00 sec)

mysql> select * from t1;
+------+-------------+
| col1 | col2        |
+------+-------------+
|   10 | hello world |
+------+-------------+
1 row in set (0.01 sec)

mysql> describe t1;
+-------+--------------+------+------+---------+-------+
| Field | Type         | Null | Key  | Default | Extra |
+-------+--------------+------+------+---------+-------+
| col1  | int          | YES  |      |         |       |
| col2  | varchar(255) | YES  |      |         |       |
+-------+--------------+------+------+---------+-------+
2 rows in set (0.00 sec)

mysql> exit;
```

## License
Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
See [CONTRIBUTING.md](CONTRIBUTING.md).
