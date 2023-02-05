# DB3 Network Docs

DB3 Network is an open-source and decentralized firebase firestore alternative to build fully decentralized dApps quickly with minimal engineering effort


<p align="center">
 <img width="600px" src="./images/position_web3.svg" align="center"/>
</p>


## Get Started

### Install


=== "Mac"
    ```
    wget https://github.com/dbpunk-labs/db3/releases/download/v0.2.6/db3-v0.2.6-macos-x86_64.tar.gz
    tar -zxvf db3-v0.2.6-macos-x86_64.tar.gz
    cd db3-v0.2.6-macos-x86_64 && export PATH=`pwd`/bin:$PATH
    ```
=== "Ubuntu"
    ```
    wget https://github.com/dbpunk-labs/db3/releases/download/v0.2.6/db3-v0.2.6-linux-x86_64.tar.gz
    tar -zxvf db3-v0.2.6-linux-x86_64.tar.gz
    cd db3-v0.2.6-linux-x86_64 && export PATH=`pwd`/bin:&PATH
    ```

### Create a key

```
db3 client init
Init key successfully!
db3 client show-key
 address                                    | scheme
--------------------------------------------+-----------
 0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b | secp256k1
```

this command will create a key used for signing message

### Create a Database

```
db3 console --url https://grpc.devnet.db3.network
db3>-$ new-db
 database address                           | transaction id
--------------------------------------------+----------------------------------------------
 0xffe0f0ea53dd3ccf6de1fc046a0f8eb68f98dded | ZJqQkwULNOuyVeeECGoHIHusyFTghsTWVJYMsg1afZM=
db3>-$ show-db --addr 0xffe0f0ea53dd3ccf6de1fc046a0f8eb68f98dded
 database address                           | sender address                             | releated transactions                        | collections 
--------------------------------------------+--------------------------------------------+----------------------------------------------+-------------
 0xffe0f0ea53dd3ccf6de1fc046a0f8eb68f98dded | 0x96bdb8e20fbd831fcb37dde9f81930a82ab5436b | ZJqQkwULNOuyVeeECGoHIHusyFTghsTWVJYMsg1afZM= |  
```

### Create a Collection

```
db3>-$  new-collection --addr 0xffe0f0ea53dd3ccf6de1fc046a0f8eb68f98dded  --name books --index '{"name":"idx1","fields":[{"field_path":"name","value_mode":{"Order":1}}]}'
send add collection done with tx
3lY5/WKRw53x33UUZ6gCvsN4axLrdcf9PD41HqNIOYA=
db3>-$ show-collection --addr 0xffe0f0ea53dd3ccf6de1fc046a0f8eb68f98dded
 name  | index
-------+----------------------------------------------------------------------------
 books | {"name":"idx1","fields":[{"field_path":"name","value_mode":{"Order":1}}]}
```
