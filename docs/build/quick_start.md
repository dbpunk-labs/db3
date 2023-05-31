Build a demo on DB3 Network is super easy, follow the steps:

# **Step1: Create or Find a database**

You can create or find a database on **[Data Manager System](https://dms.db3.network/)**  
Every public databases on DB3 can be find and be reused by every builder

# **Step2: Build dApps based on the database**

Since you've got a database , now you can build dApps based on the database.

Here is a Demo example you may refer to **[Message_wall](https://github.com/dbpunk-labs/message-wall)**

Or try **[On line Website](https://message-wall-iota.vercel.app/)** of the demo

## **Chosing Nodes**

DB3 network is composed by a group of nodes. You can chose what ever node to connect when building a dApp, or you can run self-running a node.

### **Find exsiting node**

We have two types of node: Normal Nodes & Validator Nodes.
Both types of node can be connected directly, the differences is that you have to sign when querying data from the validator nodes while no signiture is required when querying from normal nodes.
The normal nodes will give user a better using experience but sacrifice some data security. All the data querid from normal nodes is not verified and this means the truth of the data cannot be trusted. While validator nodes do not have a security and trust problem but you have to sign for every [Query Session]()

**Normal Nodes**

- https://grpc.devnet.db3.network

**Validator Nodes**

- http://18.162.230.6:26659
- http://16.163.108.68:26659
- http://18.162.114.26:26659

### **Running a local Node**

Use this command line to download and install a client

```
$ curl --proto '=https' --tlsv1.2 -sSf https://up.db3.network/db3up_init.sh | sh
```

_Note: **curl** and **python3** are required in your environment_

A `db3` command and `db3up` command will be available on your terminal if everything goes well

> Note: If you encounter the error 'db3 not found' or 'db3up not found', use the following solution to resolve it:  
> Run the `source ~/.zshrc` if you use zsh or `source ~/.bashrc` if you use bash

You have two options available. The first is to host an independent local network, while the second involves connecting to a community network that will synchronize data from remote nodes.

**As independent local network**  
Command line

```
$ db3up localnet
```

This command will init a local network at the end point of `http://127.0.0.1:26659`

**As Community node**  
Command line

```
$ db3up join_devnet
```

This command will also init a local network at the end point of `http://127.0.0.1:26659` but as a communit node and it will synchronize data from remote nodes and keep same block height
