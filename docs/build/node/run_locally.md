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
