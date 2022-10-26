# DB3 Network

DB3 is a modular database network including three modules

**Kv Storage Shard Chains**

Every shard chain works as a kv storage engine and records the bills for each account

**DVM Compute Layer**

Every node in compute layer has two main functions
1. provide dvm execution service to client
2. validate the result of dvm execution in mempool

**Lazy Settlement Chain**

The Settlement Chain will settle all bills from storage shard chains every 10 minutes
