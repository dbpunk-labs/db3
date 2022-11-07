## json rpc of db3


### latest_blocks

example request
```
curl --header "Content-Type: application/json" --request POST --data '{"method": "latest_blocks", "params": [], "id": 1, "jsonrpc": "2.0"}' localhost:26670
```

example resposne
```
{
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "last_height": "502",
        "block_metas": [
            {
                "block_id": {
                    "hash": "85E0A39AD0A4223F2F570A969D3FADA58EACBC18432BC35E01730B9F4FF08A4B",
                    "part_set_header": {
                        "total": 1,
                        "hash": "5A540E9D187AEB8B7E9E785BC91D77EBA60C9D26DCA98B58CACFB20D436E4224"
                    }
                },
                "block_size": "603",
                "header": {
                    "version": {
                        "block": "11",
                        "app": "1"
                    },
                    "chain_id": "test-chain-ZtZvlU",
                    "height": "502",
                    "time": "2022-11-07T08:35:35.770057345Z",
                    "last_block_id": {
                        "hash": "00E49A9C37DB7481591F0E86E4DC38C8E3E87E8FEE882AFB69A7E37716D8FAD5",
                        "part_set_header": {
                            "total": 1,
                            "hash": "92F57E46B22842297FB6D437A61F7C16B9A8A2AA0BDE32C99ED4349FD3924081"
                        }
                    },
                    "last_commit_hash": "D8D0568C978AC531E49A75D9F8A13D712DBC5ADA26E8E1C8D4FACB57CE267081",
                    "data_hash": "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
                    "validators_hash": "F731A85ACD9629551D91F65511E1C6C23DED9AD4FE272934A5FA3D41E4422EA2",
                    "next_validators_hash": "F731A85ACD9629551D91F65511E1C6C23DED9AD4FE272934A5FA3D41E4422EA2",
                    "consensus_hash": "048091BC7DDC283F77BFBF91D73C44DA58C3DF8A9CBC867405D8B7F3DAADA22F",
                    "app_hash": "0000000000000000000000000000000000000000000000000000000000000000",
                    "last_results_hash": "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
                    "evidence_hash": "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
                    "proposer_address": "49F66106913FA8967EAF7A1D1D978BE4EED4199F"
                },
                "num_txs": "0"
            }
        ]
    }
}
```

### block

example request
```
curl --header "Content-Type: application/json" --request POST --data '{"method": "block", "params": ["85E0A39AD0A4223F2F570A969D3FADA58EACBC18432BC35E01730B9F4FF08A4B"], "id": 1, "jsonrpc": "2.0"}' localhost:26670
```

example response

```
{
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "block_id": {
            "hash": "85E0A39AD0A4223F2F570A969D3FADA58EACBC18432BC35E01730B9F4FF08A4B",
            "part_set_header": {
                "total": 1,
                "hash": "5A540E9D187AEB8B7E9E785BC91D77EBA60C9D26DCA98B58CACFB20D436E4224"
            }
        },
        "block": {
            "header": {
                "version": {
                    "block": "11",
                    "app": "1"
                },
                "chain_id": "test-chain-ZtZvlU",
                "height": "502",
                "time": "2022-11-07T08:35:35.770057345Z",
                "last_block_id": {
                    "hash": "00E49A9C37DB7481591F0E86E4DC38C8E3E87E8FEE882AFB69A7E37716D8FAD5",
                    "part_set_header": {
                        "total": 1,
                        "hash": "92F57E46B22842297FB6D437A61F7C16B9A8A2AA0BDE32C99ED4349FD3924081"
                    }
                },
                "last_commit_hash": "D8D0568C978AC531E49A75D9F8A13D712DBC5ADA26E8E1C8D4FACB57CE267081",
                "data_hash": "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
                "validators_hash": "F731A85ACD9629551D91F65511E1C6C23DED9AD4FE272934A5FA3D41E4422EA2",
                "next_validators_hash": "F731A85ACD9629551D91F65511E1C6C23DED9AD4FE272934A5FA3D41E4422EA2",
                "consensus_hash": "048091BC7DDC283F77BFBF91D73C44DA58C3DF8A9CBC867405D8B7F3DAADA22F",
                "app_hash": "0000000000000000000000000000000000000000000000000000000000000000",
                "last_results_hash": "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
                "evidence_hash": "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
                "proposer_address": "49F66106913FA8967EAF7A1D1D978BE4EED4199F"
            },
            "data": {
                "txs": null
            },
            "evidence": {
                "evidence": null
            },
            "last_commit": {
                "height": "501",
                "round": 0,
                "block_id": {
                    "hash": "00E49A9C37DB7481591F0E86E4DC38C8E3E87E8FEE882AFB69A7E37716D8FAD5",
                    "part_set_header": {
                        "total": 1,
                        "hash": "92F57E46B22842297FB6D437A61F7C16B9A8A2AA0BDE32C99ED4349FD3924081"
                    }
                },
                "signatures": [
                    {
                        "block_id_flag": 2,
                        "validator_address": "49F66106913FA8967EAF7A1D1D978BE4EED4199F",
                        "timestamp": "2022-11-07T08:35:35.770057345Z",
                        "signature": "cDvxGLNF/J/zV14DDcRFQcJTaYwfFNZ2UIPehfS95uxqfD3kl9CnGZcuhuwKE+AtVPk7lCJ8uY3O5L5xcb33Dg=="
                    }
                ]
            }
        }
    }
}
```

### mutation


example request

```
{"method": "mutation", "params": ["b22Ui6N2RNkUyESx7KBXbFNW0RghyUO4vA8rW/DadSc="], "id": 1, "jsonrpc": "2.0"}
```

example response
```
{
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "ns": "my_twitter",
        "kv_pairs": [
            {
                "key": "kkkk25",
                "value": "76616C75653235",
                "action": 0
            }
        ],
        "nonce": 1,
        "chain_id": 0,
        "chain_role": 10,
        "gas_price": null,
        "gas": 10,
        "signature": "B7EB0B947FD1E08355804907D329246C3590C552BEB7595C035C66200D61DC5B7FD89658A3D3AD1DA68BDC265ADE81BE0BE06E8CA0547437BDB7DD3A8233411F00"
    }
}
```

### account

example request
```
curl --header "Content-Type: application/json" --request POST --data '{"method": "account", "params": ["0x0dce49e41905e6c0c5091adcedee2dee524a3b06"], "id": 1, "jsonrpc": "2.0"}' localhost:26670
```
example response 
```
{
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "total_bills": {
            "utype": 1,
            "amount": 3200
        },
        "total_storage_in_bytes": 74,
        "total_mutation_count": 1,
        "total_query_session_count": 0,
        "credits": {
            "utype": 0,
            "amount": 10
        },
        "nonce": 0,
        "bill_next_id": 0
    }
}
```

### net_info

example request
```
curl --header "Content-Type: application/json" --request POST --data '{"method": "net_info", "params": [], "id": 1, "jsonrpc": "2.0"}' localhost:26670
```

example response
```
{
  "jsonrpc": "2.0",
  "id": -1,
  "result": {
    "listening": true,
    "listeners": [
      "Listener(@18.117.125.43:26656)"
    ],
    "n_peers": "3",
    "peers": [
      {
        "node_info": {
          "protocol_version": {
            "p2p": "8",
            "block": "11",
            "app": "1"
          },
          "id": "fb07c70e0a230755b0006ce49de8ff38339214c3",
          "listen_addr": "18.142.114.145:26656",
          "network": "chain-Gk7GrO",
          "version": "0.34.22",
          "channels": "40202122233038606100",
          "moniker": "node0",
          "other": {
            "tx_index": "on",
            "rpc_address": "tcp://0.0.0.0:26657"
          }
        },
        "is_outbound": true,
        "connection_status": {
          "Duration": "236120999073348",
          "SendMonitor": {
            "Start": "2022-11-04T15:20:16.58Z",
            "Bytes": "446069262",
            "Samples": "949837",
            "InstRate": "1800",
            "CurRate": "2626",
            "AvgRate": "1889",
            "PeakRate": "29140",
            "BytesRem": "0",
            "Duration": "236121000000000",
            "Idle": "140000000",
            "TimeRem": "0",
            "Progress": 0,
            "Active": true
          },
          "RecvMonitor": {
            "Start": "2022-11-04T15:20:16.58Z",
            "Bytes": "450462463",
            "Samples": "869447",
            "InstRate": "2257",
            "CurRate": "2315",
            "AvgRate": "1908",
            "PeakRate": "23770",
            "BytesRem": "0",
            "Duration": "236121000000000",
            "Idle": "140000000",
            "TimeRem": "0",
            "Progress": 0,
            "Active": true
          },
          "Channels": [
            {
              "ID": 48,
              "SendQueueCapacity": "1",
              "SendQueueSize": "0",
              "Priority": "5",
              "RecentlySent": "0"
            },
            {
              "ID": 64,
              "SendQueueCapacity": "1000",
              "SendQueueSize": "0",
              "Priority": "5",
              "RecentlySent": "0"
            },
            {
              "ID": 32,
              "SendQueueCapacity": "100",
              "SendQueueSize": "0",
              "Priority": "6",
              "RecentlySent": "1924"
            },
            {
              "ID": 33,
              "SendQueueCapacity": "100",
              "SendQueueSize": "0",
              "Priority": "10",
              "RecentlySent": "5734"
            },
            {
              "ID": 34,
              "SendQueueCapacity": "100",
              "SendQueueSize": "0",
              "Priority": "7",
              "RecentlySent": "10202"
            },
            {
              "ID": 35,
              "SendQueueCapacity": "2",
              "SendQueueSize": "0",
              "Priority": "1",
              "RecentlySent": "2"
            },
            {
              "ID": 56,
              "SendQueueCapacity": "1",
              "SendQueueSize": "0",
              "Priority": "6",
              "RecentlySent": "0"
            },
            {
              "ID": 96,
              "SendQueueCapacity": "10",
              "SendQueueSize": "0",
              "Priority": "5",
              "RecentlySent": "0"
            },
            {
              "ID": 97,
              "SendQueueCapacity": "10",
              "SendQueueSize": "0",
              "Priority": "3",
              "RecentlySent": "0"
            },
            {
              "ID": 0,
              "SendQueueCapacity": "10",
              "SendQueueSize": "0",
              "Priority": "1",
              "RecentlySent": "0"
            }
          ]
        },
        "remote_ip": "18.142.114.145"
      },
      {
        "node_info": {
          "protocol_version": {
            "p2p": "8",
            "block": "11",
            "app": "1"
          },
          "id": "dc7715d279f8f9de6bc42f29cb684749b8ecec75",
          "listen_addr": "18.162.230.6:26656",
          "network": "chain-Gk7GrO",
          "version": "0.34.22",
          "channels": "40202122233038606100",
          "moniker": "node1",
          "other": {
            "tx_index": "on",
            "rpc_address": "tcp://0.0.0.0:26657"
          }
        },
        "is_outbound": true,
        "connection_status": {
          "Duration": "236120048580126",
          "SendMonitor": {
            "Start": "2022-11-04T15:20:17.54Z",
            "Bytes": "446099240",
            "Samples": "948563",
            "InstRate": "530",
            "CurRate": "1894",
            "AvgRate": "1889",
            "PeakRate": "21470",
            "BytesRem": "0",
            "Duration": "236120000000000",
            "Idle": "40000000",
            "TimeRem": "0",
            "Progress": 0,
            "Active": true
          },
          "RecvMonitor": {
            "Start": "2022-11-04T15:20:17.54Z",
            "Bytes": "447633478",
            "Samples": "887600",
            "InstRate": "5033",
            "CurRate": "2539",
            "AvgRate": "1896",
            "PeakRate": "23630",
            "BytesRem": "0",
            "Duration": "236120000000000",
            "Idle": "20000000",
            "TimeRem": "0",
            "Progress": 0,
            "Active": true
          },
          "Channels": [
            {
              "ID": 48,
              "SendQueueCapacity": "1",
              "SendQueueSize": "0",
              "Priority": "5",
              "RecentlySent": "0"
            },
            {
              "ID": 64,
              "SendQueueCapacity": "1000",
              "SendQueueSize": "0",
              "Priority": "5",
              "RecentlySent": "0"
            },
            {
              "ID": 32,
              "SendQueueCapacity": "100",
              "SendQueueSize": "0",
              "Priority": "6",
              "RecentlySent": "1665"
            },
            {
              "ID": 33,
              "SendQueueCapacity": "100",
              "SendQueueSize": "0",
              "Priority": "10",
              "RecentlySent": "4112"
            },
            {
              "ID": 34,
              "SendQueueCapacity": "100",
              "SendQueueSize": "0",
              "Priority": "7",
              "RecentlySent": "8986"
            },
            {
              "ID": 35,
              "SendQueueCapacity": "2",
              "SendQueueSize": "0",
              "Priority": "1",
              "RecentlySent": "78"
            },
            {
              "ID": 56,
              "SendQueueCapacity": "1",
              "SendQueueSize": "0",
              "Priority": "6",
              "RecentlySent": "0"
            },
            {
              "ID": 96,
              "SendQueueCapacity": "10",
              "SendQueueSize": "0",
              "Priority": "5",
              "RecentlySent": "0"
            },
            {
              "ID": 97,
              "SendQueueCapacity": "10",
              "SendQueueSize": "0",
              "Priority": "3",
              "RecentlySent": "0"
            },
            {
              "ID": 0,
              "SendQueueCapacity": "10",
              "SendQueueSize": "0",
              "Priority": "1",
              "RecentlySent": "0"
            }
          ]
        },
        "remote_ip": "18.162.230.6"
      },
      {
        "node_info": {
          "protocol_version": {
            "p2p": "8",
            "block": "11",
            "app": "1"
          },
          "id": "5e223414d6f64c4174132cac44febfdc73c6b402",
          "listen_addr": "13.41.65.17:26656",
          "network": "chain-Gk7GrO",
          "version": "0.34.22",
          "channels": "40202122233038606100",
          "moniker": "node3",
          "other": {
            "tx_index": "on",
            "rpc_address": "tcp://0.0.0.0:26657"
          }
        },
        "is_outbound": false,
        "connection_status": {
          "Duration": "235732489203424",
          "SendMonitor": {
            "Start": "2022-11-04T15:26:45.1Z",
            "Bytes": "429559234",
            "Samples": "903770",
            "InstRate": "2242",
            "CurRate": "2315",
            "AvgRate": "1822",
            "PeakRate": "49571",
            "BytesRem": "0",
            "Duration": "235732440000000",
            "Idle": "40000000",
            "TimeRem": "0",
            "Progress": 0,
            "Active": true
          },
          "RecvMonitor": {
            "Start": "2022-11-04T15:26:45.1Z",
            "Bytes": "428638306",
            "Samples": "853598",
            "InstRate": "0",
            "CurRate": "2396",
            "AvgRate": "1818",
            "PeakRate": "35550",
            "BytesRem": "0",
            "Duration": "235732480000000",
            "Idle": "120000000",
            "TimeRem": "0",
            "Progress": 0,
            "Active": true
          },
          "Channels": [
            {
              "ID": 48,
              "SendQueueCapacity": "1",
              "SendQueueSize": "0",
              "Priority": "5",
              "RecentlySent": "0"
            },
            {
              "ID": 64,
              "SendQueueCapacity": "1000",
              "SendQueueSize": "0",
              "Priority": "5",
              "RecentlySent": "0"
            },
            {
              "ID": 32,
              "SendQueueCapacity": "100",
              "SendQueueSize": "0",
              "Priority": "6",
              "RecentlySent": "1789"
            },
            {
              "ID": 33,
              "SendQueueCapacity": "100",
              "SendQueueSize": "0",
              "Priority": "10",
              "RecentlySent": "4625"
            },
            {
              "ID": 34,
              "SendQueueCapacity": "100",
              "SendQueueSize": "0",
              "Priority": "7",
              "RecentlySent": "9033"
            },
            {
              "ID": 35,
              "SendQueueCapacity": "2",
              "SendQueueSize": "0",
              "Priority": "1",
              "RecentlySent": "98"
            },
            {
              "ID": 56,
              "SendQueueCapacity": "1",
              "SendQueueSize": "0",
              "Priority": "6",
              "RecentlySent": "0"
            },
            {
              "ID": 96,
              "SendQueueCapacity": "10",
              "SendQueueSize": "0",
              "Priority": "5",
              "RecentlySent": "0"
            },
            {
              "ID": 97,
              "SendQueueCapacity": "10",
              "SendQueueSize": "0",
              "Priority": "3",
              "RecentlySent": "0"
            },
            {
              "ID": 0,
              "SendQueueCapacity": "10",
              "SendQueueSize": "0",
              "Priority": "1",
              "RecentlySent": "7"
            }
          ]
        },
        "remote_ip": "13.41.65.17"
      }
    ]
  }
}
```

### validators

example request

```
curl --header "Content-Type: application/json" --request POST --data '{"method": "validators", "params": [81], "id": 1, "jsonrpc": "2.0"}' localhost:26670
```

example response

```
{
    "jsonrpc": "2.0",
    "id": 1,
    "result": {
        "block_height": "81",
        "validators": [
            {
                "address": "49F66106913FA8967EAF7A1D1D978BE4EED4199F",
                "pub_key": {
                    "type": "tendermint/PubKeyEd25519",
                    "value": "BQOCIcbYVlQgIIKDpYblP1DWZLdoV+V6hzBwvI5TjuU="
                },
                "power": "10",
                "name": null
            }
        ],
        "total": "1"
    }
}
```

