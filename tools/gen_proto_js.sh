#! /bin/bash

#
# gen_proto.sh

sudo npm install -g protoc-gen-ts
protoc --ts_out ./src/db3js/src/pkg --proto_path src/proto/proto src/proto/proto/db3_base.proto
protoc --ts_out ./src/db3js/src/pkg --proto_path src/proto/proto src/proto/proto/db3_mutation.proto
protoc --ts_out ./src/db3js/src/pkg --proto_path src/proto/proto src/proto/proto/db3_bill.proto
protoc --ts_out ./src/db3js/src/pkg --proto_path src/proto/proto src/proto/proto/db3_account.proto
protoc --ts_out ./src/db3js/src/pkg --proto_path src/proto/proto src/proto/proto/db3_namespace.proto
protoc --ts_out ./src/db3js/src/pkg --proto_path src/proto/proto src/proto/proto/db3_session.proto
protoc --ts_out ./src/db3js/src/pkg --proto_path src/proto/proto src/proto/proto/db3_node.proto

