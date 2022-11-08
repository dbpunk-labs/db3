#! /bin/bash
#
# gen_proto.sh
protoc -I=../proto/proto db3_base.proto \
--js_out=import_style=commonjs:. \
--grpc-web_out=import_style=commonjs,mode=grpcwebtext:.

protoc -I=../proto/proto db3_mutation.proto \
--js_out=import_style=commonjs:. \
--grpc-web_out=import_style=commonjs,mode=grpcwebtext:.

protoc -I=../proto/proto db3_bill.proto \
--js_out=import_style=commonjs:. \
--grpc-web_out=import_style=commonjs,mode=grpcwebtext:.

protoc -I=../proto/proto db3_account.proto \
--js_out=import_style=commonjs:. \
--grpc-web_out=import_style=commonjs,mode=grpcwebtext:.


protoc -I=../proto/proto db3_node.proto \
--js_out=import_style=commonjs:. \
--grpc-web_out=import_style=commonjs,mode=grpcwebtext:.



