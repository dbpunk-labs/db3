#! /bin/bash
#
# gen_proto.sh
PROTOC_GEN_TS_PATH="./node_modules/.bin/protoc-gen-ts"
OUT_DIR="./pkg"

# mkdir -p pkg

# ./node_modules/.bin/pbjs --es6 -t static-module -w es6 -o ../db3js/src/pkg/db3_proto.js ../proto/proto/*.proto
# ./node_modules/.bin/pbts -o ../db3js/src/pkg/db3_proto.d.ts ../db3js/src/pkg/db3_proto.js
# protoc -I=../proto/proto --ts_out=./pkg db3_base.proto
# protoc -I=../proto/proto --ts_out=./pkg db3_mutation.proto
# protoc -I=../proto/proto --ts_out=./pkg db3_bill.proto
# protoc -I=../proto/proto --ts_out=./pkg db3_account.proto
# protoc -I=../proto/proto --ts_out=./pkg db3_node.proto
    

protoc -I=../proto/proto db3_base.proto \
--js_out=import_style=commonjs:../db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:../db3js/src/pkg

protoc -I=../proto/proto db3_mutation.proto \
--js_out=import_style=commonjs:../db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:../db3js/src/pkg

protoc -I=../proto/proto db3_bill.proto \
--js_out=import_style=commonjs:../db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:../db3js/src/pkg

protoc -I=../proto/proto db3_account.proto \
--js_out=import_style=commonjs:../db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:../db3js/src/pkg


protoc -I=../proto/proto db3_node.proto \
--js_out=import_style=commonjs:../db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:../db3js/src/pkg



