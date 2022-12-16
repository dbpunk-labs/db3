#! /bin/bash

#
# gen_proto.sh

protoc -I=src/proto/proto db3_base.proto \
--js_out=import_style=commonjs:src/db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:src/db3js/src/pkg

protoc -I=src/proto/proto db3_mutation.proto \
--js_out=import_style=commonjs:src/db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:src/db3js/src/pkg

protoc -I=src/proto/proto db3_bill.proto \
--js_out=import_style=commonjs:src/db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:src/db3js/src/pkg

protoc -I=src/proto/proto db3_account.proto \
--js_out=import_style=commonjs:src/db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:src/db3js/src/pkg


protoc -I=src/proto/proto db3_node.proto \
--js_out=import_style=commonjs:src/db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:src/db3js/src/pkg

protoc -I=src/proto/proto db3_namespace.proto \
--js_out=import_style=commonjs:src/db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:src/db3js/src/pkg

protoc -I=src/proto/proto db3_session.proto \
--js_out=import_style=commonjs:src/db3js/src/pkg \
--grpc-web_out=import_style=typescript,mode=grpcwebtext:src/db3js/src/pkg




