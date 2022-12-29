

PROTO_PATH = ../proto/proto
PROTOS	:= $(shell find ${PROTO_PATH} -name '*.proto')


generate: $(PROTOS)
	@npx protoc \
		--ts_out src/pkg/ \
		--ts_opt optimize_code_size\
		--ts_opt long_type_string \
		--proto_path $(PROTO_PATH) \
		--proto_path node_modules/@protobuf-ts/plugin/ \
		$^
	@echo "'${@}' done"

