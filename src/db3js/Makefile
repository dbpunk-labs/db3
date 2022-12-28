

PROTO_PATH = ../proto/proto
PROTOS	:= $(shell find ${PROTO_PATH} -name '*.proto')


generate: $(PROTOS)
	@npx protoc \
		--ts_out src/pkg/ \
		--ts_opt enable_angular_annotations \
		--ts_opt long_type_string \
		--proto_path $(PROTO_PATH) \
		--proto_path node_modules/@protobuf-ts/plugin/ \
		--experimental_allow_proto3_optional \
		$^
	@echo "'${@}' done"

