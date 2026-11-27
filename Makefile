.PHONY: proto lint

# Regenerate the Go code for the gRPC API into api/kvpb.
proto:
	mkdir -p api/kvpb
	protoc -I api --go_out=api/kvpb --go_opt=paths=source_relative \
		--go-grpc_out=api/kvpb --go-grpc_opt=paths=source_relative \
		api/kv.proto

# Run the static checks on every package.
lint:
	gofmt -l .
	go vet ./...
	staticcheck ./...
