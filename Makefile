.PHONY: proto lint

# Regenerate the Go code for the gRPC API.
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/kv.proto

# Run the static checks on every package.
lint:
	gofmt -l .
	go vet ./...
	staticcheck ./...
