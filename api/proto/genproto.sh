protoc \
  --proto_path=. \
  --go_out=./starchart \
  --go_opt=paths=source_relative \
  --go-grpc_out=./starchart \
  --go-grpc_opt=paths=source_relative \
  starchart.proto
