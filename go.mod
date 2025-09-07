module github.com/rstutsman/cs6450-labs

go 1.25

replace github.com/rstutsman/cs6450-labs/kvs => ./

require google.golang.org/grpc v1.75.0

require (
	golang.org/x/net v0.41.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.26.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250707201910-8d1bb00bc6a7 // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)
