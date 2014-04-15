export GOPATH := $(shell pwd)
pwd := $(shell pwd)
$(shell rm -fr pkg)
all:
	go build -o bin/server service/server.go

client:
	go build -o bin/client service/client.go
