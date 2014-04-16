export GOPATH := $(shell pwd)
pwd := $(shell pwd)
$(shell rm -fr pkg)
all:server client eventlog

server:
	go build -o bin/server service/server.go

client:
	go build -o bin/client service/client.go

eventlog:
	go build -o bin/eventlog service/eventlog.go
