export GOPATH := $(shell pwd)
pwd := $(shell pwd)
$(shell rm -fr pkg)
all:server client

filemark:
	go build -o bin/filemark service/filemark.go

tsdbimport:
	go build -o bin/tsdbimport service/tsdbimport.go

sumfile:
	go build -o bin/sumfile service/sumfile.go

tsdbsum:
	go build -o bin/tsdbsum service/tsdbsum.go

server:
	go build -o bin/server service/server.go

client:
	go build -o bin/client service/client.go

eventlog:
	go build -o bin/eventlog service/eventlog.go

confmaker:
	go build -o bin/confmaker service/confmaker.go
