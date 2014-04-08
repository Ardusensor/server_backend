export GOPATH=$(shell pwd)

osp_server: *.go lint
	go fmt && go build

test:
	go test

run:
	go run main.go http_handers.go

clean:
	rm -f osp_server

lint: bin/golint
	bin/golint *.go

bin/golint:
	go get github.com/golang/lint/golint