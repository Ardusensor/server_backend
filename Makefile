export GOPATH=$(shell pwd)

osp_server: *.go lint
	go fmt && go build

test:
	go test

run:
	go run payload.go main.go http_handers.go

clean:
	rm -f osp_server

lint: bin/golint
	bin/golint *.go

bin/golint:
	go get github.com/golang/lint/golint

linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/osp_server
