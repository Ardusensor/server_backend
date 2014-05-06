export GOPATH=$(shell pwd)

backend: *.go lint errcheck
	go fmt && go build -o bin/backend

test:
	go test

run:
	go run payload.go main.go http_handers.go storage.go

clean:
	rm -f bin/backend

lint: bin/golint
	bin/golint *.go

bin/golint:
	go get github.com/golang/lint/golint

linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/backend

bin/errcheck:
	go get github.com/kisielk/errcheck

errcheck: bin/errcheck
	bin/errcheck -ignore 'Close|[wW]rite.*|Flush|Seek|[rR]ead.*|Notify|Rollback'
