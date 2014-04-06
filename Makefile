
osp_server: *.go
	go fmt && go build

test:
	go test

run:
	go run main.go http_handers.go