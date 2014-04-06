
osp_server: *.go
	go fmt && go build

test:
	go test
