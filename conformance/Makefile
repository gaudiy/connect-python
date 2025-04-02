.PHONY: all
all: proto

../bin/protoc-gen-connect-python:
	@go build -o ../bin/protoc-gen-connect-python -v ../cmd/protoc-gen-connect-python

.PHONY: proto
proto: ../bin/protoc-gen-connect-python
	@buf generate --debug -v buf.build/connectrpc/conformance:v1.0.4

.PHONY: clean
clean:
	@rm -rf gen ../bin/protoc-gen-connect-python
