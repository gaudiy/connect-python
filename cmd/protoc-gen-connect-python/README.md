# protoc-gen-connect-python

## Regenerate

```console
$ go build -o ./bin/protoc-gen-connect-python -v -x ./cmd/protoc-gen-connect-python && protoc --plugin=${PWD}/bin/protoc-gen-connect-python -I . --connect-python_out=. ./tests/testdata/ping/v1/ping.proto
```
