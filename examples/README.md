# examples

## Regenerate proto

```console
$ go build -o ./bin/protoc-gen-connect-python -v -x ./cmd/protoc-gen-connect-python && protoc --plugin=${PWD}/bin/protoc-gen-connect-python -I . --connect-python_out=. --connect-python_opt=paths=source_relative ./examples/proto/connectrpc/eliza/v1/eliza.proto
$ protoc --plugin=~/go/bin/protoc-gen-connect-go -I . --go_out=. --go_opt=paths=source_relative --connect-go_out=. --connect-go_opt=paths=source_relative ./examples/proto/connectrpc/eliza/v1/eliza.proto
```
