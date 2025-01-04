# examples

## Regenerate proto

```console
protoc --plugin=~/go/bin/protoc-gen-connect-go -I . --go_out=. --go_opt=paths=source_relative --connect-go_out=. --connect-go_opt=paths=source_relative ./examples/proto/connectrpc/eliza/v1/eliza.proto
```
