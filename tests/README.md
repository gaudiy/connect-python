# tests

## Regenerate

```console
$ python -m grpc_tools.protoc -I proto --python_out=tests/testdata --mypy_out=tests/testdata --grpc_python_out=tests/testdata --mypy_grpc_out=tests/testdata proto/ping/v1/ping.proto
```
