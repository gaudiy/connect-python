# tests

## Run test

TODO(tsubakiky): Write how to test command.

## Regenerate proto

```console
$ uv venv
$ uv sync
$ source .venv/bin/activate
$ python -m grpc_tools.protoc -I . --python_out=. --mypy_out=. --grpc_python_out=. --mypy_grpc_out=. tests/testdata/ping/v1/ping.proto
```
