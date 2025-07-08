# Contributing to Connect Python

Thank you for contributing to connect-py! Please read the [Contributor License Agreement (CLA)](https://site.gaudiy.com/contributor-license-agreement) before submitting any contributions.

## Development Setup

**Prerequisites:**
- Python 3.13+, Go, `protoc`, [uv](https://github.com/astral-sh/uv)

**Setup:**
```bash
git clone https://github.com/YOUR_USERNAME/connect-python.git
cd connect-py
uv sync

go build -o ./bin/protoc-gen-connect-python -v -x ./cmd/protoc-gen-connect-python
```

## Development

**Tests:**
```bash
pytest                           # Unit tests

# Conformance tests
cd conformance
connectconformance -vv --trace --conf ./server_config.yaml --mode server -- uv run python server_runner.py
connectconformance -vv --trace --conf ./client_config.yaml --mode client -- uv run python client_runner.py
```

**Code Quality:**
```bash
make lint     # Run all checks
make format   # Format and fix
```

**Code Generation:**
```bash
# Regenerate examples
go build -o ./bin/protoc-gen-connect-python -v -x ./cmd/protoc-gen-connect-python && protoc --plugin=${PWD}/bin/protoc-gen-connect-python -I . --connect-python_out=. --connect-python_opt=paths=source_relative ./examples/proto/connectrpc/eliza/v1/eliza.proto
```

## Contributions

**Bug Reports:**
- Clear description and reproduction steps
- Environment details and code examples

**Pull Requests:**
- Create issue first for major changes
- Write tests and update docs
- Keep changes focused (one feature per PR)
- Ensure CI passes
