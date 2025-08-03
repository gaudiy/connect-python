# Connect Python

Connect is a simple, reliable, and interoperable RPC framework that combines the best of gRPC and REST. This Python implementation provides a clean, efficient way to build type-safe APIs with Protocol Buffers while maintaining excellent compatibility with existing HTTP infrastructure.

## Key Features

- **Multi-protocol support**: Serve Connect, gRPC and gRPC-Web clients from one endpoint
- **Type Safety**: Built on Protocol Buffers with automatic code generation
- **HTTP ecosystem friendly**: Works with standard HTTP/1.1+ and existing infrastructure
- **Streaming**: Full support for unary, client streaming, server streaming, and bidirectional streaming (half-duplex)
- **Developer-friendly**: Easy debugging with standard HTTP tools like curl
- **Standard HTTP semantics**: Meaningful status codes, cacheable GET requests for read-only RPCs

## Installation

```bash
pip install connectrpc
```

**⚠️ Dependency Notice**: For gRPC/gRPC-Web support, this package uses forked libraries:

- **httpcore**: https://github.com/tsubakiky/httpcore (auto-installed)
- **hypercorn**: https://github.com/tsubakiky/hypercorn (manual install for servers)

**Requirements by protocol**:
- **Connect protocol**: Standard PyPI versions work fine
- **gRPC/gRPC-Web**: Forked versions required (HTTP trailer support)

**For server development** install forked hypercorn:
```bash
pip install git+https://github.com/tsubakiky/hypercorn.git
```

## Quick Start

### 1. Define your service

Create a Protocol Buffer definition (`ping.proto`):

```protobuf
syntax = "proto3";

package connectrpc.ping.v1;

message PingRequest {
  string message = 1;
}

message PingResponse {
  string message = 1;
}

service PingService {
  rpc Ping(PingRequest) returns (PingResponse);
}
```

### 2. Generate Python code

Install and use the Connect Python plugin to generate client and server code:

```bash
# Install the code generator
go install github.com/gaudiy/connect-python/cmd/protoc-gen-connect-python@latest

# Generate Python code
protoc --plugin=$(go env GOPATH)/bin/protoc-gen-connect-python -I . --connect-python_out=. --connect-python_opt=paths=source_relative ping.proto
```

### 3. Implement your service

```python
from connectrpc.connect import UnaryRequest, UnaryResponse
from connectrpc.handler_context import HandlerContext
from connectrpc.middleware import ConnectMiddleware
from starlette.applications import Starlette
from starlette.middleware import Middleware

from ping_pb2 import PingRequest, PingResponse
from ping_connect_pb2 import PingServiceHandler, create_PingService_handlers

class PingService(PingServiceHandler):
    async def Ping(
        self, 
        request: UnaryRequest[PingRequest], 
        context: HandlerContext
    ) -> UnaryResponse[PingResponse]:
        return UnaryResponse(
            PingResponse(message=f"Pong: {request.message.message}")
        )

# Create the ASGI app
app = Starlette(
    middleware=[
        Middleware(
            ConnectMiddleware,
            create_PingService_handlers(PingService())
        )
    ]
)
```

### 4. Run your server

```python
import asyncio
import hypercorn  # Must be tsubakiky's fork!
import hypercorn.asyncio

if __name__ == "__main__":
    config = hypercorn.Config()
    config.bind = ["localhost:8080"]
    asyncio.run(hypercorn.asyncio.serve(app, config))
```

**⚠️ Server Note**: Use forked Hypercorn for gRPC/gRPC-Web clients. Standard Hypercorn works for Connect protocol only.

### 5. Use the client

```python
from connectrpc.client import UnaryRequest
from connectrpc.connection_pool import AsyncConnectionPool
from ping_pb2 import PingRequest
from ping_connect_pb2 import PingServiceClient

async def main():
    async with AsyncConnectionPool() as pool:
        client = PingServiceClient(
            pool=pool,
            base_url="http://localhost:8080"
        )

        request = UnaryRequest(PingRequest(message="Hello, Connect!"))
        response = await client.Ping(request)
        print(f"Response: {response.message.message}")

asyncio.run(main())
```

## Streaming Support

Connect Python supports all streaming patterns:

```python
# Server streaming
async def GetUpdates(self, request, context):
    # Get the single request message
    message = await request.single()

    async def stream_updates():
        for i in range(10):
            yield UpdateResponse(message=f"Update {i} for {message.name}")

    return StreamResponse(stream_updates())

# Client streaming
async def SendData(self, request, context):
    sentences = ""
    async for message in request.messages:
        sentences += message.data

    return StreamResponse(SummaryResponse(content=sentences))

# Bidirectional streaming (half-duplex)
async def Chat(self, request, context):
    # First, consume all client messages
    collected_messages = []
    async for message in request.messages:
        collected_messages.append(message.text)

    # Then, generate responses based on collected messages
    async def echo_messages():
        for i, msg in enumerate(collected_messages):
            yield ChatResponse(message=f"Echo {i+1}: {msg}")

    return StreamResponse(echo_messages())
```

### Streaming Limitations

> [!IMPORTANT]
> The current implementation of bidirectional streaming only supports **half-duplex** mode, where the server must fully consume the client's input stream before producing output.
> Full-duplex bidirectional streaming (where client and server can send messages simultaneously) is not yet supported but is planned for future releases.

This means:
- ✅ **Supported**: Server processes all client messages, then sends responses
- ❌ **Not yet supported**: True real-time bidirectional communication with simultaneous message exchange

For most use cases, half-duplex streaming is sufficient. If you need full-duplex streaming, consider using separate unary or streaming RPCs for now.

## Multi-Protocol Support

One server handles all client types:
- **Connect**: HTTP-friendly with standard tooling
- **gRPC**: Full compatibility with existing implementations
- **gRPC-Web**: Direct browser support, no proxy needed
- **Plain HTTP**: POST for RPCs, GET for read-only calls

## Examples

The `examples/` directory contains a complete implementation of an Eliza chatbot service demonstrating:

- Unary RPC calls
- Server streaming
- Client streaming
- Bidirectional streaming (half-duplex)
- Client and server implementations

Run the example:

```bash
cd examples

# Start the server
python server.py

# In another terminal, run the client
python client.py --unary              # Unary call
python client.py --server-streaming   # Server streaming
python client.py --client-streaming   # Client streaming
```

## Development

### Requirements

- Python 3.13+
- Protocol Buffers compiler (`protoc`)
- Go (for the code generator)
- **[uv](https://github.com/astral-sh/uv)** (recommended package manager)

### Setup

**Recommended**: Use [uv](https://github.com/astral-sh/uv) for package management. On macOS, you can install it with `brew install uv`. For other platforms, see the [uv documentation](https://docs.astral.sh/uv/).

```bash
# Clone the repository
git clone https://github.com/gaudiy/connect-python.git
cd connect-python

# Install dependencies (using uv - recommended)
uv sync

# Run tests
pytest

# Run conformance tests
cd conformance

# Server conformance tests
connectconformance -vv --trace --conf ./server_config.yaml --mode server -- uv run python server_runner.py

# Client conformance tests
connectconformance -vv --trace --conf ./client_config.yaml --mode client -- uv run python client_runner.py
```

**⚠️ Development Dependencies**:
- Forked `httpcore` auto-configured in `pyproject.toml`
- **Server development**: Install forked hypercorn manually
- **Client-only**: No additional setup needed
- **Connect only**: Standard PyPI versions work

### Code Generation

This project includes a Protocol Buffer plugin (`protoc-gen-connect-python`) written in Go that generates Python client and server code from `.proto` files.

## Contributing

We warmly welcome and greatly value contributions to the connectrpc. However, before diving in, we kindly request that you take a moment to review our Contribution Guidelines.

Additionally, please carefully read the Contributor License Agreement (CLA) before submitting your contribution to Gaudiy. By submitting your contribution, you are considered to have accepted and agreed to be bound by the terms and conditions outlined in the CLA, regardless of circumstances.

https://site.gaudiy.com/contributor-license-agreement

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
