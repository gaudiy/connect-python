# Copyright 2025 Gaudiy Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

# ruff: noqa: ARG001 D103 D100


import gzip
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest

from connectrpc.call_options import CallOptions
from connectrpc.client import Client
from connectrpc.client_interceptor import ClientInterceptor, StreamFunc
from connectrpc.code import Code
from connectrpc.connect import StreamRequest, StreamResponse, StreamType
from connectrpc.connection_pool import AsyncConnectionPool
from connectrpc.envelope import Envelope, EnvelopeFlags
from connectrpc.error import ConnectError
from connectrpc.options import ClientOptions
from tests.conftest import ASGIRequest, Receive, Scope, Send, ServerConfig
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse
from tests.testdata.ping.v1.v1connect.ping_connect import PingServiceProcedures


async def server_streaming(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            [b"content-type", b"application/connect+proto"],
            [b"connect-accept-encoding", b"identity"],
        ],
    })

    request = ASGIRequest(scope, receive)
    body = await request.body()
    env, _ = Envelope.decode(body)
    assert env is not None

    for k, v in request.headers.items():
        if k == "content-type":
            assert v == "application/connect+proto"
        if k == "connect-accept-encoding":
            assert v == "gzip"
        if k == "connect-protocol-version":
            assert v == "1"

        assert k not in ["connect-content-encoding"]

    ping_request = PingRequest()
    ping_request.ParseFromString(env.data)

    env = Envelope(PingResponse(name=f"Hi {ping_request.name}.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(PingResponse(name="I'm Eliza.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(json.dumps({}).encode(), EnvelopeFlags.end_stream)
    await send({"type": "http.response.body", "body": env.encode(), "more_body": False})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(server_streaming)], indirect=["hypercorn_server"])
async def test_server_streaming(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = StreamRequest(content=PingRequest(name="Bob"))

        async with client.call_server_stream(ping_request) as response:
            want = ["Hi Bob.", "I'm Eliza."]
            async for message in response.messages:
                assert message.name in want
                want.remove(message.name)


async def server_streaming_end_stream_error(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            [b"content-type", b"application/connect+proto"],
            [b"connect-accept-encoding", b"identity"],
        ],
    })

    request = ASGIRequest(scope, receive)
    body = await request.body()
    env, _ = Envelope.decode(body)
    assert env is not None

    ping_request = PingRequest()
    ping_request.ParseFromString(env.data)

    env = Envelope(PingResponse(name=f"Hi {ping_request.name}.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(PingResponse(name="I'm Eliza.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    # Send an error response
    env = Envelope(
        json.dumps({"error": {"code": "unavailable"}, "metadata": {"acme-operation-cost": ["237"]}}).encode(),
        EnvelopeFlags.end_stream,
    )
    await send({"type": "http.response.body", "body": env.encode(), "more_body": False})


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ["hypercorn_server"], [pytest.param(server_streaming_end_stream_error)], indirect=["hypercorn_server"]
)
async def test_server_streaming_end_stream_error(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = StreamRequest(content=PingRequest(name="Bob"))

        async with client.call_server_stream(ping_request) as response:
            want = ["Hi Bob.", "I'm Eliza."]
            with pytest.raises(ConnectError) as excinfo:
                async for message in response.messages:
                    assert message.name in want
                    want.remove(message.name)

            assert excinfo.value.code == Code.UNAVAILABLE
            assert excinfo.value.metadata["acme-operation-cost"] == "237"
            assert excinfo.value.raw_message == ""
            assert len(excinfo.value.details) == 0
            assert excinfo.value.wire_error is True


async def server_streaming_received_message_after_end_stream(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            [b"content-type", b"application/connect+proto"],
            [b"connect-accept-encoding", b"identity"],
        ],
    })

    request = ASGIRequest(scope, receive)
    body = await request.body()
    env, _ = Envelope.decode(body)
    assert env is not None

    ping_request = PingRequest()
    ping_request.ParseFromString(env.data)

    env = Envelope(PingResponse(name=f"Hi {ping_request.name}.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(PingResponse(name="I'm Eliza.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    # Send an error response
    env = Envelope(
        json.dumps({"metadata": {"acme-operation-cost": ["237"]}}).encode(),
        EnvelopeFlags.end_stream,
    )
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(PingResponse(name="Nice to meet you.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": False})


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ["hypercorn_server"],
    [pytest.param(server_streaming_received_message_after_end_stream)],
    indirect=["hypercorn_server"],
)
async def test_server_streaming_received_message_after_end_stream(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = StreamRequest(content=PingRequest(name="Bob"))

        async with client.call_server_stream(ping_request) as response:
            want = ["Hi Bob.", "I'm Eliza."]

            with pytest.raises(ConnectError) as excinfo:
                async for message in response.messages:
                    assert message.name in want
                    want.remove(message.name)

            assert excinfo.value.code == Code.INVALID_ARGUMENT
            assert excinfo.value.raw_message == "received message after end stream"


async def server_streaming_received_extra_end_stream(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            [b"content-type", b"application/connect+proto"],
            [b"connect-accept-encoding", b"identity"],
        ],
    })

    request = ASGIRequest(scope, receive)
    body = await request.body()
    env, _ = Envelope.decode(body)
    assert env is not None

    ping_request = PingRequest()
    ping_request.ParseFromString(env.data)

    env = Envelope(PingResponse(name=f"Hi {ping_request.name}.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(PingResponse(name="I'm Eliza.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    # Send an error response
    env = Envelope(
        json.dumps({"metadata": {"acme-operation-cost": ["237"]}}).encode(),
        EnvelopeFlags.end_stream,
    )
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    # Send an error response
    env = Envelope(
        json.dumps({"metadata": {"acme-operation-cost": ["474"]}}).encode(),
        EnvelopeFlags.end_stream,
    )
    await send({"type": "http.response.body", "body": env.encode(), "more_body": False})


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ["hypercorn_server"],
    [pytest.param(server_streaming_received_extra_end_stream)],
    indirect=["hypercorn_server"],
)
async def test_server_streaming_received_extra_end_stream(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = StreamRequest(content=PingRequest(name="Bob"))

        async with client.call_server_stream(ping_request) as response:
            want = ["Hi Bob.", "I'm Eliza."]

            with pytest.raises(ConnectError) as excinfo:
                async for message in response.messages:
                    assert message.name in want
                    want.remove(message.name)

            assert excinfo.value.code == Code.INVALID_ARGUMENT
            assert excinfo.value.raw_message == "received extra end stream message"


async def server_streaming_not_received_end_stream(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            [b"content-type", b"application/connect+proto"],
            [b"connect-accept-encoding", b"identity"],
        ],
    })

    request = ASGIRequest(scope, receive)
    body = await request.body()
    env, _ = Envelope.decode(body)
    assert env is not None

    ping_request = PingRequest()
    ping_request.ParseFromString(env.data)

    env = Envelope(PingResponse(name=f"Hi {ping_request.name}.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(PingResponse(name="I'm Eliza.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": False})


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ["hypercorn_server"],
    [pytest.param(server_streaming_not_received_end_stream)],
    indirect=["hypercorn_server"],
)
async def test_server_streaming_not_received_end_stream(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = StreamRequest(content=PingRequest(name="Bob"))

        async with client.call_server_stream(ping_request) as response:
            want = ["Hi Bob.", "I'm Eliza."]

            with pytest.raises(ConnectError) as excinfo:
                async for message in response.messages:
                    assert message.name in want
                    want.remove(message.name)

            assert excinfo.value.code == Code.INVALID_ARGUMENT
            assert excinfo.value.raw_message == "missing end stream message"


async def server_streaming_response_envelope_message_compression(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            [b"content-type", b"application/connect+proto"],
            [b"connect-accept-encoding", b"gzip"],
            [b"connect-content-encoding", b"gzip"],
        ],
    })

    request = ASGIRequest(scope, receive)
    body = await request.body()
    env, _ = Envelope.decode(body)
    assert env is not None

    ping_request = PingRequest()
    ping_request.ParseFromString(env.data)

    env = Envelope(
        gzip.compress(PingResponse(name=f"Hi {ping_request.name}.").SerializeToString()), EnvelopeFlags.compressed
    )
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(gzip.compress(PingResponse(name="I'm Eliza.").SerializeToString()), EnvelopeFlags.compressed)
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(json.dumps({}).encode(), EnvelopeFlags.end_stream)
    await send({"type": "http.response.body", "body": env.encode(), "more_body": False})


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ["hypercorn_server"],
    [pytest.param(server_streaming_response_envelope_message_compression)],
    indirect=["hypercorn_server"],
)
async def test_server_streaming_response_envelope_message_compression(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = StreamRequest(content=PingRequest(name="Bob"))

        async with client.call_server_stream(ping_request) as response:
            want = ["Hi Bob.", "I'm Eliza."]
            async for message in response.messages:
                assert message.name in want
                want.remove(message.name)


async def server_streaming_request_envelope_message_compression(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            [b"content-type", b"application/connect+proto"],
            [b"connect-accept-encoding", b"gzip"],
            [b"connect-content-encoding", b"gzip"],
        ],
    })

    request = ASGIRequest(scope, receive)
    body = await request.body()
    env, _ = Envelope.decode(body)
    assert env is not None
    for k, v in request.headers.items():
        if k == "content-content-encoding":
            assert v == "gzip"

    assert env.is_set(EnvelopeFlags.compressed)

    env.data = gzip.decompress(env.data)

    ping_request = PingRequest()
    ping_request.ParseFromString(env.data)

    env = Envelope(
        gzip.compress(PingResponse(name=f"Hi {ping_request.name}.").SerializeToString()), EnvelopeFlags.compressed
    )
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(gzip.compress(PingResponse(name="I'm Eliza.").SerializeToString()), EnvelopeFlags.compressed)
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(json.dumps({}).encode(), EnvelopeFlags.end_stream)
    await send({"type": "http.response.body", "body": env.encode(), "more_body": False})


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ["hypercorn_server"],
    [pytest.param(server_streaming_request_envelope_message_compression)],
    indirect=["hypercorn_server"],
)
async def test_server_streaming_request_envelope_message_compression(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(
            pool=pool,
            url=url,
            input=PingRequest,
            output=PingResponse,
            options=ClientOptions(request_compression_name="gzip"),
        )
        ping_request = StreamRequest(content=PingRequest(name="Bob"))

        async with client.call_server_stream(ping_request) as response:
            want = ["Hi Bob.", "I'm Eliza."]
            async for message in response.messages:
                assert message.name in want
                want.remove(message.name)


@pytest.mark.asyncio()
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(None)], indirect=["hypercorn_server"])
async def test_server_streaming_interceptor(hypercorn_server: ServerConfig) -> None:
    import io
    import tempfile

    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/stream"

    ephemeral_files: list[io.BufferedRandom] = []

    class FileInterceptor1(ClientInterceptor):
        def wrap_stream(self, next: StreamFunc) -> StreamFunc:
            async def _wrapped(request: StreamRequest[Any], call_options: CallOptions) -> StreamResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                assert request.spec.stream_type == StreamType.ServerStream
                assert request.peer.protocol == "connect"

                ephemeral_files.append(fp)
                fp.write(b"interceptor: 1")

                return await next(request, call_options)

            return _wrapped

    class FileInterceptor2(ClientInterceptor):
        def wrap_stream(self, next: StreamFunc) -> StreamFunc:
            async def _wrapped(request: StreamRequest[Any], call_options: CallOptions) -> StreamResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                assert request.spec.stream_type == StreamType.ServerStream
                assert request.peer.protocol == "connect"

                ephemeral_files.append(fp)
                fp.write(b"interceptor: 2")

                return await next(request, call_options)

            return _wrapped

    async with AsyncConnectionPool() as pool:
        client = Client(
            pool=pool,
            url=url,
            input=PingRequest,
            output=PingResponse,
            options=ClientOptions(interceptors=[FileInterceptor1(), FileInterceptor2()]),
        )

        ping_request = StreamRequest(content=PingRequest(name="test"))

        async with client.call_server_stream(ping_request):
            assert len(ephemeral_files) == 2
            for i, ephemeral_file in enumerate(reversed(ephemeral_files)):
                ephemeral_file.seek(0)
                assert ephemeral_file.read() == f"interceptor: {i + 1}".encode()

                ephemeral_file.close()


async def server_streaming_not_httpstatus_200(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 429,
        "headers": [
            [b"content-type", b"application/connect+proto"],
            [b"connect-accept-encoding", b"identity"],
        ],
    })

    request = ASGIRequest(scope, receive)
    _ = await request.body()

    await send({"type": "http.response.body", "body": b"", "more_body": False})


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ["hypercorn_server"], [pytest.param(server_streaming_not_httpstatus_200)], indirect=["hypercorn_server"]
)
async def test_server_streaming_not_httpstatus_200(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = StreamRequest(content=PingRequest(name="Bob"))

        with pytest.raises(ConnectError) as excinfo:
            async with client.call_server_stream(ping_request):
                assert excinfo.value.code == Code.UNAVAILABLE
                assert len(excinfo.value.details) == 0
                assert excinfo.value.wire_error is False
                assert excinfo.value.metadata == {}


async def client_streaming(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [
            [b"content-type", b"application/connect+proto"],
            [b"connect-accept-encoding", b"identity"],
        ],
    })

    request = ASGIRequest(scope, receive)
    ping_requests: list[PingRequest] = []
    async for body in request.iter_bytes():
        env, _ = Envelope.decode(body)
        if env:
            ping_request = PingRequest()
            ping_request.ParseFromString(env.data)
            ping_requests.append(ping_request)

    for k, v in request.headers.items():
        if k == "content-type":
            assert v == "application/connect+proto"
        if k == "connect-accept-encoding":
            assert v == "gzip"
        if k == "connect-protocol-version":
            assert v == "1"

        assert k not in ["connect-content-encoding"]

    assert len(ping_requests) == 3
    assert " ".join([ping_request.name for ping_request in ping_requests]) == "Hello. My name is Bob. How are you?"

    env = Envelope(PingResponse(name="I'm fine.").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(json.dumps({}).encode(), EnvelopeFlags.end_stream)
    await send({"type": "http.response.body", "body": env.encode(), "more_body": False})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(client_streaming)], indirect=["hypercorn_server"])
async def test_client_streaming(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async def iterator() -> AsyncIterator[PingRequest]:
        messages = ["Hello.", "My name is Bob.", "How are you?"]
        for message in messages:
            yield PingRequest(name=message)

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = StreamRequest(content=iterator())

        async with client.call_client_stream(ping_request) as response:
            want = ["I'm fine."]
            async for message in response.messages:
                assert message.name in want
                want.remove(message.name)


@pytest.mark.asyncio()
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(None)], indirect=["hypercorn_server"])
async def test_client_streaming_interceptor(hypercorn_server: ServerConfig) -> None:
    import io
    import tempfile

    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/stream"

    ephemeral_files: list[io.BufferedRandom] = []

    class FileInterceptor1(ClientInterceptor):
        def wrap_stream(self, next: StreamFunc) -> StreamFunc:
            async def _wrapped(request: StreamRequest[Any], call_options: CallOptions) -> StreamResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                assert request.spec.stream_type == StreamType.ClientStream
                assert request.peer.protocol == "connect"

                ephemeral_files.append(fp)
                fp.write(b"interceptor: 1")

                return await next(request, call_options)

            return _wrapped

    class FileInterceptor2(ClientInterceptor):
        def wrap_stream(self, next: StreamFunc) -> StreamFunc:
            async def _wrapped(request: StreamRequest[Any], call_options: CallOptions) -> StreamResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                assert request.spec.stream_type == StreamType.ClientStream
                assert request.peer.protocol == "connect"

                ephemeral_files.append(fp)
                fp.write(b"interceptor: 2")

                return await next(request, call_options)

            return _wrapped

    async def iterator() -> AsyncIterator[PingRequest]:
        yield PingRequest(name="test")

    async with AsyncConnectionPool() as pool:
        client = Client(
            pool=pool,
            url=url,
            input=PingRequest,
            output=PingResponse,
            options=ClientOptions(interceptors=[FileInterceptor1(), FileInterceptor2()]),
        )

        ping_request = StreamRequest(content=iterator())

        async with client.call_client_stream(ping_request):
            assert len(ephemeral_files) == 2
            for i, ephemeral_file in enumerate(reversed(ephemeral_files)):
                ephemeral_file.seek(0)
                assert ephemeral_file.read() == f"interceptor: {i + 1}".encode()

                ephemeral_file.close()
