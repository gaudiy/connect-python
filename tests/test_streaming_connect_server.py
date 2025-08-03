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

import gzip
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest

from connectrpc.code import Code
from connectrpc.connect import StreamRequest, StreamResponse, StreamType
from connectrpc.envelope import Envelope, EnvelopeFlags
from connectrpc.error import ConnectError
from connectrpc.handler_context import HandlerContext
from connectrpc.handler_interceptor import HandlerInterceptor, StreamFunc
from connectrpc.headers import Headers
from connectrpc.options import HandlerOptions
from tests.conftest import AsyncClient
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse
from tests.testdata.ping.v1.v1connect.ping_connect import (
    PingServiceHandler,
)

CHUNK_SIZE = 65_536


@pytest.mark.asyncio()
async def test_server_streaming() -> None:
    class PingService(PingServiceHandler):
        async def PingServerStream(
            self, request: StreamRequest[PingRequest], context: HandlerContext
        ) -> StreamResponse[PingResponse]:
            messages = ""
            async for data in request.messages:
                messages += " " + data.name

            async def iterator() -> AsyncIterator[PingResponse]:
                for i in range(3):
                    yield PingResponse(name=f"Hello {i}!")

            return StreamResponse(iterator())

    def to_bytes() -> bytes:
        env = Envelope(PingRequest(name="test").SerializeToString(), EnvelopeFlags(0))
        return env.encode()

    async with AsyncClient(PingService()) as client:
        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/PingServerStream",
            data=to_bytes(),
            headers={
                "content-type": "application/connect+proto",
                "connect-accept-encoding": "identity",
            },
            stream=True,
        )

        want = ["Hello 0!", "Hello 1!", "Hello 2!"]
        async for message in response.iter_content(CHUNK_SIZE):
            assert isinstance(message, bytes)

            env, _ = Envelope.decode(message)
            if env:
                if env.flags == EnvelopeFlags(0):
                    ping_response = PingResponse()
                    ping_response.ParseFromString(env.data)
                    assert ping_response.name in want
                    want.remove(ping_response.name)
                elif env.flags == EnvelopeFlags.end_stream:
                    assert env.data == b"{}"
            else:
                assert message == b""

        for k, v in response.headers.items():
            if k == "content-type":
                assert v == "application/connect+proto"
            if k == "connect-accept-encoding":
                assert v == "gzip"


@pytest.mark.asyncio()
async def test_server_streaming_end_stream_error() -> None:
    class PingService(PingServiceHandler):
        async def PingServerStream(
            self, request: StreamRequest[PingRequest], context: HandlerContext
        ) -> StreamResponse[PingResponse]:
            messages = ""
            async for data in request.messages:
                messages += " " + data.name

            async def iterator() -> AsyncIterator[PingResponse]:
                for i in range(3):
                    yield PingResponse(name=f"Hello {i}!")

            raise ConnectError(
                code=Code.UNAVAILABLE,
                message="Service unavailable",
                metadata=Headers({"acme-operation-cost": "237"}),
            )

            return StreamResponse(iterator())

    def to_bytes() -> bytes:
        env = Envelope(PingRequest(name="test").SerializeToString(), EnvelopeFlags(0))
        return env.encode()

    async with AsyncClient(PingService()) as client:
        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/PingServerStream",
            data=to_bytes(),
            headers={
                "content-type": "application/connect+proto",
                "connect-accept-encoding": "identity",
            },
            stream=True,
        )

        async for message in response.iter_content(CHUNK_SIZE):
            assert isinstance(message, bytes)

            env, _ = Envelope.decode(message)
            if env:
                assert env.flags == EnvelopeFlags.end_stream

                body_str = env.data.decode()
                body = json.loads(body_str)
                assert body["error"]["code"] == Code.UNAVAILABLE.string()
                assert body["error"]["message"] == "Service unavailable"

                metadata = body["metadata"]
                value = metadata["acme-operation-cost"]
                assert isinstance(value, list)
                assert value[0] == "237"
            else:
                assert message == b""


@pytest.mark.asyncio()
async def test_server_streaming_response_envelope_message_compression() -> None:
    class PingService(PingServiceHandler):
        async def PingServerStream(
            self, request: StreamRequest[PingRequest], context: HandlerContext
        ) -> StreamResponse[PingResponse]:
            messages = ""
            async for data in request.messages:
                messages += " " + data.name

            async def iterator() -> AsyncIterator[PingResponse]:
                for i in range(3):
                    yield PingResponse(name=f"Hello {i}!")

            return StreamResponse(iterator())

    def to_bytes() -> bytes:
        env = Envelope(PingRequest(name="test").SerializeToString(), EnvelopeFlags(0))
        return env.encode()

    async with AsyncClient(PingService()) as client:
        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/PingServerStream",
            data=to_bytes(),
            headers={
                "content-type": "application/connect+proto",
                "connect-accept-encoding": "gzip",
            },
            stream=True,
        )

        want = ["Hello 0!", "Hello 1!", "Hello 2!"]
        async for message in response.iter_content(CHUNK_SIZE):
            assert isinstance(message, bytes)

            env, _ = Envelope.decode(message)

            if env:
                assert env.is_set(EnvelopeFlags.compressed)

                if not env.is_set(EnvelopeFlags.end_stream):
                    ping_response = PingResponse()
                    data = gzip.decompress(env.data)
                    ping_response.ParseFromString(data)
                    assert ping_response.name in want
                    want.remove(ping_response.name)
                else:
                    data = gzip.decompress(env.data)
                    assert data == b"{}"
            else:
                assert message == b""

        for k, v in response.headers.items():
            if k == "content-type":
                assert v == "application/connect+proto"
            if k == "connect-accept-encoding":
                assert v == "gzip"
            if k == "connect-content-encoding":
                assert v == "gzip"


@pytest.mark.asyncio()
async def test_server_streaming_request_envelope_message_compression() -> None:
    class PingService(PingServiceHandler):
        async def PingServerStream(
            self, request: StreamRequest[PingRequest], context: HandlerContext
        ) -> StreamResponse[PingResponse]:
            messages = ""
            async for data in request.messages:
                messages += " " + data.name

            async def iterator() -> AsyncIterator[PingResponse]:
                for i in range(3):
                    yield PingResponse(name=f"Hello {i}!")

            return StreamResponse(iterator())

    def to_bytes() -> bytes:
        ping_request = PingRequest(name="test").SerializeToString()
        compressed_ping_request = gzip.compress(ping_request)
        env = Envelope(compressed_ping_request, EnvelopeFlags(0) | EnvelopeFlags.compressed)
        return env.encode()

        return env.encode()

    async with AsyncClient(PingService()) as client:
        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/PingServerStream",
            data=to_bytes(),
            headers={
                "content-type": "application/connect+proto",
                "connect-accept-encoding": "gzip",
                "connect-content-encoding": "gzip",
            },
            stream=True,
        )

        want = ["Hello 0!", "Hello 1!", "Hello 2!"]
        async for message in response.iter_content(CHUNK_SIZE):
            assert isinstance(message, bytes)

            env, _ = Envelope.decode(message)

            if env:
                assert env.is_set(EnvelopeFlags.compressed)

                if not env.is_set(EnvelopeFlags.end_stream):
                    ping_response = PingResponse()
                    data = gzip.decompress(env.data)
                    ping_response.ParseFromString(data)
                    assert ping_response.name in want
                    want.remove(ping_response.name)
                else:
                    data = gzip.decompress(env.data)
                    assert data == b"{}"
            else:
                assert message == b""

        for k, v in response.headers.items():
            if k == "content-type":
                assert v == "application/connect+proto"
            if k == "connect-accept-encoding":
                assert v == "gzip"
            if k == "connect-content-encoding":
                assert v == "gzip"


@pytest.mark.asyncio()
async def test_server_streaming_invalid_request_envelope_message_compression() -> None:
    class PingService(PingServiceHandler):
        async def PingServerStream(
            self, request: StreamRequest[PingRequest], context: HandlerContext
        ) -> StreamResponse[PingResponse]:
            messages = ""
            async for data in request.messages:
                messages += " " + data.name

            async def iterator() -> AsyncIterator[PingResponse]:
                for i in range(3):
                    yield PingResponse(name=f"Hello {i}!")

            return StreamResponse(iterator())

    def to_bytes() -> bytes:
        ping_request = PingRequest(name="test").SerializeToString()
        compressed_ping_request = gzip.compress(ping_request)

        # Invalid flags
        env = Envelope(compressed_ping_request, EnvelopeFlags(0))
        return env.encode()

    async with AsyncClient(PingService()) as client:
        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/PingServerStream",
            data=to_bytes(),
            headers={
                "content-type": "application/connect+proto",
                "connect-accept-encoding": "gzip",
                "connect-content-encoding": "gzip",
            },
            stream=True,
        )

        want = ["Hello 0!", "Hello 1!", "Hello 2!"]
        async for message in response.iter_content(CHUNK_SIZE):
            assert isinstance(message, bytes)

            env, _ = Envelope.decode(message)

            if env:
                assert env.is_set(EnvelopeFlags.compressed)

                if not env.is_set(EnvelopeFlags.end_stream):
                    ping_response = PingResponse()
                    data = gzip.decompress(env.data)
                    ping_response.ParseFromString(data)
                    assert ping_response.name in want
                    want.remove(ping_response.name)
                else:
                    data = gzip.decompress(env.data)
                    body = json.loads(data)
                    assert body["error"]["code"] == Code.INVALID_ARGUMENT.string()
            else:
                assert message == b""

        for k, v in response.headers.items():
            if k == "content-type":
                assert v == "application/connect+proto"
            if k == "connect-accept-encoding":
                assert v == "gzip"
            if k == "connect-content-encoding":
                assert v == "gzip"


@pytest.mark.asyncio()
async def test_server_streaming_interceptor() -> None:
    import io
    import tempfile

    class PingService(PingServiceHandler):
        async def PingServerStream(
            self, request: StreamRequest[PingRequest], context: HandlerContext
        ) -> StreamResponse[PingResponse]:
            async def iterator() -> AsyncIterator[PingResponse]:
                for i in range(3):
                    yield PingResponse(name=f"Hello {i}!")

            return StreamResponse(iterator())

    def to_bytes() -> bytes:
        env = Envelope(PingRequest(name="test").SerializeToString(), EnvelopeFlags(0))
        return env.encode()

    ephemeral_files: list[io.BufferedRandom] = []

    class FileInterceptor1(HandlerInterceptor):
        def wrap_stream(self, next: StreamFunc) -> StreamFunc:
            async def _wrapped(request: StreamRequest[Any], context: HandlerContext) -> StreamResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                assert request.spec.stream_type == StreamType.ServerStream
                assert request.peer.protocol == "connect"

                ephemeral_files.append(fp)
                fp.write(b"interceptor: 1")

                return await next(request, context)

            return _wrapped

    class FileInterceptor2(HandlerInterceptor):
        def wrap_stream(self, next: StreamFunc) -> StreamFunc:
            async def _wrapped(request: StreamRequest[Any], context: HandlerContext) -> StreamResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                assert request.spec.stream_type == StreamType.ServerStream
                assert request.peer.protocol == "connect"

                ephemeral_files.append(fp)
                fp.write(b"interceptor: 2")

                return await next(request, context)

            return _wrapped

    async with AsyncClient(
        PingService(), HandlerOptions(interceptors=[FileInterceptor1(), FileInterceptor2()])
    ) as client:
        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/PingServerStream",
            data=to_bytes(),
            headers={
                "content-type": "application/connect+proto",
                "connect-accept-encoding": "identity",
            },
            stream=True,
        )

        # Consume the response stream to ensure interceptors are triggered
        async for _ in response.iter_content(CHUNK_SIZE):
            pass

        assert len(ephemeral_files) == 2
        for i, ephemeral_file in enumerate(reversed(ephemeral_files)):
            ephemeral_file.seek(0)
            assert ephemeral_file.read() == f"interceptor: {i + 1}".encode()

            ephemeral_file.close()


@pytest.mark.asyncio()
async def test_client_streaming() -> None:
    class PingService(PingServiceHandler):
        async def PingClientStream(
            self, request: StreamRequest[PingRequest], context: HandlerContext
        ) -> StreamResponse[PingResponse]:
            messages = ""
            async for data in request.messages:
                messages += data.name

            return StreamResponse(
                PingResponse(name=messages),
            )

    async def iter_bytes() -> AsyncIterator[bytes]:
        for i in range(3):
            ping_request = PingRequest(name=f"Hello {i}!")
            env = Envelope(ping_request.SerializeToString(), EnvelopeFlags(0))
            yield env.encode()

    async with AsyncClient(PingService()) as client:
        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/PingClientStream",
            data=iter_bytes(),
            headers={
                "content-type": "application/connect+proto",
                "connect-accept-encoding": "identity",
            },
            stream=True,
        )

        async for message in response.iter_content(CHUNK_SIZE):
            assert isinstance(message, bytes)

            env, _ = Envelope.decode(message)

            if env:
                if env.flags == EnvelopeFlags(0):
                    ping_response = PingResponse()
                    ping_response.ParseFromString(env.data)
                    assert ping_response.name == "Hello 0!Hello 1!Hello 2!"

                elif env.flags == EnvelopeFlags.end_stream:
                    assert env.data == b"{}"
            else:
                assert message == b""

        for k, v in response.headers.items():
            if k == "content-type":
                assert v == "application/connect+proto"
            if k == "connect-accept-encoding":
                assert v == "gzip"


@pytest.mark.asyncio()
async def test_client_streaming_interceptor() -> None:
    import io
    import tempfile

    class PingService(PingServiceHandler):
        async def PingClientStream(
            self, request: StreamRequest[PingRequest], context: HandlerContext
        ) -> StreamResponse[PingResponse]:
            messages = ""
            async for data in request.messages:
                messages += data.name

            return StreamResponse(
                PingResponse(name=messages),
            )

    async def iter_bytes() -> AsyncIterator[bytes]:
        for i in range(3):
            ping_request = PingRequest(name=f"Hello {i}!")
            env = Envelope(ping_request.SerializeToString(), EnvelopeFlags(0))
            yield env.encode()

    ephemeral_files: list[io.BufferedRandom] = []

    class FileInterceptor1(HandlerInterceptor):
        def wrap_stream(self, next: StreamFunc) -> StreamFunc:
            async def _wrapped(request: StreamRequest[Any], context: HandlerContext) -> StreamResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                assert request.spec.stream_type == StreamType.ClientStream
                assert request.peer.protocol == "connect"

                ephemeral_files.append(fp)
                fp.write(b"interceptor: 1")

                return await next(request, context)

            return _wrapped

    class FileInterceptor2(HandlerInterceptor):
        def wrap_stream(self, next: StreamFunc) -> StreamFunc:
            async def _wrapped(request: StreamRequest[Any], context: HandlerContext) -> StreamResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                assert request.spec.stream_type == StreamType.ClientStream
                assert request.peer.protocol == "connect"

                ephemeral_files.append(fp)
                fp.write(b"interceptor: 2")

                return await next(request, context)

            return _wrapped

    async with AsyncClient(
        PingService(), HandlerOptions(interceptors=[FileInterceptor1(), FileInterceptor2()])
    ) as client:
        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/PingClientStream",
            data=iter_bytes(),
            headers={
                "content-type": "application/connect+proto",
                "connect-accept-encoding": "identity",
            },
            stream=True,
        )

        # Consume the response stream to ensure interceptors are triggered
        async for _ in response.iter_content(CHUNK_SIZE):
            pass

        assert len(ephemeral_files) == 2
        for i, ephemeral_file in enumerate(reversed(ephemeral_files)):
            ephemeral_file.seek(0)
            assert ephemeral_file.read() == f"interceptor: {i + 1}".encode()

            ephemeral_file.close()
