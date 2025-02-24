import gzip
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest

from connect.code import Code
from connect.connect import StreamRequest, StreamResponse
from connect.envelope import Envelope, EnvelopeFlags
from connect.error import ConnectError
from connect.headers import Headers
from connect.interceptor import Interceptor, StreamFunc
from connect.options import ConnectOptions
from tests.conftest import AsyncClient
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse
from tests.testdata.ping.v1.v1connect.ping_connect import (
    PingServiceHandler,
)

CHUNK_SIZE = 65_536


@pytest.mark.asyncio()
async def test_server_streaming() -> None:
    class PingService(PingServiceHandler):
        async def PingServerStream(self, request: StreamRequest[PingRequest]) -> StreamResponse[PingResponse]:
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
        async def PingServerStream(self, request: StreamRequest[PingRequest]) -> StreamResponse[PingResponse]:
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
        async def PingServerStream(self, request: StreamRequest[PingRequest]) -> StreamResponse[PingResponse]:
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
        async def PingServerStream(self, request: StreamRequest[PingRequest]) -> StreamResponse[PingResponse]:
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
        async def PingServerStream(self, request: StreamRequest[PingRequest]) -> StreamResponse[PingResponse]:
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
        async def PingServerStream(self, request: StreamRequest[PingRequest]) -> StreamResponse[PingResponse]:
            async def iterator() -> AsyncIterator[PingResponse]:
                for i in range(3):
                    yield PingResponse(name=f"Hello {i}!")

            return StreamResponse(iterator())

    def to_bytes() -> bytes:
        env = Envelope(PingRequest(name="test").SerializeToString(), EnvelopeFlags(0))
        return env.encode()

    ephemeral_files: list[io.BufferedRandom] = []

    class FileInterceptor1(Interceptor):
        def wrap_stream(self, next: StreamFunc) -> StreamFunc:
            async def _wrapped(request: StreamRequest[Any]) -> StreamResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                ephemeral_files.append(fp)
                fp.write(b"interceptor: 1")

                return await next(request)

            return _wrapped

    class FileInterceptor2(Interceptor):
        def wrap_stream(self, next: StreamFunc) -> StreamFunc:
            async def _wrapped(request: StreamRequest[Any]) -> StreamResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                ephemeral_files.append(fp)
                fp.write(b"interceptor: 2")

                return await next(request)

            return _wrapped

    async with AsyncClient(
        PingService(), ConnectOptions(interceptors=[FileInterceptor1(), FileInterceptor2()])
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
