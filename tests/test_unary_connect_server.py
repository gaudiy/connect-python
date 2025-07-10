# ruff: noqa: ARG001 D103 D100

import base64
import gzip
import json
import zlib

import pytest

from connect.connect import UnaryRequest, UnaryResponse
from connect.handler_context import HandlerContext
from connect.idempotency_level import IdempotencyLevel
from connect.options import HandlerOptions
from tests.conftest import AsyncClient
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse
from tests.testdata.ping.v1.v1connect.ping_connect import PingServiceHandler


@pytest.mark.asyncio()
async def test_post_application_proto() -> None:
    class PingService(PingServiceHandler):
        """Ping service implementation."""

        async def Ping(
            self, request: UnaryRequest[PingRequest], context: HandlerContext
        ) -> UnaryResponse[PingResponse]:
            """Return a ping response."""
            data = request.message

            return UnaryResponse(PingResponse(name=data.name))

    async with AsyncClient(PingService()) as client:
        content = PingRequest(name="test").SerializeToString()
        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/Ping",
            data=content,
            headers={"Content-Type": "application/proto", "Accept-Encoding": "identity"},
        )

        assert response.status_code == 200
        ping_response = PingResponse()
        ping_response.ParseFromString(response.content)
        assert ping_response.name == "test"


@pytest.mark.asyncio()
async def test_post_application_json() -> None:
    class PingService(PingServiceHandler):
        """Ping service implementation."""

        async def Ping(
            self, request: UnaryRequest[PingRequest], context: HandlerContext
        ) -> UnaryResponse[PingResponse]:
            """Return a ping response."""
            data = request.message

            return UnaryResponse(PingResponse(name=data.name))

    async with AsyncClient(PingService()) as client:
        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/Ping",
            json={"name": "test"},
            headers={"Content-Type": "application/json", "Accept-Encoding": "identity"},
        )

        assert response.status_code == 200
        assert response.json() == {"name": "test"}


@pytest.mark.asyncio()
async def test_post_gzip_compression() -> None:
    class PingService(PingServiceHandler):
        """Ping service implementation."""

        async def Ping(
            self, request: UnaryRequest[PingRequest], context: HandlerContext
        ) -> UnaryResponse[PingResponse]:
            """Return a ping response."""
            data = request.message

            return UnaryResponse(PingResponse(name=data.name))

    async with AsyncClient(PingService()) as client:
        content = PingRequest(name="test").SerializeToString()
        compressed_content = gzip.compress(content)

        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/Ping",
            data=compressed_content,
            headers={"Content-Type": "application/proto", "Content-Encoding": "gzip", "Accept-Encoding": "gzip"},
        )

        assert response.status_code == 200
        ping_response = PingResponse()
        decoded_content = gzip.decompress(response.content)
        ping_response.ParseFromString(decoded_content)
        assert ping_response.name == "test"


@pytest.mark.asyncio()
async def test_post_only_accept_encoding_gzip() -> None:
    class PingService(PingServiceHandler):
        """Ping service implementation."""

        async def Ping(
            self, request: UnaryRequest[PingRequest], context: HandlerContext
        ) -> UnaryResponse[PingResponse]:
            """Return a ping response."""
            data = request.message

            return UnaryResponse(PingResponse(name=data.name))

    async with AsyncClient(PingService()) as client:
        content = PingRequest(name="test").SerializeToString()
        response = await client.post(
            path="/tests.testdata.ping.v1.PingService/Ping",
            data=content,
            headers={"Content-Type": "application/proto", "Accept-Encoding": "gzip"},
        )

        assert response.status_code == 200
        ping_response = PingResponse()
        decoded_content = gzip.decompress(response.content)
        ping_response.ParseFromString(decoded_content)
        assert ping_response.name == "test"


@pytest.mark.asyncio()
async def test_get() -> None:
    class PingService(PingServiceHandler):
        """Ping service implementation."""

        async def Ping(
            self, request: UnaryRequest[PingRequest], context: HandlerContext
        ) -> UnaryResponse[PingResponse]:
            """Return a ping response."""
            data = request.message

            return UnaryResponse(PingResponse(name=data.name))

    async with AsyncClient(
        PingService(),
        options=HandlerOptions(idempotency_level=IdempotencyLevel.NO_SIDE_EFFECTS),
    ) as client:
        encoded_message = json.dumps({"name": "test"}).encode()
        response = await client.get(
            path="/tests.testdata.ping.v1.PingService/Ping",
            query_string={
                "encoding": "json",
                "message": encoded_message,
            },
            headers={"Accept-Encoding": "identity"},
        )

        assert response.status_code == 200
        assert response.json() == {"name": "test"}


@pytest.mark.asyncio()
async def test_get_base64() -> None:
    class PingService(PingServiceHandler):
        """Ping service implementation."""

        async def Ping(
            self, request: UnaryRequest[PingRequest], context: HandlerContext
        ) -> UnaryResponse[PingResponse]:
            """Return a ping response."""
            data = request.message

            return UnaryResponse(PingResponse(name=data.name))

    async with AsyncClient(
        PingService(),
        options=HandlerOptions(idempotency_level=IdempotencyLevel.NO_SIDE_EFFECTS),
    ) as client:
        encoded_message = base64.b64encode(json.dumps({"name": "test"}).encode()).decode()
        response = await client.get(
            path="/tests.testdata.ping.v1.PingService/Ping",
            query_string={
                "encoding": "json",
                "message": encoded_message,
                "base64": "1",
            },
            headers={"Accept-Encoding": "identity"},
        )

        assert response.status_code == 200
        assert response.json() == {"name": "test"}


@pytest.mark.asyncio()
async def test_unsupported_raw_deflate_compression() -> None:
    class PingService(PingServiceHandler):
        """Ping service implementation."""

        async def Ping(
            self, request: UnaryRequest[PingRequest], context: HandlerContext
        ) -> UnaryResponse[PingResponse]:
            """Return a ping response."""
            data = request.message

            return UnaryResponse(PingResponse(name=data.name))

    async with AsyncClient(
        PingService(),
        options=HandlerOptions(idempotency_level=IdempotencyLevel.NO_SIDE_EFFECTS),
    ) as client:
        compressor = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
        content = PingRequest(name="test").SerializeToString()
        compressed_content = compressor.compress(content) + compressor.flush()

        response = await client.post(
            "/tests.testdata.ping.v1.PingService/Ping",
            data=compressed_content,
            headers={"Content-Type": "application/proto", "Content-Encoding": "deflate"},
        )

        assert response.status_code == 501
        assert response.json().get("code") == "unimplemented"
