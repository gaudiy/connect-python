import base64
import gzip

import pytest
from starlette.requests import Request

from connect.client import Client
from connect.connect import ConnectRequest
from connect.idempotency_level import IdempotencyLevel
from connect.options import ClientOptions
from tests.conftest import Receive, Scope, Send, TestServer
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse
from tests.testdata.ping.v1.v1connect.ping_connect import PingServiceProcedures


async def ping_proto(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [[b"content-type", b"application/proto"]],
    })

    response = PingResponse(name="test").SerializeToString()

    await send({"type": "http.response.body", "body": response})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["server"], [pytest.param(ping_proto)], indirect=["server"])
async def test_client_call_unary(server: TestServer) -> None:
    url = server.make_url(PingServiceProcedures.Ping.value + "/proto")

    client = Client(url=url, input=PingRequest, output=PingResponse)
    ping_request = ConnectRequest(message=PingRequest(name="test"))

    await client.call_unary(ping_request)


async def ping_response_gzip(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [[b"content-type", b"application/proto"], [b"content-encoding", b"gzip"]],
    })

    response = PingResponse(name="test").SerializeToString()
    response = gzip.compress(response)

    await send({"type": "http.response.body", "body": response})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["server"], [pytest.param(ping_response_gzip)], indirect=["server"])
async def test_client_call_unary_with_response_gzip(server: TestServer) -> None:
    url = server.make_url(PingServiceProcedures.Ping.value + "/proto")

    client = Client(url=url, input=PingRequest, output=PingResponse)
    ping_request = ConnectRequest(message=PingRequest(name="test"))

    await client.call_unary(ping_request)


async def ping_request_gzip(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [[b"content-type", b"application/proto"]],
    })

    headers = dict(scope["headers"])
    assert headers.get(b"content-encoding") == b"gzip"

    request = Request(scope, receive)
    body = await request.body()

    decompressed_body = gzip.decompress(body)
    assert decompressed_body == PingRequest(name="test").SerializeToString()

    response = PingResponse(name="test").SerializeToString()

    await send({"type": "http.response.body", "body": response})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["server"], [pytest.param(ping_request_gzip)], indirect=["server"])
async def test_client_call_unary_with_request_gzip(server: TestServer) -> None:
    url = server.make_url(PingServiceProcedures.Ping.value + "/proto")

    client = Client(
        url=url, input=PingRequest, output=PingResponse, options=ClientOptions(request_compression_name="gzip")
    )
    ping_request = ConnectRequest(message=PingRequest(name="test"))

    await client.call_unary(ping_request)


async def ping_proto_get(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [[b"content-type", b"application/proto"]],
    })

    assert scope["method"] == "GET"

    request = Request(scope, receive)

    for k, v in request.headers.items():
        assert k not in [
            "connect-protocol-version",
            "content-type",
            "content-encoding",
            "content-length",
        ]
        if k == "connect-protocol-version":
            assert v is None
        if k == "content-type":
            assert v is None
        if k == "content-encoding":
            assert v is None
        if k == "content-length":
            assert v is None

    assert request.query_params.get("encoding") == "proto"
    assert request.query_params.get("connect") == "v1"

    message_query = request.query_params.get("message")
    assert message_query

    base64_query = request.query_params.get("base64")
    if base64_query:
        assert base64.b64decode(message_query) == PingRequest(name="test").SerializeToString()
    else:
        assert message_query == PingRequest(name="test").SerializeToString().decode()

    response = PingResponse(name="test").SerializeToString()

    await send({"type": "http.response.body", "body": response})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["server"], [pytest.param(ping_proto_get)], indirect=["server"])
async def test_client_call_unary_get(server: TestServer) -> None:
    url = server.make_url(PingServiceProcedures.Ping.value + "/proto")

    client = Client(
        url=url,
        input=PingRequest,
        output=PingResponse,
        options=ClientOptions(idempotency_level=IdempotencyLevel.NO_SIDE_EFFECTS, enable_get=True),
    )
    ping_request = ConnectRequest(message=PingRequest(name="test"))

    await client.call_unary(ping_request)
