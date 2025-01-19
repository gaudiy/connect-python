import gzip

import pytest
from starlette.requests import Request

from connect.client import Client
from connect.connect import ConnectRequest
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
