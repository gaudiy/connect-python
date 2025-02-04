# ruff: noqa: ARG001 D103 D100


import json

import pytest

from connect.client import Client
from connect.connect import ConnectRequest
from connect.protocol_connect import Envelope, EnvelopeFlags
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
            [b"connect-content-encoding", b"identity"],
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

    env = Envelope(PingResponse(name="I'm Eliza").SerializeToString(), EnvelopeFlags(0))
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    env = Envelope(json.dumps({}).encode(), EnvelopeFlags.end_stream)
    await send({"type": "http.response.body", "body": env.encode(), "more_body": True})

    await send({"type": "http.response.body", "body": b"", "more_body": False})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(server_streaming)], indirect=["hypercorn_server"])
async def test_server_streaming(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    client = Client(url=url, input=PingRequest, output=PingResponse)
    ping_request = ConnectRequest(message=PingRequest(name="Bob"))

    response_iterator = client.call_server_stream(ping_request)
    want = ["Hi Bob.", "I'm Eliza"]
    async for response in response_iterator:
        assert response.message.name in want
        want.remove(response.message.name)
