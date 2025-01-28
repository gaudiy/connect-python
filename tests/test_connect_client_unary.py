import base64
import gzip
import json

import pytest
from starlette.requests import Request
from starlette.responses import PlainTextResponse

from connect.client import Client
from connect.code import Code
from connect.connect import ConnectRequest
from connect.error import ConnectError
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


async def ping_proto_unimplemented_error(scope: Scope, receive: Receive, send: Send) -> None:
    response = PlainTextResponse("Not Found", status_code=404)
    await response(scope, receive, send)


@pytest.mark.asyncio()
@pytest.mark.parametrize(["server"], [pytest.param(ping_proto_unimplemented_error)], indirect=["server"])
async def test_client_call_unary_unimplemented_error(server: TestServer) -> None:
    url = server.make_url(PingServiceProcedures.Ping.value + "/proto")

    client = Client(url=url, input=PingRequest, output=PingResponse)
    ping_request = ConnectRequest(message=PingRequest(name="test"))

    with pytest.raises(ConnectError) as excinfo:
        await client.call_unary(ping_request)

    assert "unimplemented" in str(excinfo.value)
    assert excinfo.value.code == Code.UNIMPLEMENTED


async def ping_proto_invalid_content_type_prefix_error(scope: Scope, receive: Receive, send: Send) -> None:
    response = PlainTextResponse("Not Found", status_code=200)
    await response(scope, receive, send)


@pytest.mark.asyncio()
@pytest.mark.parametrize(["server"], [pytest.param(ping_proto_invalid_content_type_prefix_error)], indirect=["server"])
async def test_client_call_unary_invalid_content_type_prefix_error(server: TestServer) -> None:
    url = server.make_url(PingServiceProcedures.Ping.value + "/proto")

    client = Client(url=url, input=PingRequest, output=PingResponse)
    ping_request = ConnectRequest(message=PingRequest(name="test"))

    with pytest.raises(ConnectError) as excinfo:
        await client.call_unary(ping_request)

    assert excinfo.value.code == Code.UNKNOWN


async def ping_proto_json_error_with_details(scope: Scope, receive: Receive, send: Send) -> None:
    import google.protobuf.json_format
    import google.protobuf.struct_pb2 as struct_pb2

    msg = struct_pb2.Struct(
        fields={"name": struct_pb2.Value(string_value="test"), "age": struct_pb2.Value(number_value=1)}
    )

    content = {
        "code": Code.UNAVAILABLE.string(),
        "message": "Service unavailable",
        "details": [
            {
                "type": msg.DESCRIPTOR.full_name,
                "value": base64.b64encode(msg.SerializeToString()).decode(),
                "debug": google.protobuf.json_format.MessageToDict(msg),
            }
        ],
    }

    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 503,
        "headers": [[b"content-type", b"application/json"]],
    })

    await send({"type": "http.response.body", "body": json.dumps(content).encode()})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["server"], [pytest.param(ping_proto_json_error_with_details)], indirect=["server"])
async def test_client_call_unary_json_error_with_details(server: TestServer) -> None:
    import google.protobuf.struct_pb2 as struct_pb2

    url = server.make_url(PingServiceProcedures.Ping.value + "/proto")

    client = Client(url=url, input=PingRequest, output=PingResponse)
    ping_request = ConnectRequest(message=PingRequest(name="test"))

    with pytest.raises(ConnectError) as excinfo:
        await client.call_unary(ping_request)

    assert excinfo.value.code == Code.UNAVAILABLE
    assert excinfo.value.raw_message == "Service unavailable"

    got_msg = excinfo.value.details[0].get_inner()
    assert isinstance(got_msg, struct_pb2.Struct)
    assert got_msg.fields["name"].string_value == "test"
    assert got_msg.fields["age"].number_value == 1


async def ping_proto_json_compressed_error_with_details(scope: Scope, receive: Receive, send: Send) -> None:
    import google.protobuf.json_format
    import google.protobuf.struct_pb2 as struct_pb2

    msg = struct_pb2.Struct(
        fields={"name": struct_pb2.Value(string_value="test"), "age": struct_pb2.Value(number_value=1)}
    )

    content = {
        "code": Code.UNAVAILABLE.string(),
        "message": "Service unavailable",
        "details": [
            {
                "type": msg.DESCRIPTOR.full_name,
                "value": base64.b64encode(msg.SerializeToString()).decode(),
                "debug": google.protobuf.json_format.MessageToDict(msg),
            }
        ],
    }

    compressed_content = gzip.compress(json.dumps(content).encode("utf-8"))

    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 503,
        "headers": [[b"content-type", b"application/json"], [b"content-encoding", b"gzip"]],
    })

    await send({"type": "http.response.body", "body": compressed_content})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["server"], [pytest.param(ping_proto_json_compressed_error_with_details)], indirect=["server"])
async def test_client_call_unary_json_compressed_error_with_details(server: TestServer) -> None:
    import google.protobuf.struct_pb2 as struct_pb2

    url = server.make_url(PingServiceProcedures.Ping.value + "/proto")

    client = Client(url=url, input=PingRequest, output=PingResponse)
    ping_request = ConnectRequest(message=PingRequest(name="test"))

    with pytest.raises(ConnectError) as excinfo:
        await client.call_unary(ping_request)

    assert excinfo.value.code == Code.UNAVAILABLE
    assert excinfo.value.raw_message == "Service unavailable"
    assert excinfo.value.metadata["content-type"] == "application/json"
    assert excinfo.value.metadata["content-encoding"] == "gzip"
    assert len(excinfo.value.details) == 1

    got_msg = excinfo.value.details[0].get_inner()
    assert isinstance(got_msg, struct_pb2.Struct)
    assert got_msg.fields["name"].string_value == "test"
    assert got_msg.fields["age"].number_value == 1
