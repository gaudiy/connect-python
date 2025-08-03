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

import base64
import gzip
import json
from typing import Any

import pytest

from connectrpc.call_options import CallOptions
from connectrpc.client import Client
from connectrpc.client_interceptor import ClientInterceptor, UnaryFunc
from connectrpc.code import Code
from connectrpc.connect import StreamType, UnaryRequest, UnaryResponse
from connectrpc.connection_pool import AsyncConnectionPool
from connectrpc.error import ConnectError
from connectrpc.idempotency_level import IdempotencyLevel
from connectrpc.options import ClientOptions
from tests.conftest import ASGIRequest, Receive, Scope, Send, ServerConfig
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse
from tests.testdata.ping.v1.v1connect.ping_connect import PingServiceProcedures


@pytest.mark.asyncio()
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(None)], indirect=["hypercorn_server"])
async def test_post_application_proto(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = UnaryRequest(content=PingRequest(name="test"))

        response = await client.call_unary(ping_request)

        assert response.message.name == "test"


async def post_response_gzip(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [[b"content-type", b"application/proto"], [b"content-encoding", b"gzip"]],
    })

    request = ASGIRequest(scope, receive)
    _ = await request.body()

    response = PingResponse(name="test").SerializeToString()
    response = gzip.compress(response)

    await send({"type": "http.response.body", "body": response})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(post_response_gzip)], indirect=["hypercorn_server"])
async def test_post_response_gzip(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = UnaryRequest(content=PingRequest(name="test"))

        await client.call_unary(ping_request)


async def post_request_gzip(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [[b"content-type", b"application/proto"]],
    })

    headers = dict(scope["headers"])
    assert headers.get(b"content-encoding") == b"gzip"

    request = ASGIRequest(scope, receive)
    body = await request.body()

    decompressed_body = gzip.decompress(body)
    assert decompressed_body == PingRequest(name="test").SerializeToString()

    response = PingResponse(name="test").SerializeToString()

    await send({"type": "http.response.body", "body": response})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(post_request_gzip)], indirect=["hypercorn_server"])
async def test_post_request_gzip(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(
            pool=pool,
            url=url,
            input=PingRequest,
            output=PingResponse,
            options=ClientOptions(request_compression_name="gzip"),
        )
        ping_request = UnaryRequest(content=PingRequest(name="test"))

        await client.call_unary(ping_request)


async def get_application_proto(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [[b"content-type", b"application/proto"]],
    })

    assert scope["method"] == "GET"

    request = ASGIRequest(scope, receive)
    _ = await request.body()

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
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(get_application_proto)], indirect=["hypercorn_server"])
async def test_get_application_proto(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(
            pool=pool,
            url=url,
            input=PingRequest,
            output=PingResponse,
            options=ClientOptions(idempotency_level=IdempotencyLevel.NO_SIDE_EFFECTS, enable_get=True),
        )
        ping_request = UnaryRequest(content=PingRequest(name="test"))

        await client.call_unary(ping_request)


async def post_not_found(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 404,
        "headers": [[b"content-type", b"text/plain"]],
    })

    request = ASGIRequest(scope, receive)
    _ = await request.body()

    await send({"type": "http.response.body", "body": b"Not Found"})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(post_not_found)], indirect=["hypercorn_server"])
async def test_post_not_found(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = UnaryRequest(content=PingRequest(name="test"))

        with pytest.raises(ConnectError) as excinfo:
            await client.call_unary(ping_request)

        assert "unimplemented" in str(excinfo.value)
        assert excinfo.value.code == Code.UNIMPLEMENTED


async def post_invalid_content_type_prefix(scope: Scope, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    await send({
        "type": "http.response.start",
        "status": 200,
        "headers": [[b"content-type", b"text/plain"]],
    })

    request = ASGIRequest(scope, receive)
    _ = await request.body()

    await send({"type": "http.response.body", "body": b"Not Found"})


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ["hypercorn_server"], [pytest.param(post_invalid_content_type_prefix)], indirect=["hypercorn_server"]
)
async def test_post_invalid_content_type_prefix(hypercorn_server: ServerConfig) -> None:
    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = UnaryRequest(content=PingRequest(name="test"))

        with pytest.raises(ConnectError) as excinfo:
            await client.call_unary(ping_request)

        assert excinfo.value.code == Code.UNKNOWN


async def post_error_details(scope: Scope, receive: Receive, send: Send) -> None:
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

    request = ASGIRequest(scope, receive)
    _ = await request.body()

    await send({"type": "http.response.body", "body": json.dumps(content).encode()})


@pytest.mark.asyncio()
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(post_error_details)], indirect=["hypercorn_server"])
async def test_post_error_details(hypercorn_server: ServerConfig) -> None:
    import google.protobuf.struct_pb2 as struct_pb2

    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = UnaryRequest(content=PingRequest(name="test"))

        with pytest.raises(ConnectError) as excinfo:
            await client.call_unary(ping_request)

        assert excinfo.value.code == Code.UNAVAILABLE
        assert excinfo.value.raw_message == "Service unavailable"

        got_msg = excinfo.value.details[0].get_inner()
        assert isinstance(got_msg, struct_pb2.Struct)
        assert got_msg.fields["name"].string_value == "test"
        assert got_msg.fields["age"].number_value == 1


async def post_compressed_error_details(scope: Scope, receive: Receive, send: Send) -> None:
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

    request = ASGIRequest(scope, receive)
    _ = await request.body()

    await send({"type": "http.response.body", "body": compressed_content})


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ["hypercorn_server"], [pytest.param(post_compressed_error_details)], indirect=["hypercorn_server"]
)
async def test_post_compressed_error_details(hypercorn_server: ServerConfig) -> None:
    import google.protobuf.struct_pb2 as struct_pb2

    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    async with AsyncConnectionPool() as pool:
        client = Client(pool=pool, url=url, input=PingRequest, output=PingResponse)
        ping_request = UnaryRequest(content=PingRequest(name="test"))

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


@pytest.mark.asyncio()
@pytest.mark.parametrize(["hypercorn_server"], [pytest.param(None)], indirect=["hypercorn_server"])
async def test_post_interceptor(hypercorn_server: ServerConfig) -> None:
    import io
    import tempfile

    url = hypercorn_server.base_url + PingServiceProcedures.Ping.value + "/proto"

    ephemeral_files: list[io.BufferedRandom] = []

    class FileInterceptor1(ClientInterceptor):
        def wrap_unary(self, next: UnaryFunc) -> UnaryFunc:
            """Wrap a unary function with the interceptor."""

            async def _wrapped(request: UnaryRequest[Any], call_options: CallOptions) -> UnaryResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                assert request.spec.stream_type == StreamType.Unary
                assert request.peer.protocol == "connect"

                ephemeral_files.append(fp)
                fp.write(b"interceptor: 1")

                return await next(request, call_options)

            return _wrapped

    class FileInterceptor2(ClientInterceptor):
        def wrap_unary(self, next: UnaryFunc) -> UnaryFunc:
            """Wrap a unary function with the interceptor."""

            async def _wrapped(request: UnaryRequest[Any], call_options: CallOptions) -> UnaryResponse[Any]:
                nonlocal ephemeral_files
                fp = tempfile.TemporaryFile()  # noqa: SIM115

                assert request.spec.stream_type == StreamType.Unary
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
        ping_request = UnaryRequest(content=PingRequest(name="test"))

        await client.call_unary(ping_request)

        assert len(ephemeral_files) == 2
        for i, ephemeral_file in enumerate(reversed(ephemeral_files)):
            ephemeral_file.seek(0)
            assert ephemeral_file.read() == f"interceptor: {i + 1}".encode()

            ephemeral_file.close()
