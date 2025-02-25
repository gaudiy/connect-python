# ruff: noqa: ARG001 D103 D100

import base64
import gzip
import json
import urllib.parse
import zlib

from connect.testclient import TestClient
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse

from .main import app


def test_post_application_proto() -> None:
    client = TestClient(app)
    content = PingRequest(name="test").SerializeToString()
    response = client.post(
        "/tests.testdata.ping.v1.PingService/Ping",
        content=content,
        headers={"Content-Type": "application/proto", "Accept-Encoding": "identity"},
    )

    assert response.status_code == 200
    ping_response = PingResponse()
    assert ping_response.ParseFromString(response.content) == len(response.content)


def test_post_application_json() -> None:
    client = TestClient(app)
    response = client.post(
        "/tests.testdata.ping.v1.PingService/Ping",
        json={"name": "test"},
        headers={"Content-Type": "application/json", "Accept-Encoding": "identity"},
    )

    assert response.status_code == 200
    assert response.json() == {"name": "test"}


def test_post_gzip_compression() -> None:
    client = TestClient(app)
    content = PingRequest(name="test").SerializeToString()
    compressed_content = gzip.compress(content)

    response = client.post(
        "/tests.testdata.ping.v1.PingService/Ping",
        content=compressed_content,
        headers={"Content-Type": "application/proto", "Content-Encoding": "gzip", "Accept-Encoding": "gzip"},
    )

    assert response.status_code == 200
    ping_response = PingResponse()
    assert ping_response.ParseFromString(response.content) == len(response.content)


def test_post_only_accept_encoding_gzip() -> None:
    client = TestClient(app)
    content = PingRequest(name="test").SerializeToString()
    response = client.post(
        "/tests.testdata.ping.v1.PingService/Ping",
        content=content,
        headers={"Content-Type": "application/proto", "Accept-Encoding": "gzip"},
    )

    assert response.status_code == 200
    ping_response = PingResponse()
    assert ping_response.ParseFromString(response.content) == len(response.content)


def test_get() -> None:
    client = TestClient(app)
    encoded_message = urllib.parse.quote(json.dumps({"name": "test"}))
    response = client.get(
        f"/tests.testdata.ping.v1.PingService/Ping?encoding=json&message={encoded_message}",
        headers={"Accept-Encoding": "identity"},
    )

    assert response.status_code == 200
    assert response.json() == {"name": "test"}


def test_get_base64() -> None:
    client = TestClient(app)
    encoded_message = base64.b64encode(json.dumps({"name": "test"}).encode()).decode()
    response = client.get(
        f"/tests.testdata.ping.v1.PingService/Ping?encoding=json&message={encoded_message}&base64=1",
        headers={"Accept-Encoding": "identity"},
    )

    assert response.status_code == 200
    assert response.json() == {"name": "test"}


def test_unsupported_raw_deflate_compression() -> None:
    client = TestClient(app)
    compressor = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
    content = PingRequest(name="test").SerializeToString()
    compressed_content = compressor.compress(content) + compressor.flush()

    response = client.post(
        "/tests.testdata.ping.v1.PingService/Ping",
        content=compressed_content,
        headers={"Content-Type": "application/proto", "Content-Encoding": "deflate"},
    )

    assert response.status_code == 501
    assert response.json().get("code") == "unimplemented"
