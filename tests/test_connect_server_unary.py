# ruff: noqa: ARG001 D103 D100

import base64
import json
import urllib.parse

from connect.testclient import TestClient
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse

from .main import app

client = TestClient(app)


def test_application_proto_ping() -> None:
    content = PingRequest(name="test").SerializeToString()
    response = client.post(
        "/tests.testdata.ping.v1.PingService/Ping",
        content=content,
        headers={"Content-Type": "application/proto", "Accept-Encoding": "identity"},
    )
    assert response.status_code == 200
    ping_response = PingResponse()
    assert ping_response.ParseFromString(response.content) == len(response.content)


def test_application_json_ping() -> None:
    response = client.post(
        "/tests.testdata.ping.v1.PingService/Ping",
        json={"name": "test"},
        headers={"Content-Type": "application/json", "Accept-Encoding": "identity"},
    )
    assert response.status_code == 200
    assert response.json() == {"name": "test"}


def test_application_proto_ping_with_compression() -> None:
    content = PingRequest(name="test").SerializeToString()
    response = client.post(
        "/tests.testdata.ping.v1.PingService/Ping",
        content=content,
        headers={"Content-Type": "application/proto", "Accept-Encoding": "gzip"},
    )
    assert response.status_code == 200
    ping_response = PingResponse()
    assert ping_response.ParseFromString(response.content) == len(response.content)


def test_get_application_json_ping() -> None:
    encoded_message = urllib.parse.quote(json.dumps({"name": "test"}))
    response = client.get(
        f"/tests.testdata.ping.v1.PingService/Ping?encoding=json&message={encoded_message}",
        headers={"Accept-Encoding": "identity"},
    )
    assert response.status_code == 200
    assert response.json() == {"name": "test"}


def test_get_application_json_ping_with_base64() -> None:
    encoded_message = base64.b64encode(json.dumps({"name": "test"}).encode()).decode()
    response = client.get(
        f"/tests.testdata.ping.v1.PingService/Ping?encoding=json&message={encoded_message}&base64=1",
        headers={"Accept-Encoding": "identity"},
    )
    assert response.status_code == 200
    assert response.json() == {"name": "test"}
