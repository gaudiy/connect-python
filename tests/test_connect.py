"""Test the connect module."""

from connect.testclient import TestClient
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse

from .main import app

client = TestClient(app)


def test_application_proto_ping() -> None:
    """Test the ping function."""
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
    """Test the ping function."""
    response = client.post(
        "/tests.testdata.ping.v1.PingService/Ping",
        json={"name": "test"},
        headers={"Content-Type": "application/json", "Accept-Encoding": "identity"},
    )
    assert response.status_code == 200
    assert response.json() == {"name": "test"}


def test_application_proto_ping_with_compression() -> None:
    """Test the ping function."""
    content = PingRequest(name="test").SerializeToString()
    response = client.post(
        "/tests.testdata.ping.v1.PingService/Ping",
        content=content,
        headers={"Content-Type": "application/proto", "Accept-Encoding": "gzip"},
    )
    assert response.status_code == 200
    ping_response = PingResponse()
    assert ping_response.ParseFromString(response.content) == len(response.content)
