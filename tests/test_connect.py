"""Test the connect module."""

from connect.testclient import TestClient
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse

from .main import app

client = TestClient(app)


def test_ping() -> None:
    """Test the ping function."""
    content = PingRequest(name="test").SerializeToString()
    response = client.post(
        "/gaudiy.ping.v1.PingService/Ping", content=content, headers={"Content-Type": "application/proto"}
    )
    assert response.status_code == 200
    ping_response = PingResponse()
    assert ping_response.ParseFromString(response.content) == len(response.content)
