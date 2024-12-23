"""Test the connect module."""

from connect.testclient import TestClient

from .main import app

client = TestClient(app)


def test_ping() -> None:
    """Test the ping function."""
    response = client.post("/tests.testdata.ping.v1.PingService/Ping", json={"name": "test"})
    assert response.status_code == 200
    assert response.json() == {"name": "test"}
