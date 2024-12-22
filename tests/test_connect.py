"""Test the connect module."""

from connect.testclient import TestClient

from .main import app

client = TestClient(app)


def test_get_root() -> None:
    """Test the get_root function."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.text == '{"message": "Hello, ASGI!"}'
