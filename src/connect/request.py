"""Request module for handling requests."""

from typing import Any, Generic, TypeVar

import google.protobuf.message
from starlette.requests import Request as Request

Req = TypeVar("Req", bound=google.protobuf.message.Message)


class ConnectRequest(Generic[Req]):
    """Request class for handling requests."""

    def __init__(self, message: Req, **kwargs: Any) -> None:  # noqa: ARG002
        """Initialize the request with a body."""
        self.message = message

    @classmethod
    def from_request(cls, message: type[Req], request: dict[Any, Any]) -> "ConnectRequest[Req]":
        """Create a request from a Starlette request."""
        content_type = request.get("headers", {}).get("content-type")
        if content_type == "application/json":
            raise ValueError("Unsupported content type: application/json")
        elif content_type == "application/proto":
            data = message()
            data.ParseFromString(request.get("body", b""))
            return cls(data)
        else:
            raise ValueError(f"Unsupported content type: {content_type}")
