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
    async def from_request(cls, message: type[Req], request: Request) -> "ConnectRequest[Req]":
        """Create a request from a Starlette request."""
        content_type = request.headers.get("content-type")
        if content_type == "application/json":
            raise ValueError("Unsupported content type: application/json")
        elif content_type == "application/proto":
            data = message()
            body = await request.body()
            data.ParseFromString(body)
            return cls(data)
        else:
            raise ValueError(f"Unsupported content type: {content_type}")
