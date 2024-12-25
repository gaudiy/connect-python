"""Response module for the connect package."""

from typing import Generic, TypeVar

import google.protobuf.message
from starlette.responses import Response as Response

Res = TypeVar("Res", bound=google.protobuf.message.Message)


class ConnectResponse(Generic[Res]):
    """Response class for handling responses."""

    message: Res

    def __init__(self, message: Res) -> None:
        """Initialize the response with a message."""
        self.message = message

    def encode(self, content_type: str | None) -> bytes:
        """Encode the response into a byte string."""
        if content_type == "application/json":
            raise ValueError("Unsupported content type: application/json")
        elif content_type == "application/proto":
            return self.message.SerializeToString()
        else:
            raise ValueError(f"Unsupported content type: {content_type}")
