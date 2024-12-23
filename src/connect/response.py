"""Response module for the connect package."""

from typing import Generic, TypeVar

import google.protobuf.message
from starlette.responses import JSONResponse
from starlette.responses import Response as Response

TProtoMessage = TypeVar("TProtoMessage", bound=google.protobuf.message.Message)


class ConnectResponse(Generic[TProtoMessage]):
    """Response class for handling responses."""

    message: TProtoMessage

    def __init__(self, message: TProtoMessage) -> None:
        """Initialize the response with a message."""
        self.message = message

    def to_response(self) -> Response:
        """Convert the response to a Starlette response."""
        return JSONResponse(content={"name": "test"}, media_type="application/json")

    pass
