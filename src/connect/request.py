"""Request module for handling requests."""

from typing import Any, Generic, TypeVar

import google.protobuf.message
from google.protobuf import json_format
from starlette.requests import Request as Request

TProtoMessage = TypeVar("TProtoMessage", bound=google.protobuf.message.Message)


class ConnectRequest(Generic[TProtoMessage]):
    """Request class for handling requests."""

    def __init__(self, request: dict[Any, Any], **kwargs: Any) -> None:
        """Initialize the request with a body."""
        self.request = request

    def parse(self, message: TProtoMessage) -> TProtoMessage:
        """Parse the request body into a protobuf message."""
        body = self.request.get("body", b"")
        return json_format.Parse(body, message)
