"""Request module for handling requests."""

from typing import Generic, TypeVar

from starlette.requests import Request as Request

Req = TypeVar("Req")


class ConnectRequest(Generic[Req]):
    """Request class for handling requests."""

    message: Req

    def __init__(self, message: Req) -> None:
        """Initialize the request with a body."""
        self.message = message

    def any(self) -> Req:
        """Return the request message."""
        return self.message
