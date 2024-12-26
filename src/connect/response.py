"""Response module for the connect package."""

from typing import Generic, TypeVar

from starlette.responses import Response as Response

Res = TypeVar("Res")


class ConnectResponse(Generic[Res]):
    """Response class for handling responses."""

    message: Res

    def __init__(self, message: Res) -> None:
        """Initialize the response with a message."""
        self.message = message

    def any(self) -> Res:
        """Return the response message."""
        return self.message
