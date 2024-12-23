"""Response module for the connect package."""

from typing import Generic, TypeVar

from starlette.responses import Response as Response

T = TypeVar("T")


class ConnectResponse(Generic[T]):
    """Response class for handling responses."""

    pass
