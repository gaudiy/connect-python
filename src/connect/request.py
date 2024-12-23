"""Request module for handling requests."""

from typing import Generic, TypeVar

from starlette.requests import Request as Request

T = TypeVar("T")


class ConnectRequest(Generic[T]):
    """Request class for handling requests."""

    pass
