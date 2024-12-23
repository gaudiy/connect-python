"""Handerl module."""

from collections.abc import Callable
from typing import Any

from connect.options import ConnectOptions
from connect.request import ConnectRequest
from connect.response import ConnectResponse

UnaryFunc = Callable[[ConnectRequest[Any]], ConnectResponse[Any]]


class UnaryHander:
    """UnaryHander class."""

    def __init__(self, procedure: str, unary: UnaryFunc, options: ConnectOptions | None):
        """Initialize the unary handler."""
        pass

    def serve(self, **kwargs: Any) -> Any:
        """Serve the unary handler."""
        pass
