"""Handerl module."""

from collections.abc import Awaitable, Callable
from typing import Any

from connect.options import ConnectOptions
from connect.request import ConnectRequest
from connect.response import ConnectResponse

UnaryFunc = Callable[[ConnectRequest[Any]], Awaitable[ConnectResponse[Any]]]


class UnaryHander:
    """UnaryHander class."""

    def __init__(self, procedure: str, unary: UnaryFunc, options: ConnectOptions | None):
        """Initialize the unary handler."""
        self.procedure = procedure
        self.unary = unary
        self.options = options

    async def serve(self, request: dict[Any, Any], **kwargs: Any) -> ConnectResponse[Any]:
        """Serve the unary handler."""
        response = await self.unary(ConnectRequest(request, **kwargs))
        return response
