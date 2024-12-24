"""Handler module."""

from collections.abc import Awaitable, Callable
from typing import Any

from connect.options import ConnectOptions
from connect.request import ConnectRequest, Req
from connect.response import ConnectResponse, Res

UnaryFunc = Callable[[ConnectRequest[Req]], Awaitable[ConnectResponse[Res]]]


class UnaryHandler:
    """UnaryHandler class."""

    def __init__(
        self,
        procedure: str,
        unary: UnaryFunc[Req, Res],
        request_message: type[Req],
        response_message: type[Res],
        options: ConnectOptions | None,
    ):
        """Initialize the unary handler."""
        self.procedure = procedure
        self.unary = unary
        self.request_message = request_message
        self.response_message = response_message
        self.options = options

    async def serve(self, request: dict[Any, Any], **kwargs: Any) -> bytes:  # noqa: ARG002
        """Serve the unary handler."""
        response = await self.unary(ConnectRequest.from_request(self.request_message, request))
        res_bytes = response.encode(content_type=request.get("headers", {}).get("content-type", "application/json"))
        return res_bytes
