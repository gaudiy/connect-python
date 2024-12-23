"""Middleware for handling HTTP requests."""

import json

from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send


async def hello_asgi(_request: Request) -> Response:
    """Return a JSON response with a hello world message."""
    body = json.dumps({"message": "Hello, ASGI!"})
    response = Response(body, media_type="application/json")
    return response


class ConnectMiddleware:
    """Middleware for handling HTTP requests."""

    def __init__(self, app: ASGIApp) -> None:
        """Initialize the middleware with an ASGI application."""
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle an ASGI scope."""
        if scope["type"] == "http":
            request = Request(scope, receive)
            response = await hello_asgi(request)
            await response(scope, receive, send)
        else:
            await self.app(scope, receive, send)
