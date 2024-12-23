"""Example of a Connect server."""

from connect.middleware import ConnectMiddleware
from starlette.applications import Starlette
from starlette.middleware import Middleware

from gaudiy.ping.v1.ping_pb2 import PingRequest, PingResponse
from gaudiy.ping.v1.v1connect.ping_connect import PingServiceHandler, add_PingService_to_handler


class PingService(PingServiceHandler):
    """Ping service implementation."""

    def Ping(self, request: PingRequest) -> PingResponse:
        """Return a ping response."""
        return PingResponse(name=request.name)


routes = [Middleware(ConnectMiddleware, add_PingService_to_handler(handler=PingService()))]

app = Starlette()
