"""Main module for the tests."""

from starlette.applications import Starlette
from starlette.middleware import Middleware

from connect.middleware import ConnectMiddleware
from connect.request import ConnectRequest
from connect.response import ConnectResponse
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse
from tests.testdata.ping.v1.v1connect.ping_connect import PingServiceHandler, add_PingService_to_handler


class PingService(PingServiceHandler):
    """Ping service implementation."""

    async def Ping(self, request: ConnectRequest[PingRequest]) -> ConnectResponse[PingResponse]:
        """Return a ping response."""
        data = request.message
        return ConnectResponse(PingResponse(name=data.name))


middleware = [Middleware(ConnectMiddleware, add_PingService_to_handler(handler=PingService()))]

app = Starlette(middleware=middleware)
