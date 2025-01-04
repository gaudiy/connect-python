"""Main module for the tests."""

from starlette.applications import Starlette
from starlette.middleware import Middleware

from connect.connect import ConnectRequest, ConnectResponse
from connect.idempotency_level import IdempotencyLevel
from connect.middleware import ConnectMiddleware
from connect.options import ConnectOptions
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse
from tests.testdata.ping.v1.v1connect.ping_connect import (
    PingService_service_descriptor,
    PingServiceHandler,
    create_PingService_handlers,
)


class PingService(PingServiceHandler):
    """Ping service implementation."""

    async def Ping(self, request: ConnectRequest[PingRequest]) -> ConnectResponse[PingResponse]:
        """Return a ping response."""
        data = request.message
        return ConnectResponse(PingResponse(name=data.name))


middleware = [
    Middleware(
        ConnectMiddleware,
        create_PingService_handlers(
            service=PingService(),
            options=ConnectOptions(
                descriptor=PingService_service_descriptor, idempotency_level=IdempotencyLevel.NO_SIDE_EFFECTS
            ),
        ),
    )
]

app = Starlette(middleware=middleware)
