"""Main module for the tests."""

from connect.app import ConnectASGI
from connect.connect import ConnectRequest, ConnectResponse
from tests.testdata.ping.v1.ping_pb2 import PingRequest, PingResponse
from tests.testdata.ping.v1.v1connect.ping_connect import (
    PingServiceHandler,
    create_PingService_handlers,
)


class PingService(PingServiceHandler):
    """Ping service implementation."""

    async def Ping(self, request: ConnectRequest[PingRequest]) -> ConnectResponse[PingResponse]:
        """Return a ping response."""
        data = request.message
        return ConnectResponse(PingResponse(name=data.name))


app = ConnectASGI(handlers=create_PingService_handlers(service=PingService()))
