"""Main module for the tests."""

from connect.app import ConnectASGI
from connect.request import ConnectRequest
from connect.response import ConnectResponse

from proto.connectrpc.eliza.v1.eliza_pb2 import SayRequest, SayResponse
from proto.connectrpc.eliza.v1.v1connect.eliza_connect_pb2 import ElizaServiceHandler, create_ElizaService_handler


class ElizaService(ElizaServiceHandler):
    """Ping service implementation."""

    async def Say(self, request: ConnectRequest[SayRequest]) -> ConnectResponse[SayResponse]:
        """Return a ping response."""
        data = request.message
        return ConnectResponse(SayResponse(sentence=data.sentence))


app = ConnectASGI(handlers=create_ElizaService_handler(service=ElizaService()))
