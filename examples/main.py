"""Main module for the tests."""

import os

from connect.app import ConnectASGI
from connect.connect import ConnectRequest, ConnectResponse, Req, Res
from connect.interceptor import Interceptor, UnaryFunc
from connect.options import ConnectOptions

from proto.connectrpc.eliza.v1.eliza_pb2 import SayRequest, SayResponse
from proto.connectrpc.eliza.v1.v1connect.eliza_connect_pb2 import ElizaServiceHandler, create_ElizaService_handler


class ElizaService(ElizaServiceHandler):
    """Ping service implementation."""

    async def Say(self, request: ConnectRequest[SayRequest]) -> ConnectResponse[SayResponse]:
        """Return a ping response."""
        data = request.message
        return ConnectResponse(SayResponse(sentence=data.sentence))


class IPRestrictionInterceptor(Interceptor):
    """IP restriction interceptor."""

    async def wrap_unary(self, next: UnaryFunc[Req, Res]) -> UnaryFunc[Req, Res]:
        """Wrap a unary function with the interceptor."""

        async def _wrapped(request: ConnectRequest[Req]) -> ConnectResponse[Res]:
            white_list = os.environ.get("WHITE_LIST", "").split(",")
            if not white_list:
                raise Exception("White list not found")

            address = request.peer().address
            if not address:
                raise Exception("Address not found")

            ip = address.host
            if ip != "":
                raise Exception("IP not allowed")

            if ip not in white_list:
                raise Exception("IP not allowed")

            return await next(request)

        return _wrapped


app = ConnectASGI(
    handlers=create_ElizaService_handler(
        service=ElizaService(), options=ConnectOptions(interceptors=[IPRestrictionInterceptor()])
    )
)
