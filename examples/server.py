"""Main module for the tests."""

import os
from typing import Any

import hypercorn.asyncio
from connect.connect import UnaryRequest, UnaryResponse
from connect.handler_context import HandlerContext
from connect.handler_interceptor import HandlerInterceptor, UnaryFunc
from connect.middleware import ConnectMiddleware
from starlette.applications import Starlette
from starlette.middleware import Middleware

from proto.connectrpc.eliza.v1.eliza_pb2 import SayRequest, SayResponse
from proto.connectrpc.eliza.v1.v1connect.eliza_connect_pb2 import ElizaServiceHandler, create_ElizaService_handlers


class ElizaService(ElizaServiceHandler):
    """Ping service implementation."""

    async def Say(self, request: UnaryRequest[SayRequest]) -> UnaryResponse[SayResponse]:
        """Return a ping response."""
        data = request.message
        return UnaryResponse(SayResponse(sentence=data.sentence))


class IPRestrictionInterceptor(HandlerInterceptor):
    """IP restriction interceptor."""

    def wrap_unary(self, next: UnaryFunc) -> UnaryFunc:
        """Wrap a unary function with the interceptor."""

        async def _wrapped(request: UnaryRequest[Any], context: HandlerContext) -> UnaryResponse[Any]:
            ip_allow_list = os.environ.get("IP_ALLOW_LIST", "").split(",")
            if not ip_allow_list:
                raise Exception("White list not found")

            address = request.peer.address
            if not address:
                raise Exception("Address not found")

            ip = address.host
            if ip == "":
                raise Exception("IP not allowed")

            if ip not in ip_allow_list:
                raise Exception("IP not allowed")

            return await next(request, context)

        return _wrapped


middleware = [
    Middleware(
        ConnectMiddleware,
        create_ElizaService_handlers(service=ElizaService()),
    )
]

app = Starlette(middleware=middleware)


if __name__ == "__main__":
    import asyncio

    import hypercorn

    config = hypercorn.Config()
    config.bind = ["localhost:8080"]
    asyncio.run(hypercorn.asyncio.serve(app, config))  # type: ignore
