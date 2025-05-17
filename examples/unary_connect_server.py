"""Main module for the tests."""

import hypercorn.asyncio
from connect.connect import UnaryRequest, UnaryResponse
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
