from collections.abc import AsyncIterator

import hypercorn.asyncio
from starlette.applications import Starlette
from starlette.middleware import Middleware

from connect.connect import StreamRequest, StreamResponse, UnaryRequest, UnaryResponse
from connect.middleware import ConnectMiddleware
from examples.proto.connectrpc.eliza.v1.eliza_pb2 import IntroduceRequest, IntroduceResponse, SayRequest, SayResponse
from examples.proto.connectrpc.eliza.v1.v1connect.eliza_connect_pb2 import (
    ElizaServiceHandler,
    create_ElizaService_handlers,
)


class ElizaService(ElizaServiceHandler):
    """Ping service implementation."""

    async def Say(self, request: UnaryRequest[SayRequest]) -> UnaryResponse[SayResponse]:
        """Return a ping response."""
        data = request.message
        return UnaryResponse(SayResponse(sentence=data.sentence))

    async def IntroduceClient(self, request: StreamRequest[IntroduceRequest]) -> StreamResponse[IntroduceResponse]:
        raise NotImplementedError()
        # """Introduce the client."""

        # async def handler() -> AsyncIterator[IntroduceResponse]:
        #     async for message in request.messages:
        #         yield IntroduceResponse(sentence=f"Hello, {message.name}!")

        # return StreamResponse(handler())

    async def IntroduceServer(self, request: StreamRequest[IntroduceRequest]) -> StreamResponse[IntroduceResponse]:
        """Introduce the server."""
        messages = ""
        async for message in request.messages:
            messages += message.name

        print(f"Received messages: {messages}")

        async def handler() -> AsyncIterator[IntroduceResponse]:
            for _ in range(3):
                yield IntroduceResponse(sentence=f"Hello, {messages}!")

        return StreamResponse(handler())


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
