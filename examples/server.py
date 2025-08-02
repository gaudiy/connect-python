# Copyright 2025 Gaudiy Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

"""Main module for the tests."""

import asyncio
from collections.abc import AsyncIterator

import hypercorn
import hypercorn.asyncio
from gconnect.connect import StreamRequest, StreamResponse, UnaryRequest, UnaryResponse
from gconnect.handler_context import HandlerContext
from gconnect.middleware import ConnectMiddleware
from starlette.applications import Starlette
from starlette.middleware import Middleware

import eliza
from proto.connectrpc.eliza.v1.eliza_pb2 import (
    IntroduceRequest,
    IntroduceResponse,
    ReflectRequest,
    ReflectResponse,
    SayRequest,
    SayResponse,
)
from proto.connectrpc.eliza.v1.v1connect.eliza_connect_pb2 import ElizaServiceHandler, create_ElizaService_handlers


class ElizaService(ElizaServiceHandler):
    """Ping service implementation."""

    async def Say(self, request: UnaryRequest[SayRequest], _context: HandlerContext) -> UnaryResponse[SayResponse]:
        """Say a message to the Eliza service."""
        reply, _ = eliza.reply(request.message.sentence)

        return UnaryResponse(SayResponse(sentence=reply))

    async def Introduce(
        self, request: StreamRequest[IntroduceRequest], _context: HandlerContext
    ) -> StreamResponse[IntroduceResponse]:
        """Introduce the Eliza service."""
        message = await request.single()
        name = message.name
        intros = eliza.get_intro_responses(name)

        async def reply_generator() -> AsyncIterator[IntroduceResponse]:
            for intro in intros:
                yield IntroduceResponse(sentence=intro)

        return StreamResponse(
            reply_generator(),
        )

    async def Reflect(
        self, request: StreamRequest[ReflectRequest], _context: HandlerContext
    ) -> StreamResponse[ReflectResponse]:
        """Reflect the message back to the user."""
        sentences = ""
        async for message in request.messages:
            sentences += message.sentence

        return StreamResponse(
            ReflectResponse(sentence=sentences),
        )


middleware = [
    Middleware(
        ConnectMiddleware,
        create_ElizaService_handlers(service=ElizaService()),
    )
]

app = Starlette(middleware=middleware)


if __name__ == "__main__":
    config = hypercorn.Config()
    config.bind = ["localhost:8080"]
    asyncio.run(hypercorn.asyncio.serve(app, config))  # type: ignore
