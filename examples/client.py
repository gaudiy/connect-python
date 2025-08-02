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

import argparse
import asyncio
from collections.abc import AsyncGenerator

from gconnect.connect import StreamRequest, UnaryRequest
from gconnect.connection_pool import AsyncConnectionPool

from proto.connectrpc.eliza.v1.eliza_pb2 import IntroduceRequest, ReflectRequest, SayRequest
from proto.connectrpc.eliza.v1.v1connect.eliza_connect_pb2 import ElizaServiceClient


async def run_unary(client: ElizaServiceClient) -> None:
    """Run unary RPC (Say)."""
    request = UnaryRequest(SayRequest(sentence="Hi"))
    response = await client.Say(request)

    print(f"Response: {response.message.sentence}")


async def run_server_streaming(client: ElizaServiceClient) -> None:
    """Run server streaming RPC (Introduce)."""
    request = StreamRequest(IntroduceRequest(name="Alice"))

    message_count = 1
    async with client.Introduce(request) as response:
        async for message in response.messages:
            print(f"Received message {message_count}: {message.sentence}")
            message_count += 1


async def run_client_streaming(client: ElizaServiceClient) -> None:
    """Run client streaming RPC (Reflect)."""

    async def request_generator() -> AsyncGenerator[ReflectRequest]:
        for i in range(5):
            yield ReflectRequest(sentence=f"Alice is thinking... {i}")

    request = StreamRequest(request_generator())
    async with client.Reflect(request) as response:
        message = await response.single()

        print(f"Final response: {message.sentence}")


async def main() -> None:
    """Interact with the ElizaServiceClient asynchronously."""
    parser = argparse.ArgumentParser(description="Eliza client with different RPC types")
    parser.add_argument("-u", "--unary", action="store_true", help="Send unary RPC")
    parser.add_argument("-ss", "--server-streaming", action="store_true", help="Send server streaming RPC")
    parser.add_argument("-cs", "--client-streaming", action="store_true", help="Send client streaming RPC")

    args = parser.parse_args()

    if not any([args.unary, args.server_streaming, args.client_streaming]):
        print("Please specify an RPC type: -u, -ss, or -cs")
        return

    if sum([args.unary, args.server_streaming, args.client_streaming]) > 1:
        print("Please specify only one RPC type")
        return

    async with AsyncConnectionPool() as pool:
        client = ElizaServiceClient(
            pool=pool,
            base_url="http://localhost:8080/",
        )

        try:
            if args.unary:
                await run_unary(client)
            elif args.server_streaming:
                await run_server_streaming(client)
            elif args.client_streaming:
                await run_client_streaming(client)
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
