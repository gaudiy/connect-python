"""Main module for the tests."""

import asyncio
import logging

from connect.connect import ConnectRequest

from proto.connectrpc.eliza.v1.eliza_pb2 import SayRequest
from proto.connectrpc.eliza.v1.v1connect.eliza_connect_pb2 import ElizaServiceClient

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


async def main() -> None:
    client = ElizaServiceClient(
        base_url="http://localhost:8080/",
    )
    response = await client.Say(ConnectRequest(SayRequest(sentence="I feel happy.")))

    logger.debug(response.message.sentence)
    for k, v in response.headers.items():
        logger.debug(f"{k}: {v}")


if __name__ == "__main__":
    asyncio.run(main())
