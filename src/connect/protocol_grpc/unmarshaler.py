"""Module for gRPC message unmarshaling using EnvelopeReader and related utilities."""

from collections.abc import AsyncIterable, AsyncIterator
from copy import copy
from typing import Any

from connect.codec import Codec
from connect.compression import Compression
from connect.envelope import EnvelopeFlags, EnvelopeReader
from connect.error import ConnectError
from connect.headers import Headers


class GRPCUnmarshaler(EnvelopeReader):
    """GRPCUnmarshaler is a specialized EnvelopeReader for handling gRPC message unmarshaling.

    Args:
        codec (Codec | None): The codec used for decoding messages.
        read_max_bytes (int): The maximum number of bytes to read from the stream.
        stream (AsyncIterable[bytes] | None, optional): The asynchronous byte stream to read messages from.
        compression (Compression | None, optional): Compression algorithm to use for decompressing messages.

    Methods:
        async unmarshal(message: Any) -> AsyncIterator[Any]:
            Asynchronously unmarshals the given message, yielding each decoded object.
            Iterates over the results of the internal _unmarshal method, yielding only the object part of each tuple.

    """

    web: bool
    _web_trailers: Headers | None

    def __init__(
        self,
        web: bool,
        codec: Codec | None,
        read_max_bytes: int,
        stream: AsyncIterable[bytes] | None = None,
        compression: Compression | None = None,
    ) -> None:
        """Initialize the protocol gRPC handler.

        Args:
            web (bool): Indicates if the connection is for a web environment.
            codec (Codec | None): The codec to use for encoding/decoding messages. Can be None.
            read_max_bytes (int): The maximum number of bytes to read from the stream.
            stream (AsyncIterable[bytes] | None, optional): An asynchronous iterable stream of bytes. Defaults to None.
            compression (Compression | None, optional): The compression method to use. Defaults to None.

        """
        super().__init__(codec, read_max_bytes, stream, compression)
        self.web = web
        self._web_trailers = None

    async def unmarshal(self, message: Any) -> AsyncIterator[tuple[Any, bool]]:
        """Asynchronously unmarshals a given message and yields each resulting object.

        Args:
            message (Any): The message to be unmarshaled.

        Yields:
            Any: Each object obtained from unmarshaling the message.

        """
        async for obj, end in super().unmarshal(message):
            if end:
                env = self.last
                if not env:
                    raise ConnectError("protocol error: empty envelope")

                data = copy(env.data)
                env.data = b""

                if not (self.web and env.is_set(EnvelopeFlags.trailer)):
                    raise ConnectError(
                        f"protocol error: invalid envelope flags: {env.flags}",
                    )

                trailers = Headers()
                lines = data.decode("utf-8").splitlines()
                for line in lines:
                    if line == "":
                        continue

                    name, value = line.split(":", 1)
                    name = name.strip().lower()
                    value = value.strip()
                    if name in trailers:
                        trailers[name] += "," + value
                    else:
                        trailers[name] = value

                self._web_trailers = trailers

            yield obj, end

    @property
    def web_trailers(self) -> Headers | None:
        """Return the trailers received in the last envelope.

        Returns:
            Headers | None: The trailers received in the last envelope, or None if no trailers were received.

        """
        return self._web_trailers
