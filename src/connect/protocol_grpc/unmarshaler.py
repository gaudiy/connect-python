"""GRPCUnmarshaler provides async gRPC message unmarshaling with web trailer support."""

from collections.abc import AsyncIterable, AsyncIterator
from copy import copy
from typing import Any

from connect.codec import Codec
from connect.compression import Compression
from connect.envelope import EnvelopeFlags, EnvelopeReader
from connect.error import ConnectError
from connect.headers import Headers


class GRPCUnmarshaler(EnvelopeReader):
    """GRPCUnmarshaler is a specialized EnvelopeReader for handling gRPC protocol messages.

    With support for both standard and web environments, it provides asynchronous
    unmarshaling of messages, extracting and storing HTTP/2 trailers when operating in
    web mode.

    Attributes:
        _web_trailers (Headers | None): Stores the trailers received in the last envelope, if any.

    Methods:
        __init__(web, codec, read_max_bytes, stream=None, compression=None):
            Initializes the GRPCUnmarshaler with the specified parameters.

        async unmarshal(message):
            Asynchronously unmarshals a given message, yielding each resulting object and
            handling trailers in web mode.

        web_trailers:
            Returns the trailers received in the last envelope, or None if no trailers were received.
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
        """Initializes the object with the given parameters.

        Args:
            web (bool): Indicates whether the web mode is enabled.
            codec (Codec | None): The codec to use for decoding, or None.
            read_max_bytes (int): The maximum number of bytes to read.
            stream (AsyncIterable[bytes] | None, optional): An asynchronous iterable stream of bytes. Defaults to None.
            compression (Compression | None, optional): The compression method to use, or None. Defaults to None.

        Returns:
            None
        """
        super().__init__(codec, read_max_bytes, stream, compression)
        self.web = web
        self._web_trailers = None

    async def unmarshal(self, message: Any) -> AsyncIterator[tuple[Any, bool]]:
        """Asynchronously unmarshals a message and yields objects along with an end flag.

        Iterates over the result of the superclass's `unmarshal` method, processing each object and its corresponding end flag.
        When the end flag is True, validates and parses the envelope's data as HTTP/2 trailers, storing them in the instance.
        Raises a ConnectError if the envelope is empty or has invalid flags.

        Args:
            message (Any): The message to be unmarshaled.

        Yields:
            tuple[Any, bool]: A tuple containing the unmarshaled object and a boolean indicating if it is the end of the stream.

        Raises:
            ConnectError: If the envelope is empty or has invalid flags.
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
        """Returns the HTTP trailers associated with the web response, if any.

        Returns:
            Headers | None: The HTTP trailers as a Headers object if present, otherwise None.
        """
        return self._web_trailers
