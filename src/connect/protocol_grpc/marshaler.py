"""Marshaler for encoding messages into the gRPC wire format, including gRPC-Web trailer support."""

from connect.codec import Codec
from connect.compression import Compression
from connect.envelope import EnvelopeFlags, EnvelopeWriter
from connect.headers import Headers


class GRPCMarshaler(EnvelopeWriter):
    """GRPCMarshaler is responsible for marshaling messages into the gRPC wire format.

    Args:
        codec (Codec | None): The codec used for encoding/decoding messages.
        compression (Compression | None): The compression algorithm to use, if any.
        compress_min_bytes (int): Minimum message size in bytes before compression is applied.
        send_max_bytes (int): Maximum allowed size of a message to send.

    Methods:
        marshal(messages: AsyncIterable[bytes]) -> AsyncIterator[bytes]:
            Asynchronously marshals a stream of message bytes into the gRPC wire format.
            Yields marshaled message bytes ready for transmission.

    """

    def __init__(
        self,
        codec: Codec | None,
        compression: Compression | None,
        compress_min_bytes: int,
        send_max_bytes: int,
    ) -> None:
        """Initialize the protocol with the specified configuration.

        Args:
            codec (Codec | None): The codec to use for encoding/decoding messages, or None for default.
            compression (Compression | None): The compression algorithm to use, or None for no compression.
            compress_min_bytes (int): The minimum number of bytes before compression is applied.
            send_max_bytes (int): The maximum number of bytes allowed to send in a single message.

        Returns:
            None

        """
        super().__init__(codec, compression, compress_min_bytes, send_max_bytes)

    async def marshal_web_trailers(self, trailers: Headers) -> bytes:
        """Serialize HTTP trailer headers into a gRPC-Web trailer envelope.

        Args:
            trailers (Headers): A dictionary-like object containing HTTP trailer headers.

        Returns:
            bytes: The serialized gRPC-Web trailer envelope containing the trailer headers.

        """
        lines = []
        for key, value in trailers.items():
            lines.append(f"{key}: {value}\r\n")

        env = self.write_envelope("".join(lines).encode(), EnvelopeFlags.trailer)

        return env.encode()
