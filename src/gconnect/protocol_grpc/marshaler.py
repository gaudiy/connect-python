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

"""gRPC-Web marshaler for serializing messages and trailers with optional compression."""

from gconnect.codec import Codec
from gconnect.compression import Compression
from gconnect.envelope import EnvelopeFlags, EnvelopeWriter
from gconnect.headers import Headers


class GRPCMarshaler(EnvelopeWriter):
    """GRPCMarshaler is responsible for serializing and deserializing messages and trailers according to the gRPC-Web protocol.

    This class extends EnvelopeWriter to provide gRPC-Web specific marshaling logic, including support for optional compression, configurable minimum compression thresholds, and maximum send size enforcement. It also provides utilities for encoding HTTP trailer headers into the gRPC-Web trailer envelope format.

    Attributes:
        codec (Codec | None): The codec used for encoding and decoding messages.
        compression (Compression | None): The compression algorithm used for message payloads.
        compress_min_bytes (int): The minimum payload size (in bytes) before compression is applied.
        send_max_bytes (int): The maximum allowed size (in bytes) for a single outgoing message.

    Methods:
        marshal_web_trailers(trailers: Headers) -> bytes:
            Serializes HTTP trailer headers into a gRPC-Web trailer envelope.
    """

    def __init__(
        self,
        codec: Codec | None,
        compression: Compression | None,
        compress_min_bytes: int,
        send_max_bytes: int,
    ) -> None:
        """Initializes the object with the specified codec, compression settings, minimum bytes for compression, and maximum bytes to send.

        Args:
            codec (Codec | None): The codec to use for encoding/decoding, or None for default.
            compression (Compression | None): The compression algorithm to use, or None for no compression.
            compress_min_bytes (int): The minimum number of bytes before compression is applied.
            send_max_bytes (int): The maximum number of bytes allowed to send.

        Returns:
            None
        """
        super().__init__(codec, compression, compress_min_bytes, send_max_bytes)

    async def marshal_web_trailers(self, trailers: Headers) -> bytes:
        """Serializes HTTP trailer headers into a gRPC-Web envelope.

        Args:
            trailers (Headers): A dictionary-like object containing HTTP trailer headers.

        Returns:
            bytes: The gRPC-Web envelope containing the serialized trailer headers.

        """
        lines = []
        for key, value in trailers.items():
            lines.append(f"{key}: {value}\r\n")

        env = self.write_envelope("".join(lines).encode(), EnvelopeFlags.trailer)

        return env.encode()
