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

"""Connect-Python: A Python implementation of the Connect protocol."""

from gconnect.call_options import CallOptions
from gconnect.client import Client, ClientConfig
from gconnect.code import Code
from gconnect.codec import Codec, ProtoBinaryCodec, ProtoJSONCodec
from gconnect.compression import Compression, GZipCompression
from gconnect.connect import (
    Peer,
    Spec,
    StreamingClientConn,
    StreamingHandlerConn,
    StreamRequest,
    StreamResponse,
    StreamType,
    UnaryRequest,
    UnaryResponse,
)
from gconnect.content_stream import AsyncByteStream
from gconnect.error import ConnectError
from gconnect.handler import Handler
from gconnect.handler_context import HandlerContext
from gconnect.headers import Headers
from gconnect.idempotency_level import IdempotencyLevel
from gconnect.middleware import ConnectMiddleware
from gconnect.options import ClientOptions, HandlerOptions
from gconnect.protocol import Protocol
from gconnect.request import Request
from gconnect.response import Response as HTTPResponse
from gconnect.response import StreamingResponse
from gconnect.response_writer import ServerResponseWriter
from gconnect.version import __version__

__all__ = [
    "__version__",
    "AsyncByteStream",
    "CallOptions",
    "Client",
    "ClientConfig",
    "ClientOptions",
    "Code",
    "Codec",
    "Compression",
    "ConnectError",
    "ConnectMiddleware",
    "HandlerOptions",
    "GZipCompression",
    "Handler",
    "HandlerContext",
    "Headers",
    "HTTPResponse",
    "IdempotencyLevel",
    "Peer",
    "Protocol",
    "ProtoBinaryCodec",
    "ProtoJSONCodec",
    "Request",
    "ServerResponseWriter",
    "Spec",
    "StreamingClientConn",
    "StreamingHandlerConn",
    "StreamingResponse",
    "StreamRequest",
    "StreamResponse",
    "StreamType",
    "UnaryRequest",
    "UnaryResponse",
]
