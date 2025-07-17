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

from connect.call_options import CallOptions
from connect.client import Client, ClientConfig
from connect.code import Code
from connect.codec import Codec, ProtoBinaryCodec, ProtoJSONCodec
from connect.compression import Compression, GZipCompression
from connect.connect import (
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
from connect.content_stream import AsyncByteStream
from connect.error import ConnectError
from connect.handler import Handler
from connect.handler_context import HandlerContext
from connect.headers import Headers
from connect.idempotency_level import IdempotencyLevel
from connect.middleware import ConnectMiddleware
from connect.options import ClientOptions, HandlerOptions
from connect.protocol import Protocol
from connect.request import Request
from connect.response import Response as HTTPResponse
from connect.response import StreamingResponse
from connect.response_writer import ServerResponseWriter
from connect.version import __version__

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
