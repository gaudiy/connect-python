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

from connectrpc.call_options import CallOptions
from connectrpc.client import Client, ClientConfig
from connectrpc.code import Code
from connectrpc.codec import Codec, ProtoBinaryCodec, ProtoJSONCodec
from connectrpc.compression import Compression, GZipCompression
from connectrpc.connect import (
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
from connectrpc.content_stream import AsyncByteStream
from connectrpc.error import ConnectError
from connectrpc.handler import Handler
from connectrpc.handler_context import HandlerContext
from connectrpc.headers import Headers
from connectrpc.idempotency_level import IdempotencyLevel
from connectrpc.middleware import ConnectMiddleware
from connectrpc.options import ClientOptions, HandlerOptions
from connectrpc.protocol import Protocol
from connectrpc.request import Request
from connectrpc.response import Response as HTTPResponse
from connectrpc.response import StreamingResponse
from connectrpc.response_writer import ServerResponseWriter
from connectrpc.version import __version__

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
