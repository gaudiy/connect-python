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

"""Constants for Connect protocol headers, content types, and user agent."""

import sys

from connect.version import __version__

CONNECT_UNARY_HEADER_COMPRESSION = "Content-Encoding"
CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION = "Accept-Encoding"
CONNECT_UNARY_TRAILER_PREFIX = "Trailer-"
CONNECT_STREAMING_HEADER_COMPRESSION = "Connect-Content-Encoding"
CONNECT_STREAMING_HEADER_ACCEPT_COMPRESSION = "Connect-Accept-Encoding"
CONNECT_HEADER_TIMEOUT = "Connect-Timeout-Ms"
CONNECT_HEADER_PROTOCOL_VERSION = "Connect-Protocol-Version"
CONNECT_PROTOCOL_VERSION = "1"

CONNECT_UNARY_CONTENT_TYPE_PREFIX = "application/"
CONNECT_UNARY_CONTENT_TYPE_JSON = "application/json"
CONNECT_STREAMING_CONTENT_TYPE_PREFIX = "application/connect+"

CONNECT_UNARY_ENCODING_QUERY_PARAMETER = "encoding"
CONNECT_UNARY_MESSAGE_QUERY_PARAMETER = "message"
CONNECT_UNARY_BASE64_QUERY_PARAMETER = "base64"
CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER = "compression"
CONNECT_UNARY_CONNECT_QUERY_PARAMETER = "connect"
CONNECT_UNARY_CONNECT_QUERY_VALUE = "v" + CONNECT_PROTOCOL_VERSION

_python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
DEFAULT_CONNECT_USER_AGENT = f"connectpy/{__version__} (Python/{_python_version})"
