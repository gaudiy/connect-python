"""Constants used in the Connect protocol implementation for Python."""

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
DEFAULT_CONNECT_USER_AGENT = f"connect-py/{__version__} (Python/{_python_version})"
