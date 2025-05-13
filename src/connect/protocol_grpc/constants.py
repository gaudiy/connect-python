"""Constants for gRPC protocol implementation in connect-python."""

import re
import sys
from http import HTTPMethod

from connect.version import __version__

GRPC_HEADER_COMPRESSION = "Grpc-Encoding"
GRPC_HEADER_ACCEPT_COMPRESSION = "Grpc-Accept-Encoding"
GRPC_HEADER_TIMEOUT = "Grpc-Timeout"
GRPC_HEADER_STATUS = "Grpc-Status"
GRPC_HEADER_MESSAGE = "Grpc-Message"
GRPC_HEADER_DETAILS = "Grpc-Status-Details-Bin"

GRPC_CONTENT_TYPE_DEFAULT = "application/grpc"
GRPC_WEB_CONTENT_TYPE_DEFAULT = "application/grpc-web"
GRPC_CONTENT_TYPE_PREFIX = GRPC_CONTENT_TYPE_DEFAULT + "+"
GRPC_WEB_CONTENT_TYPE_PREFIX = GRPC_WEB_CONTENT_TYPE_DEFAULT + "+"

HEADER_X_USER_AGENT = "X-User-Agent"

GRPC_ALLOWED_METHODS = [HTTPMethod.POST]

DEFAULT_GRPC_USER_AGENT = f"connect-python/{__version__} (Python/{__version__})"

RE_TIMEOUT = re.compile(r"^(\d{1,8})([HMSmun])$")

UNIT_TO_SECONDS = {
    "n": 1e-9,  # nanosecond
    "u": 1e-6,  # microsecond
    "m": 1e-3,  # millisecond
    "S": 1.0,
    "M": 60.0,
    "H": 3600.0,
}

MAX_HOURS = sys.maxsize // (60 * 60 * 1_000_000_000)
