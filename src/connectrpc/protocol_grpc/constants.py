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

"""Constants and settings for gRPC protocol support in connect-python."""

import re
import sys
from http import HTTPMethod

from connectrpc.version import __version__

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
_python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
DEFAULT_GRPC_USER_AGENT = f"connectrpc/{__version__} (Python/{_python_version})"

RE_TIMEOUT = re.compile(r"^(\d{1,8})([HMSmun])$")

UNIT_TO_SECONDS = {
    "n": 1e-9,  # nanosecond
    "u": 1e-6,  # microsecond
    "m": 1e-3,  # millisecond
    "S": 1.0,
    "M": 60.0,
    "H": 3600.0,
}

GRPC_TIMEOUT_MAX_VALUE = 10**8
GRPC_TIMEOUT_MAX_DURATION = 99_999_999
MAX_HOURS = sys.maxsize // (60 * 60 * 1_000_000_000)
