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

"""Helpers for Connect protocol content type handling."""

from http import HTTPStatus

from connect.code import Code
from connect.codec import CodecNameType
from connect.connect import (
    StreamType,
)
from connect.error import ConnectError
from connect.protocol import (
    code_from_http_status,
)
from connect.protocol_connect.constants import (
    CONNECT_STREAMING_CONTENT_TYPE_PREFIX,
    CONNECT_UNARY_CONTENT_TYPE_PREFIX,
)


def connect_codec_from_content_type(stream_type: StreamType, content_type: str) -> str:
    """Extracts the codec name from a given content type string based on the stream type.

    Args:
        stream_type (StreamType): The type of the stream (e.g., Unary or Streaming).
        content_type (str): The full content type string, which includes a prefix and the codec name.

    Returns:
        str: The codec name extracted from the content type.

    Raises:
        IndexError: If the content_type string is shorter than the expected prefix length.

    Note:
        The function assumes that the content_type string starts with either
        CONNECT_UNARY_CONTENT_TYPE_PREFIX or CONNECT_STREAMING_CONTENT_TYPE_PREFIX,
        depending on the stream_type.
    """
    if stream_type == StreamType.Unary:
        return content_type[len(CONNECT_UNARY_CONTENT_TYPE_PREFIX) :]

    return content_type[len(CONNECT_STREAMING_CONTENT_TYPE_PREFIX) :]


def connect_content_type_from_codec_name(stream_type: StreamType, codec_name: str) -> str:
    """Generates a Connect protocol content type string based on the stream type and codec name.

    Args:
        stream_type (StreamType): The type of stream (e.g., Unary or Streaming).
        codec_name (str): The name of the codec (e.g., "proto", "json").

    Returns:
        str: The content type string for the Connect protocol, combining the appropriate prefix and codec name.

    Example:
        connect_content_type_from_codec_name(StreamType.Unary, "proto")
        # Returns: "application/connect+proto"
    """
    if stream_type == StreamType.Unary:
        return CONNECT_UNARY_CONTENT_TYPE_PREFIX + codec_name

    return CONNECT_STREAMING_CONTENT_TYPE_PREFIX + codec_name


def connect_validate_unary_response_content_type(
    request_codec_name: str,
    status_code: int,
    response_content_type: str,
) -> ConnectError | None:
    """Validates the content type of a unary response in the Connect protocol.

    Args:
        request_codec_name (str): The codec name used in the request (e.g., "json", "json; charset=utf-8").
        status_code (int): The HTTP status code of the response.
        response_content_type (str): The content type of the response.

    Returns:
        ConnectError | None: Returns a ConnectError if the response content type is invalid or does not match
        the expected codec, otherwise returns None.

    Raises:
        ConnectError: If the response content type is invalid or does not match the expected format.

    Behavior:
        - For non-OK HTTP status codes, ensures the response is JSON-encoded.
        - For OK responses, checks that the content type starts with the expected prefix and matches the request codec.
        - Allows for compatibility between "json" and "json; charset=utf-8" codecs.
    """
    if status_code != HTTPStatus.OK:
        # Error response must be JSON-encoded.
        if (
            response_content_type == CONNECT_UNARY_CONTENT_TYPE_PREFIX + CodecNameType.JSON
            or response_content_type == CONNECT_UNARY_CONTENT_TYPE_PREFIX + CodecNameType.JSON_CHARSET_UTF8
        ):
            return ConnectError(
                f"HTTP {status_code}",
                code_from_http_status(status_code),
            )

        raise ConnectError(
            f"HTTP {status_code}",
            code_from_http_status(status_code),
        )

    if not response_content_type.startswith(CONNECT_UNARY_CONTENT_TYPE_PREFIX):
        raise ConnectError(
            f"invalid content-type: {response_content_type}; expecting {CONNECT_UNARY_CONTENT_TYPE_PREFIX}",
            Code.UNKNOWN,
        )

    response_codec_name = connect_codec_from_content_type(StreamType.Unary, response_content_type)
    if response_codec_name == request_codec_name:
        return None

    if (response_codec_name == CodecNameType.JSON and request_codec_name == CodecNameType.JSON_CHARSET_UTF8) or (
        response_codec_name == CodecNameType.JSON_CHARSET_UTF8 and request_codec_name == CodecNameType.JSON
    ):
        return None

    raise ConnectError(
        f"invalid content-type: {response_content_type}; expecting {CONNECT_UNARY_CONTENT_TYPE_PREFIX}{request_codec_name}",
        Code.INTERNAL,
    )
