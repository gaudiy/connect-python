"""Utilities for handling Connect protocol content types."""

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
    """Extract the codec from the content type based on the stream type.

    Args:
        stream_type (StreamType): The type of stream (Unary or Streaming).
        content_type (str): The content type string from which to extract the codec.

    Returns:
        str: The extracted codec from the content type.

    """
    if stream_type == StreamType.Unary:
        return content_type[len(CONNECT_UNARY_CONTENT_TYPE_PREFIX) :]

    return content_type[len(CONNECT_STREAMING_CONTENT_TYPE_PREFIX) :]


def connect_content_type_from_codec_name(stream_type: StreamType, codec_name: str) -> str:
    """Generate the content type string for a given stream type and codec name.

    Args:
        stream_type (StreamType): The type of the stream (e.g., Unary or Streaming).
        codec_name (str): The name of the codec.

    Returns:
        str: The content type string constructed from the stream type and codec name.

    """
    if stream_type == StreamType.Unary:
        return CONNECT_UNARY_CONTENT_TYPE_PREFIX + codec_name

    return CONNECT_STREAMING_CONTENT_TYPE_PREFIX + codec_name


def connect_validate_unary_response_content_type(
    request_codec_name: str,
    status_code: int,
    response_content_type: str,
) -> ConnectError | None:
    """Validate the content type of a unary response based on the HTTP status code and method.

    Args:
        request_codec_name (str): The name of the codec used for the request.
        http_method (HTTPMethod): The HTTP method used for the request.
        status_code (int): The HTTP status code of the response.
        response_content_type (str): The content type of the response.

    Raises:
        ConnectError: If the status code is not OK and the response content type is not valid.

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
