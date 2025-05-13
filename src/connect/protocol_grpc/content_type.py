"""Utilities for handling gRPC and gRPC-Web content types and codec validation."""

from connect.code import Code
from connect.codec import CodecNameType
from connect.error import ConnectError
from connect.protocol_grpc.constants import (
    GRPC_CONTENT_TYPE_DEFAULT,
    GRPC_CONTENT_TYPE_PREFIX,
    GRPC_WEB_CONTENT_TYPE_DEFAULT,
    GRPC_WEB_CONTENT_TYPE_PREFIX,
)


def grpc_content_type_from_codec_name(web: bool, codec_name: str) -> str:
    """Return the appropriate gRPC content type string based on the given codec name and whether the request is for gRPC-Web.

    Args:
        web (bool): Indicates if the content type is for gRPC-Web (True) or standard gRPC (False).
        codec_name (str): The name of the codec (e.g., "proto", "json").

    Returns:
        str: The corresponding gRPC content type string.

    """
    if web:
        return GRPC_WEB_CONTENT_TYPE_PREFIX + codec_name

    if codec_name == CodecNameType.PROTO:
        return GRPC_CONTENT_TYPE_DEFAULT

    return GRPC_CONTENT_TYPE_PREFIX + codec_name


def grpc_codec_from_content_type(web: bool, content_type: str) -> str:
    """Determine the gRPC codec name from the given content type string.

    Args:
        web (bool): Indicates whether the request is a gRPC-web request.
        content_type (str): The content type string to parse.

    Returns:
        str: The codec name extracted from the content type. If the content type matches the default gRPC or gRPC-web content type,
             returns the default codec name. Otherwise, extracts and returns the codec name from the content type prefix, or returns
             the original content type if no known prefix is found.

    """
    if (not web and content_type == GRPC_CONTENT_TYPE_DEFAULT) or (
        web and content_type == GRPC_WEB_CONTENT_TYPE_DEFAULT
    ):
        return CodecNameType.PROTO

    prefix = GRPC_CONTENT_TYPE_PREFIX if not web else GRPC_WEB_CONTENT_TYPE_PREFIX

    if content_type.startswith(prefix):
        return content_type[len(prefix) :]
    else:
        return content_type


def grpc_validate_response_content_type(web: bool, request_codec_name: str, response_content_type: str) -> None:
    """Validate that the gRPC response content type matches the expected value based on the request codec and whether gRPC-Web is used.

    Args:
        web (bool): Indicates if gRPC-Web is being used.
        request_codec_name (str): The name of the codec used in the request (e.g., "proto", "json").
        response_content_type (str): The content type returned in the response.

    Raises:
        ConnectError: If the response content type does not match the expected value, with an appropriate error code.

    """
    bare, prefix = GRPC_CONTENT_TYPE_DEFAULT, GRPC_CONTENT_TYPE_PREFIX
    if web:
        bare, prefix = GRPC_WEB_CONTENT_TYPE_DEFAULT, GRPC_WEB_CONTENT_TYPE_PREFIX

    if response_content_type == prefix + request_codec_name or (
        request_codec_name == CodecNameType.PROTO and response_content_type == bare
    ):
        return

    expected_content_type = bare
    if request_codec_name != CodecNameType.PROTO:
        expected_content_type = prefix + request_codec_name

    code = Code.INTERNAL
    if response_content_type != bare and not response_content_type.startswith(prefix):
        code = Code.UNKNOWN

    raise ConnectError(f"invalid content-type {response_content_type}, expected {expected_content_type}", code)
