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
    """Returns the appropriate gRPC content type string based on the codec name and whether the request is for gRPC-Web.

    Args:
        web (bool): Indicates if the content type is for gRPC-Web (True) or standard gRPC (False).
        codec_name (str): The name of the codec (e.g., "proto", "json").

    Returns:
        str: The constructed gRPC content type string.

    Notes:
        - If `web` is True, returns the gRPC-Web content type prefix concatenated with the codec name.
        - If `codec_name` is `CodecNameType.PROTO` and `web` is False, returns the default gRPC content type.
        - Otherwise, returns the standard gRPC content type prefix concatenated with the codec name.
    """
    if web:
        return GRPC_WEB_CONTENT_TYPE_PREFIX + codec_name

    if codec_name == CodecNameType.PROTO:
        return GRPC_CONTENT_TYPE_DEFAULT

    return GRPC_CONTENT_TYPE_PREFIX + codec_name


def grpc_codec_from_content_type(web: bool, content_type: str) -> str:
    """Determines the gRPC codec name from the given content type string.

    Args:
        web (bool): Indicates whether the context is gRPC-Web (True) or standard gRPC (False).
        content_type (str): The content type string to parse.

    Returns:
        str: The codec name extracted from the content type. If the content type matches the default
             for the given context, returns the default codec name. Otherwise, returns the codec name
             parsed from the content type prefix or the original content type if no prefix is found.
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
    """Validates the gRPC response content type against the expected content type based on the request codec and context.

    Args:
        web (bool): Indicates if the request is a gRPC-web request.
        request_codec_name (str): The codec name used in the request (e.g., "proto", "json").
        response_content_type (str): The content type received in the response.

    Raises:
        ConnectError: If the response content type does not match the expected content type.
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
