# Copyright 2024 Gaudiy, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Error represents an error in the Connect protocol."""

import google.protobuf.any_pb2 as any_pb2
from google.protobuf.message import Message

from connect.code import Code
from connect.headers import Headers

DEFAULT_ANY_RESOLVER_PREFIX = "type.googleapis.com/"
TYPE_URL_PREFIX = "type.googleapis.com/"


# Helper function to create error messages with code prefix
def create_message(message: str, code: Code) -> str:
    """Create an error message with a code prefix."""
    if message == "":
        return code.string()

    return f"{code.string()}: {message}"


class ErrorDetail:
    """ErrorDetail class represents the details of an error.

    Attributes:
        pb_any (any_pb2.Any): A protobuf Any type containing the error details.
        pb_inner (Message): A protobuf Message containing the inner error details.
        wire_json (str | None): A JSON string representation of the error, if available.

    """

    pb_any: any_pb2.Any
    pb_inner: Message | None = None
    wire_json: str | None = None

    def __init__(self, pb_any: any_pb2.Any, pb_inner: Message | None = None, wire_json: str | None = None) -> None:
        """Initialize an ErrorDetail."""
        self.pb_any = pb_any
        self.pb_inner = pb_inner
        self.wire_json = wire_json


def create_error_detail(msg: Message) -> ErrorDetail:
    """Create an ErrorDetail from a protobuf message."""
    if isinstance(msg, any_pb2.Any):
        return ErrorDetail(msg, msg)

    pb_any = any_pb2.Any()
    pb_any.Pack(msg)
    if pb_any.Is(msg.DESCRIPTOR):
        pb_any.Unpack(msg)

    return ErrorDetail(pb_any, msg)


class ConnectError(Exception):
    """Exception raised for errors that occur within the Connect system.

    Attributes:
        raw_message (str): The original error message.
        code (Code): The error code, default is Code.UNKNOWN.
        metadata (MutableMapping[str, str]): Additional metadata related to the error.
        details (list[ErrorDetail]): Detailed information about the error.

    """

    raw_message: str
    code: Code
    metadata: Headers
    details: list[ErrorDetail]
    wire_error: bool = False

    def __init__(
        self,
        message: str,
        code: Code = Code.UNKNOWN,
        metadata: Headers | None = None,
        details: list[ErrorDetail] | None = None,
        wire_error: bool = False,
    ):
        """Initialize a Error."""
        super().__init__(create_message(message, code))
        self.raw_message = message
        self.code = code
        self.metadata = metadata if metadata is not None else Headers()
        self.details = details if details is not None else []
        self.wire_error = wire_error
