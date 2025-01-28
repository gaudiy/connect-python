# Copyright 2024 Gaudiy, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Error represents an error in the Connect protocol."""

import google.protobuf.any_pb2 as any_pb2
import google.protobuf.symbol_database as symbol_database
from google.protobuf.message import Message

from connect.code import Code
from connect.headers import Headers

DEFAULT_ANY_RESOLVER_PREFIX = "type.googleapis.com/"


def type_url_to_message(type_url: str) -> Message:
    """Return a message instance corresponding to a given type URL."""
    if not type_url.startswith(DEFAULT_ANY_RESOLVER_PREFIX):
        raise ValueError(f"Type URL has to start with a prefix {DEFAULT_ANY_RESOLVER_PREFIX}: {type_url}")

    full_name = type_url[len(DEFAULT_ANY_RESOLVER_PREFIX) :]
    # In open-source, proto files used not to have a package specified. Because
    # the API can be used with some legacy flows and hunts as well, we need to
    # make sure that we are still able to work with the old data.
    #
    # After some grace period, this code should be removed.
    try:
        return symbol_database.Default().GetSymbol(full_name)()
    except KeyError as e:
        raise KeyError(f"Message not found for type URL: {type_url}") from e


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

    def get_inner(self) -> Message:
        """Get the inner error message."""
        if self.pb_inner:
            return self.pb_inner

        msg = type_url_to_message(self.pb_any.type_url)
        if not self.pb_any.Is(msg.DESCRIPTOR):
            raise ValueError(f"ErrorDetail type mismatch: {self.pb_any.type_url}")

        self.pb_any.Unpack(msg)
        self.pb_inner = msg

        return msg


# Helper function to create error messages with code prefix
def create_message(message: str, code: Code) -> str:
    """Create an error message with a code prefix."""
    return code.string() if message == "" else f"{code.string()}: {message}"


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
    ) -> None:
        """Initialize a Error."""
        super().__init__(create_message(message, code))
        self.raw_message = message
        self.code = code
        self.metadata = metadata if metadata is not None else Headers()
        self.details = details if details is not None else []
        self.wire_error = wire_error
