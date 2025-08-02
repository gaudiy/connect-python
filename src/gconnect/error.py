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

"""Defines Connect protocol errors and related utilities."""

import google.protobuf.any_pb2 as any_pb2
import google.protobuf.symbol_database as symbol_database
from google.protobuf.message import Message

from gconnect.code import Code
from gconnect.headers import Headers

DEFAULT_ANY_RESOLVER_PREFIX = "type.googleapis.com/"


def type_url_to_message(type_url: str) -> Message:
    """Converts a Protobuf `Any` type URL into an empty message instance.

    This function takes a type URL, as found in the `type_url` field of a
    `google.protobuf.any_pb2.Any` message, and returns an empty instance of the
    corresponding Protobuf message class. It uses the default symbol database
    to look up the message type.

    Args:
        type_url: The type URL string to resolve. It must start with
            'type.googleapis.com/'.

    Returns:
        An empty instance of the resolved Protobuf message.

    Raises:
        ValueError: If the `type_url` does not have the expected prefix.
        KeyError: If the message type for the given `type_url` cannot be
            found in the symbol database.
    """
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
    """Represents a detailed error message from a Connect RPC.

    Connect errors can include a list of details, which are Protobuf messages
    that provide more context about the error. These details are serialized as
    `google.protobuf.any_pb2.Any` messages. This class provides a wrapper
    around an `Any` message, allowing for lazy unpacking of the specific,
    underlying error message.

    Attributes:
        pb_any (any_pb2.Any): The raw `google.protobuf.any_pb2.Any` message.
        pb_inner (Message | None): The unpacked, specific Protobuf error message.
            This is lazily populated when `get_inner` is called for the first time.
        wire_json (str | None): The raw JSON representation of the error detail,
            as received over the wire.
    """

    pb_any: any_pb2.Any
    pb_inner: Message | None = None
    wire_json: str | None = None

    def __init__(self, pb_any: any_pb2.Any, pb_inner: Message | None = None, wire_json: str | None = None) -> None:
        """Initializes a new ConnectErrorDetail.

        Args:
            pb_any (any_pb2.Any): The Protobuf Any message containing the error detail.
            pb_inner (Message | None): The specific, deserialized Protobuf message from the detail.
            wire_json (str | None): The raw JSON representation of the detail from the wire.
        """
        self.pb_any = pb_any
        self.pb_inner = pb_inner
        self.wire_json = wire_json

    def get_inner(self) -> Message:
        """Unpacks and returns the inner protobuf message from the error detail.

        This method deserializes the `google.protobuf.Any` message contained
        within the error detail into its specific message type. The result is
        cached, so subsequent calls to this method will not re-unpack the
        message.

        Returns:
            The unpacked protobuf message.

        Raises:
            ValueError: If the type URL in the `Any` field does not match the
                type of the packed message, indicating a data corruption or
                mismatch.
        """
        if self.pb_inner:
            return self.pb_inner

        msg = type_url_to_message(self.pb_any.type_url)
        if not self.pb_any.Is(msg.DESCRIPTOR):
            raise ValueError(f"ErrorDetail type mismatch: {self.pb_any.type_url}")

        self.pb_any.Unpack(msg)
        self.pb_inner = msg

        return msg


def create_message(message: str, code: Code) -> str:
    """Creates a formatted error message from a code and a detail message.

    If the `message` is empty, this function returns the string representation
    of the `code`. Otherwise, it returns a string formatted as
    "<code>: <message>".

    Args:
        message: The specific error message.
        code: The error code enum instance.

    Returns:
        The formatted error message string.
    """
    return code.string() if message == "" else f"{code.string()}: {message}"


class ConnectError(Exception):
    """Represents an error in the Connect protocol.

    Connect errors are sent by servers when a request fails. They have a code,
    a message, and optional binary details. This exception is raised by clients
    when they receive an error from a server. It may also be raised by the
    framework to indicate a client-side problem (e.g., a network error).

    Attributes:
        raw_message (str): The original, unformatted error message from the server or client.
        code (Code): The Connect error code.
        metadata (Headers): Any metadata (headers) associated with the error.
        details (list[ErrorDetail]): A list of structured, typed error details.
        wire_error (bool): True if the error was raised due to a protocol-level issue
                           (e.g., malformed response, network error), rather than an
                           error returned by the application logic.
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
        """Initializes a new ConnectError.

        Args:
            message (str): The error message.
            code (Code): The Connect error code. Defaults to Code.UNKNOWN.
            metadata (Headers | None): Any metadata to attach to the error. Defaults to None.
            details (list[ErrorDetail] | None): A list of protobuf Any messages to attach as error details.
            Defaults to None.
            wire_error (bool): Whether this error was created from a serialized error on the wire.
            Defaults to False.
        """
        super().__init__(create_message(message, code))
        self.raw_message = message
        self.code = code
        self.metadata = metadata if metadata is not None else Headers()
        self.details = details if details is not None else []
        self.wire_error = wire_error

    def details_any(self) -> list[any_pb2.Any]:
        """Return the details as a list of Any messages."""
        return [detail.pb_any for detail in self.details]
