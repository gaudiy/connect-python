# Copyright 2024 Gaudiy, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Error represents an error in the Connect protocol."""

from dataclasses import dataclass
from typing import Any

import httpx._exceptions as httpx_exceptions
from google.protobuf.descriptor import Descriptor as DescMessage

from connect.code import Code


# Function to convert Code enum to string
def code_to_string(code: Code) -> str:
    """Convert a Code enum to a string."""
    return code.name.lower().replace("_", " ")


@dataclass
class OutgoingDetail:
    """OutgoingDetail represents a detail message for an outgoing error."""

    desc: DescMessage
    value: Any  # Replace with actual MessageInitShape if defined


# Helper function to create error messages with code prefix
def create_message(message: str, code: Code) -> str:
    """Create an error message with a code prefix."""
    if message:
        return f"[{code_to_string(code)}] {message}"
    return f"[{code_to_string(code)}]"


# Mapping of exception types to Code enums
EXCEPTION_CODE_MAP: dict[type[Exception], Code] = {
    # Timeout exceptions
    httpx_exceptions.ConnectTimeout: Code.DEADLINE_EXCEEDED,
    httpx_exceptions.ReadTimeout: Code.DEADLINE_EXCEEDED,
    httpx_exceptions.WriteTimeout: Code.DEADLINE_EXCEEDED,
    httpx_exceptions.PoolTimeout: Code.DEADLINE_EXCEEDED,
    # Network errors
    httpx_exceptions.ConnectError: Code.UNAVAILABLE,
    httpx_exceptions.ReadError: Code.UNAVAILABLE,
    httpx_exceptions.WriteError: Code.UNAVAILABLE,
    httpx_exceptions.CloseError: Code.INTERNAL,
    # Protocol errors
    httpx_exceptions.ProtocolError: Code.INTERNAL,
    httpx_exceptions.LocalProtocolError: Code.INTERNAL,
    httpx_exceptions.RemoteProtocolError: Code.INTERNAL,
    # Proxy and unsupported protocol
    httpx_exceptions.ProxyError: Code.UNAVAILABLE,
    httpx_exceptions.UnsupportedProtocol: Code.INVALID_ARGUMENT,
    # Decoding and redirects
    httpx_exceptions.DecodingError: Code.INVALID_ARGUMENT,
    httpx_exceptions.TooManyRedirects: Code.ABORTED,
    # URL and cookie errors
    httpx_exceptions.InvalidURL: Code.INVALID_ARGUMENT,
    httpx_exceptions.CookieConflict: Code.INVALID_ARGUMENT,
    # Stream errors
    httpx_exceptions.StreamConsumed: Code.FAILED_PRECONDITION,
    httpx_exceptions.StreamClosed: Code.FAILED_PRECONDITION,
    httpx_exceptions.ResponseNotRead: Code.FAILED_PRECONDITION,
    httpx_exceptions.RequestNotRead: Code.FAILED_PRECONDITION,
}


class Error(Exception):
    """Error represents an error in the Connect protocol."""

    def __init__(
        self,
        message: str,
        code: Code = Code.UNKNOWN,
        metadata: dict[str, str] | None = None,
        outgoing_details: list[OutgoingDetail] | None = None,
        cause: Any | None = None,
    ):
        """Initialize a Error."""
        super().__init__(create_message(message, code))
        self.raw_message = message
        self.code = code
        self.metadata = metadata if metadata is not None else {}
        self.details = outgoing_details if outgoing_details is not None else []
        self.cause = cause

    @classmethod
    def from_error(cls, error: Exception) -> "Error":
        """Convert a given exception into a Error by mapping the exception type to a corresponding Code. If the exception is already a Error, return it as is."""
        if isinstance(error, cls):
            return error

        # Iterate through the mapping and find the first matching exception type
        for exc_type, code in EXCEPTION_CODE_MAP.items():
            if isinstance(error, exc_type):
                # Special handling for HTTPStatusError to extract code from response
                if exc_type is httpx_exceptions.HTTPStatusError:
                    response = getattr(error, "response", None)
                    if response and hasattr(response, "status_code"):
                        http_to_connect_code = {
                            400: Code.INVALID_ARGUMENT,
                            401: Code.UNAUTHENTICATED,
                            403: Code.PERMISSION_DENIED,
                            404: Code.NOT_FOUND,
                            409: Code.ALREADY_EXISTS,
                            412: Code.FAILED_PRECONDITION,
                            429: Code.RESOURCE_EXHAUSTED,
                            499: Code.CANCELED,
                            500: Code.INTERNAL,
                            501: Code.UNIMPLEMENTED,
                            503: Code.UNAVAILABLE,
                            504: Code.DEADLINE_EXCEEDED,
                            # Add more mappings as needed
                        }
                        mapped_code = http_to_connect_code.get(response.status_code, Code.UNKNOWN)
                        return cls(
                            message=error.args[0] if error.args else "HTTP Status Error", code=mapped_code, cause=error
                        )

                return cls(message=error.args[0] if error.args else str(error), code=code, cause=error)

        # Default mapping for unknown exceptions
        return cls(message=str(error), code=Code.UNKNOWN, cause=error)
