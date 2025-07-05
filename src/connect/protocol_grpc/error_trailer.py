"""Provides functions to convert between ConnectError and gRPC trailer headers."""

import base64
from urllib.parse import quote, unquote

from google.protobuf.message import DecodeError
from google.rpc import status_pb2

from connect.code import Code
from connect.error import ConnectError, ErrorDetail
from connect.headers import Headers
from connect.protocol import exclude_protocol_headers
from connect.protocol_grpc.constants import (
    GRPC_HEADER_DETAILS,
    GRPC_HEADER_MESSAGE,
    GRPC_HEADER_STATUS,
)


def grpc_error_to_trailer(trailer: Headers, error: ConnectError | None) -> None:
    """Convert a ConnectError to gRPC trailer headers.

    Args:
        trailer (Headers): The trailer headers dictionary to update with gRPC error information.
        error (ConnectError | None): The error to convert. If None, indicates success.

    Side Effects:
        Modifies the `trailer` dictionary in-place to include gRPC status, message, and optional details.

    Notes:
        - If `error` is None, sets the gRPC status header to "0" (OK).
        - If `ConnectError.wire_error` is False, updates the trailer with error metadata excluding protocol headers.
        - Serializes error details using protobuf if present, encoding them in base64 for the trailer.

    """
    if error is None:
        trailer[GRPC_HEADER_STATUS] = "0"
        return

    if not error.wire_error:
        trailer.update(exclude_protocol_headers(error.metadata))

    status = status_pb2.Status(
        code=error.code.value,
        message=error.raw_message,
        details=error.details_any(),
    )
    code = status.code
    message = status.message
    details_binary = None

    if len(status.details) > 0:
        details_binary = status.SerializeToString()

    trailer[GRPC_HEADER_STATUS] = str(code)
    trailer[GRPC_HEADER_MESSAGE] = quote(message)
    if details_binary:
        trailer[GRPC_HEADER_DETAILS] = base64.b64encode(details_binary).decode().rstrip("=")


def grpc_error_from_trailer(trailers: Headers) -> ConnectError | None:
    """Parse gRPC error information from response trailers and constructs a ConnectError if present.

    Args:
        trailers (Headers): The gRPC response trailers containing error information.

    Returns:
        ConnectError | None: Returns a ConnectError instance if an error is found in the trailers,
        or None if the status code indicates success.

    Raises:
        ConnectError: If the grpc-status-details-bin trailer or protobuf error details are invalid.

    The function extracts the gRPC status code, error message, and optional error details from the trailers.
    If the status code is missing or invalid, it returns a ConnectError with an appropriate message.
    If the status code indicates success ("0"), it returns None.
    If error details are present and valid, they are attached to the ConnectError.

    """
    code_header = trailers.get(GRPC_HEADER_STATUS)
    if code_header is None:
        code = Code.UNKNOWN
        if len(trailers) == 0:
            code = Code.INTERNAL

        return ConnectError(
            f"protocol error: no {GRPC_HEADER_STATUS} header in trailers",
            code,
        )

    if code_header == "0":
        return None

    try:
        code = Code(int(code_header))
    except ValueError:
        return ConnectError(
            f"protocol error: invalid error code {code_header} in trailers",
        )

    try:
        message = unquote(trailers.get(GRPC_HEADER_MESSAGE, ""))
    except Exception:
        return ConnectError(
            f"protocol error: invalid error message {code_header} in trailers",
            code=Code.UNKNOWN,
        )

    ret_error = ConnectError(
        message,
        code,
        wire_error=True,
    )

    details_binary_encoded = trailers.get(GRPC_HEADER_DETAILS, None)
    if details_binary_encoded and len(details_binary_encoded) > 0:
        try:
            details_binary = decode_binary_header(details_binary_encoded)
        except Exception as e:
            raise ConnectError(
                f"server returned invalid grpc-status-details-bin trailer: {e}",
                code=Code.INTERNAL,
            ) from e

        status = status_pb2.Status()
        try:
            status.ParseFromString(details_binary)
        except DecodeError as e:
            raise ConnectError(
                f"server returned invalid protobuf for error details: {e}",
                code=Code.INTERNAL,
            ) from e

        for detail in status.details:
            ret_error.details.append(ErrorDetail(pb_any=detail))

        ret_error.code = Code(status.code)
        ret_error.raw_message = status.message

    return ret_error


def decode_binary_header(data: str) -> bytes:
    """Decode a base64-encoded string representing a binary header.

    If the input string's length is not a multiple of 4, it pads the string with '=' characters
    to make it valid base64 before decoding.

    Args:
        data (str): The base64-encoded string to decode.

    Returns:
        bytes: The decoded binary data.

    """
    if len(data) % 4:
        data += "=" * (-len(data) % 4)

    return base64.b64decode(data, validate=True)
