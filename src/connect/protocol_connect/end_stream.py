"""Module for handling end-of-stream JSON serialization and deserialization for Connect protocol."""

import json
from typing import Any

from connect.code import Code
from connect.error import ConnectError
from connect.headers import Headers
from connect.protocol_connect.error_json import error_from_json, error_to_json


def end_stream_to_json(error: ConnectError | None, trailers: Headers) -> dict[str, Any]:
    """Convert the end of a stream to a JSON-serializable dictionary.

    Args:
        error (ConnectError | None): An optional error object that may contain metadata.
        trailers (Headers): Headers object containing metadata.

    Returns:
        dict[str, Any]: A dictionary containing the error and metadata information in JSON-serializable format.

    """
    json_obj = {}

    metadata = Headers(trailers.copy())
    if error:
        json_obj["error"] = error_to_json(error)
        metadata.update(error.metadata.copy())

    if len(metadata) > 0:
        json_obj["metadata"] = {k: v.split(", ") for k, v in metadata.items()}

    return json_obj


def end_stream_from_bytes(data: bytes) -> tuple[ConnectError | None, Headers]:
    """Parse a byte stream to extract metadata and error information.

    Args:
        data (bytes): The byte stream to be parsed.

    Returns:
        tuple[ConnectError | None, Headers]: A tuple containing an optional ConnectError
        and a Headers object with the parsed metadata.

    Raises:
        ConnectError: If the byte stream is invalid or the metadata format is incorrect.

    """
    parse_error = ConnectError("invalid end stream", Code.UNKNOWN)
    try:
        obj = json.loads(data)
    except Exception as e:
        raise ConnectError(
            "invalid end stream",
            Code.UNKNOWN,
        ) from e

    metadata = Headers()
    if "metadata" in obj:
        if not isinstance(obj["metadata"], dict) or not all(
            isinstance(k, str) and isinstance(v, list) for k, v in obj["metadata"].items()
        ):
            raise ConnectError(
                "invalid end stream",
                Code.UNKNOWN,
            )

        for key, values in obj["metadata"].items():
            value = ", ".join(values)
            metadata[key] = value

    if "error" in obj and obj["error"] is not None:
        error = error_from_json(obj["error"], parse_error)
        return error, metadata
    else:
        return None, metadata
