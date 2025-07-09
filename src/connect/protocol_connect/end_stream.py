"""Helpers for serializing and deserializing Connect end-of-stream messages."""

import json
from typing import Any

from connect.code import Code
from connect.error import ConnectError
from connect.headers import Headers
from connect.protocol_connect.error_json import error_from_json, error_to_json


def end_stream_to_json(error: ConnectError | None, trailers: Headers) -> dict[str, Any]:
    """Converts the end-of-stream state, including an optional error and trailers, into a JSON-serializable dictionary.

    Args:
        error (ConnectError | None): An optional error object representing the stream error, if any.
        trailers (Headers): The headers (trailers) to include as metadata in the JSON output.

    Returns:
        dict[str, Any]: A dictionary containing the serialized error (if present) and metadata extracted from the trailers.
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
    """Parses a byte string representing an end stream message and returns a tuple containing a possible ConnectError and Headers.

    Args:
        data (bytes): The byte string to parse, expected to be a JSON-encoded object.

    Returns:
        tuple[ConnectError | None, Headers]: A tuple where the first element is a ConnectError if an error is present in the input, or None otherwise; the second element is a Headers object containing parsed metadata.

    Raises:
        ConnectError: If the input data is not valid JSON, or if the metadata format is invalid.
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
        metadata_obj = obj["metadata"]
        if not isinstance(metadata_obj, dict) or not all(
            isinstance(k, str) and isinstance(v, list) for k, v in metadata_obj.items()
        ):
            raise ConnectError(
                "invalid end stream",
                Code.UNKNOWN,
            )

        for key, values in metadata_obj.items():
            metadata[key] = ", ".join(values)

    error_obj = obj.get("error")
    if error_obj is not None:
        error = error_from_json(error_obj, parse_error)
        return error, metadata

    return None, metadata
