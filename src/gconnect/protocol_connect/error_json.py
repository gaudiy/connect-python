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

"""Utilities for serializing and deserializing ConnectError objects to and from JSON."""

import contextlib
import json
import threading
from typing import Any

import google.protobuf.any_pb2 as any_pb2
from google.protobuf import json_format

from gconnect.code import Code
from gconnect.error import DEFAULT_ANY_RESOLVER_PREFIX, ConnectError, ErrorDetail
from gconnect.protocol_connect.base64_utils import decode_base64_with_padding, encode_base64_without_padding

_string_to_code: dict[str, Code] | None = None
_code_mapping_lock = threading.Lock()


def code_to_string(value: Code) -> str:
    """Converts a Code enum value to its string representation.

    If the value has a 'name' attribute and it is not None, returns the lowercase name.
    Otherwise, returns the string representation of the value's 'value' attribute.

    Args:
        value (Code): The enum value to convert.

    Returns:
        str: The string representation of the enum value.
    """
    if not hasattr(value, "name") or value.name is None:
        return str(value.value)

    return value.name.lower()


def code_from_string(value: str) -> Code | None:
    """Converts a string representation of a code to its corresponding `Code` enum value.

    This function uses a thread-safe, lazily-initialized mapping to efficiently look up
    the `Code` enum associated with the given string. If the mapping is not yet initialized,
    it will be created in a thread-safe manner using double-checked locking.

    Args:
        value (str): The string representation of the code.

    Returns:
        Code | None: The corresponding `Code` enum value if found, otherwise `None`.
    """
    global _string_to_code

    # Double-checked locking pattern for thread safety
    if _string_to_code is None:
        with _code_mapping_lock:
            # Check again after acquiring the lock
            if _string_to_code is None:
                temp_mapping = {}
                for code in Code:
                    temp_mapping[code_to_string(code)] = code
                _string_to_code = temp_mapping

    return _string_to_code.get(value)


def error_from_json(obj: dict[str, Any], fallback: ConnectError) -> ConnectError:
    """Deserializes a JSON object into a ConnectError instance.

    Args:
        obj (dict[str, Any]): The JSON object representing the error, expected to contain
            at least a "message" field, and optionally "code" and "details".
        fallback (ConnectError): A fallback ConnectError instance to use for default values
            or to raise in case of malformed input.

    Returns:
        ConnectError: The deserialized ConnectError instance with populated message, code,
            and details.

    Raises:
        ConnectError: If required fields in the details are missing or if base64 decoding fails,
            the fallback error is raised.
    """
    code = fallback.code
    if "code" in obj:
        code = code_from_string(obj["code"]) or code

    message = obj.get("message", "")
    details = obj.get("details", [])

    error = ConnectError(message, code, wire_error=True)

    for detail in details:
        type_name = detail.get("type", None)
        value = detail.get("value", None)

        if type_name is None:
            raise fallback

        if value is None:
            raise fallback

        type_name = type_name if "/" in type_name else DEFAULT_ANY_RESOLVER_PREFIX + type_name
        try:
            decoded = decode_base64_with_padding(value)
        except Exception as e:
            raise fallback from e

        error.details.append(
            ErrorDetail(pb_any=any_pb2.Any(type_url=type_name, value=decoded), wire_json=json.dumps(detail))
        )

    return error


def error_to_json(error: ConnectError) -> dict[str, Any]:
    """Converts a ConnectError object into a JSON-serializable dictionary.

    Args:
        error (ConnectError): The error object to convert.

    Returns:
        dict[str, Any]: A dictionary representing the error, including its code, message, and details if present.

    The returned dictionary contains:
        - "code": The string representation of the error code.
        - "message": The raw error message, if available.
        - "details": A list of dictionaries for each error detail, each containing:
            - "type": The type name of the detail.
            - "value": The base64-encoded value of the detail.
            - "debug": (optional) A dictionary representation of the inner message, if available.
    """
    obj: dict[str, Any] = {"code": error.code.string()}

    if len(error.raw_message) > 0:
        obj["message"] = error.raw_message

    if len(error.details) > 0:
        wires = []
        for detail in error.details:
            wire: dict[str, Any] = {
                "type": detail.pb_any.TypeName(),
                "value": encode_base64_without_padding(detail.pb_any.value),
            }

            with contextlib.suppress(Exception):
                meg = detail.get_inner()
                wire["debug"] = json_format.MessageToDict(meg)

            wires.append(wire)

        obj["details"] = wires

    return obj
