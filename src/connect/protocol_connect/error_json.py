import base64
import contextlib
import json
from typing import Any

import google.protobuf.any_pb2 as any_pb2
from google.protobuf import json_format

from connect.code import Code
from connect.error import DEFAULT_ANY_RESOLVER_PREFIX, ConnectError, ErrorDetail

_string_to_code: dict[str, Code] | None = None


def code_to_string(value: Code) -> str:
    """Convert a Code object to its string representation.

    If the Code object has a 'name' attribute and it is not None, the method returns
    the lowercase version of the 'name'. Otherwise, it returns the string representation
    of the 'value' attribute.

    Args:
        value (Code): The Code object to be converted to a string.

    Returns:
        str: The string representation of the Code object.

    """
    if not hasattr(value, "name") or value.name is None:
        return str(value.value)

    return value.name.lower()


def code_from_string(value: str) -> Code | None:
    """Convert a string representation of a code to its corresponding Code enum value.

    This function uses a global dictionary to cache the mapping from string to Code enum values.
    If the cache is not initialized, it populates the cache by iterating over all Code enum values
    and mapping their string representations to the corresponding Code enum.

    Args:
        value (str): The string representation of the code.

    Returns:
        Code | None: The corresponding Code enum value if found, otherwise None.

    """
    global _string_to_code

    if _string_to_code is None:
        _string_to_code = {}
        for code in Code:
            _string_to_code[code_to_string(code)] = code

    return _string_to_code.get(value)


def error_from_json(obj: dict[str, Any], fallback: ConnectError) -> ConnectError:
    """Convert a JSON-serializable dictionary to a ConnectError object.

    Args:
        obj (dict[str, Any]): The dictionary representing the error in JSON format.
        fallback (ConnectError): A fallback ConnectError object to use in case of missing or invalid fields.

    Returns:
        ConnectError: The ConnectError object converted from the dictionary.

    Raises:
        ConnectError: If the dictionary is missing required fields or contains invalid values,
                      a ConnectError is raised with an appropriate error message and code.

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
            decoded = base64.b64decode(value.encode() + b"=" * (4 - len(value) % 4))
        except Exception as e:
            raise fallback from e

        error.details.append(
            ErrorDetail(pb_any=any_pb2.Any(type_url=type_name, value=decoded), wire_json=json.dumps(detail))
        )

    return error


def error_to_json(error: ConnectError) -> dict[str, Any]:
    """Convert a ConnectError object to a JSON-serializable dictionary.

    Args:
        error (ConnectError): The error object to convert.

    Returns:
        dict[str, Any]: A dictionary representing the error in JSON format.
            - "code" (str): The error code as a string.
            - "message" (str, optional): The raw error message, if available.
            - "details" (list[dict[str, Any]], optional): A list of dictionaries containing error details, if available.
                Each detail dictionary contains:
                - "type" (str): The type name of the detail.
                - "value" (str): The base64-encoded value of the detail.
                - "debug" (str, optional): The JSON-encoded debug information, if available.

    """
    obj: dict[str, Any] = {"code": error.code.string()}

    if len(error.raw_message) > 0:
        obj["message"] = error.raw_message

    if len(error.details) > 0:
        wires = []
        for detail in error.details:
            wire: dict[str, Any] = {
                "type": detail.pb_any.TypeName(),
                "value": base64.b64encode(detail.pb_any.value).decode().rstrip("="),
            }

            with contextlib.suppress(Exception):
                meg = detail.get_inner()
                wire["debug"] = json_format.MessageToDict(meg)

            wires.append(wire)

        obj["details"] = wires

    return obj
