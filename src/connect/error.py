# Copyright 2021-2024 The Connect Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""ConnectError represents an error in the Connect protocol."""

from dataclasses import dataclass
from typing import Any

from google.protobuf.descriptor import Descriptor as DescMessage

# Placeholder imports for protobuf functionality
# You might need to install protobuf with `pip install protobuf`
from connect.code import Code


# Function to convert Code enum to string
def code_to_string(code: Code) -> str:
    """Convert a Code enum to a string."""
    return code.name.lower().replace("_", " ")


# Type aliases for clarity
JsonValue = Any  # Adjust based on actual JSON value types
HeadersInit = dict[str, str] | None  # Adjust based on actual headers types


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


# ConnectError class definition
class ConnectError(Exception):
    """ConnectError represents an error in the Connect protocol."""

    def __init__(
        self,
        message: str,
        code: Code = Code.UNKNOWN,
        metadata: HeadersInit = None,
        outgoing_details: list[OutgoingDetail] | None = None,
        cause: Any | None = None,
    ):
        """Initialize a ConnectError."""
        super().__init__(create_message(message, code))
        # Set the raw message without the code prefix
        self.raw_message = message
        self.code = code
        self.metadata = metadata if metadata is not None else {}
        self.details = outgoing_details if outgoing_details is not None else []
        self.cause = cause

    @staticmethod
    def from_reason(reason: Any, code: Code = Code.UNKNOWN) -> "ConnectError":
        """Create a ConnectError from a reason.

        Args:
            reason (Any): _description_
            code (Code, optional): _description_. Defaults to Code.UNKNOWN.

        Returns:
            ConnectError: _description_

        """
        if isinstance(reason, ConnectError):
            return reason
        if isinstance(reason, Exception):
            if getattr(reason, "name", "") == "AbortError":
                return ConnectError(str(reason), Code.CANCELED, cause=reason)
            return ConnectError(str(reason), code, cause=reason)
        return ConnectError(str(reason), code, cause=reason)
