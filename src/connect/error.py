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

from dataclasses import dataclass
from typing import Any

from google.protobuf.descriptor import Descriptor as DescMessage

# Placeholder imports for protobuf functionality
# You might need to install protobuf with `pip install protobuf`
from google.protobuf.message import Message as ProtoMessage

from connect.code import Code


# Function to convert Code enum to string
def code_to_string(code: Code) -> str:
    """Convert a Code enum to a string."""
    return code.name.lower().replace("_", " ")


# Type aliases for clarity
JsonValue = Any  # Adjust based on actual JSON value types
HeadersInit = dict[str, str] | None  # Adjust based on actual headers types


class Registry:
    def __init__(self):
        self._messages: dict[str, DescMessage] = {}

    def register_message(self, desc: DescMessage):
        self._messages[desc.type_name] = desc

    def get_message(self, type_name: str) -> DescMessage | None:
        return self._messages.get(type_name)


# Placeholder functions for creating and parsing protobuf messages
def create(desc: DescMessage, value: Any) -> ProtoMessage:
    # Implement message creation based on descriptor and value
    # This is a stub and should be replaced with actual implementation
    return ProtoMessage()


def from_binary(desc: DescMessage, value: bytes) -> ProtoMessage:
    # Implement parsing from binary based on descriptor
    # This is a stub and should be replaced with actual implementation
    msg = ProtoMessage()
    msg.ParseFromString(value)
    return msg


# Define IncomingDetail and OutgoingDetail using dataclasses
@dataclass
class IncomingDetail:
    type: str
    value: bytes
    debug: JsonValue | None = None


@dataclass
class OutgoingDetail:
    desc: DescMessage
    value: Any  # Replace with actual MessageInitShape if defined


# Helper function to create error messages with code prefix
def create_message(message: str, code: Code) -> str:
    if message:
        return f"[{code_to_string(code)}] {message}"
    return f"[{code_to_string(code)}]"


# ConnectError class definition
class ConnectError(Exception):
    def __init__(
        self,
        message: str,
        code: Code = Code.UNKNOWN,
        metadata: HeadersInit = None,
        outgoing_details: list[OutgoingDetail] | None = None,
        cause: Any | None = None,
    ):
        super().__init__(create_message(message, code))
        # Set the raw message without the code prefix
        self.raw_message: str = message
        self.code: Code = code
        self.metadata: dict[str, str] = metadata if metadata is not None else {}
        self.details: list[OutgoingDetail | IncomingDetail] = outgoing_details if outgoing_details is not None else []
        self.cause: Any = cause

    @staticmethod
    def from_reason(reason: Any, code: Code = Code.UNKNOWN) -> "ConnectError":
        if isinstance(reason, ConnectError):
            return reason
        if isinstance(reason, Exception):
            if getattr(reason, "name", "") == "AbortError":
                return ConnectError(str(reason), Code.CANCELLED, cause=reason)
            return ConnectError(str(reason), code, cause=reason)
        return ConnectError(str(reason), code, cause=reason)

    def find_details(self, type_or_registry: DescMessage | Registry) -> list[ProtoMessage]:
        registry: Registry
        if isinstance(type_or_registry, DescMessage) and type_or_registry.kind == "message":
            registry = Registry()
            registry.register_message(type_or_registry)
        elif isinstance(type_or_registry, Registry):
            registry = type_or_registry
        else:
            raise ValueError("type_or_registry must be a DescMessage or Registry")

        found_details: list[ProtoMessage] = []
        for detail in self.details:
            if isinstance(detail, OutgoingDetail):
                desc = detail.desc
                if registry.get_message(desc.type_name):
                    message = create(desc, value=detail.value)
                    found_details.append(message)
            elif isinstance(detail, IncomingDetail):
                desc = registry.get_message(detail.type)
                if desc:
                    try:
                        message = from_binary(desc, detail.value)
                        found_details.append(message)
                    except Exception:
                        # Silently ignore decoding errors
                        pass
        return found_details

    def __str__(self):
        return self.args[0]

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(message={self.raw_message!r}, "
            f"code={self.code}, metadata={self.metadata}, "
            f"details={self.details}, cause={self.cause!r})"
        )

    # Optional: Implement __eq__ and other methods as needed


# Example usage
if __name__ == "__main__":
    # Create a ConnectError
    error = ConnectError("Resource not found", Code.NOT_FOUND)
    print(error)  # Output: [not found] Resource not found
    print(error.raw_message)  # Output: Resource not found
    print(error.code)  # Output: Code.NOT_FOUND

    # Convert another error to ConnectError
    try:
        raise ValueError("Invalid value")
    except Exception as e:
        connect_error = ConnectError.from_reason(e, Code.UNKNOWN)
        print(connect_error)  # Output: [unknown] Invalid value
        print(connect_error.cause)  # Output: ValueError("Invalid value")
