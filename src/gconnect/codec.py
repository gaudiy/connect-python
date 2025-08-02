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

"""Module providing codec classes for serializing and deserializing protobuf messages."""

import abc
import json
from typing import Any

import google.protobuf.message
from google.protobuf import json_format


class CodecNameType:
    """Defines the standard codec names used in the Connect Protocol.

    These names are used in the `Content-Type` header to specify the
    serialization format of the request and response bodies.

    Attributes:
        PROTO: The codec name for Protocol Buffers binary format ("proto").
        JSON: The codec name for JSON format ("json").
        JSON_CHARSET_UTF8: The codec name for JSON format with UTF-8 charset
            ("json; charset=utf-8").
    """

    PROTO = "proto"
    JSON = "json"
    JSON_CHARSET_UTF8 = "json; charset=utf-8"


class Codec(abc.ABC):
    """Defines the interface for a message codec.

    A Codec is responsible for serializing (marshaling) and deserializing (unmarshaling)
    messages between their Python object representation and their wire format as bytes.

    This is an abstract base class. Subclasses must implement the `name`, `marshal`,
    and `unmarshal` methods to provide a concrete implementation for a specific
    serialization format, such as Protocol Buffers or JSON.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Returns the name of the codec.

        This is an abstract method that must be implemented by subclasses.

        Returns:
            The name of the codec.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def marshal(self, message: Any) -> bytes:
        """Marshal a message into bytes.

        Args:
            message: The message to marshal.

        Returns:
            The marshaled message as bytes.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def unmarshal(self, data: bytes, message: Any) -> Any:
        """Unmarshals binary data into a message object.

        This method must be implemented by subclasses to define how to
        deserialize a byte string into a given message structure.

        Args:
            data (bytes): The raw binary data to be deserialized.
            message (Any): The target message object to populate with the
                deserialized data.

        Raises:
            NotImplementedError: This method is not implemented in the base class
                and must be overridden in a subclass.

        Returns:
            Any: The populated message object.
        """
        raise NotImplementedError()


class StableCodec(Codec):
    """Abstract base class for codecs that provide a stable byte representation.

    This class defines the interface for codecs that can serialize messages into
    a canonical, stable byte format. This is useful for scenarios like signing
    messages, where the byte representation must be consistent across different
    systems and executions.
    """

    @abc.abstractmethod
    def marshal_stable(self, message: Any) -> bytes:
        """Marshals a message into a stable byte representation.

        "Stable" means that the marshaling is deterministic: given the same
        message, the same bytes will be returned. This is important for
        use cases like cryptographic signing, where the exact byte sequence
        is critical.

        Args:
            message: The message to be marshaled.

        Returns:
            The stable byte representation of the message.

        Raises:
            NotImplementedError: This is an abstract method and must be
                implemented by a subclass.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def is_binary(self) -> bool:
        """Checks if the codec handles binary data.

        Returns:
            bool: True if the codec is binary, False otherwise.
        """
        raise NotImplementedError()


class ProtoBinaryCodec(StableCodec):
    """Codec for handling Protocol Buffers (protobuf) messages.

    This class implements the StableCodec interface to provide serialization
    and deserialization for protobuf messages. It converts protobuf message
    objects into byte strings and vice versa.

    The `marshal_stable` method provides a deterministic serialization, but it's
    important to note that protobuf's deterministic output is not guaranteed
    to be consistent across different library implementations or versions,
    especially when unknown fields are present.
    """

    @property
    def name(self) -> str:
        """Returns the name of the codec.

        Returns:
            The name of the codec.
        """
        return CodecNameType.PROTO

    def marshal(self, message: Any) -> bytes:
        """Serializes a protobuf message into a byte string.

        Args:
            message: The protobuf message to serialize.

        Returns:
            The serialized message as a byte string.

        Raises:
            ValueError: If the provided message is not a protobuf message.
        """
        if not isinstance(message, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        return message.SerializeToString()

    def unmarshal(self, data: bytes, message: Any) -> Any:
        """Unmarshals bytes into a protobuf message.

        Args:
            data: The bytes to unmarshal.
            message: The protobuf message type to unmarshal into.

        Returns:
            An instance of the message class populated with the given data.

        Raises:
            ValueError: If the given message is not a protobuf message type.
        """
        obj = message()
        if not isinstance(obj, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        obj.ParseFromString(data)
        return obj

    def marshal_stable(self, message: Any) -> bytes:
        """Serializes a protobuf message into a deterministic byte string.

        This method ensures that serializing the same message multiple times
        will produce the exact same byte string. This is useful for applications
        requiring a stable binary representation, such as cryptographic signing
        or hashing.

        Args:
            message: The protobuf message to serialize.

        Returns:
            A byte string representing the deterministically serialized message.

        Raises:
            ValueError: If the provided data is not a protobuf message.
        """
        if not isinstance(message, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        return message.SerializeToString(deterministic=True)

    def is_binary(self) -> bool:
        """Check if the codec handles binary data."""
        return True


class ProtoJSONCodec(StableCodec):
    """A codec for serializing and deserializing protobuf messages to and from JSON.

    This class implements the StableCodec interface to handle conversions between
    protobuf message objects and their JSON string representation. It leverages the
    `google.protobuf.json_format` library for the core conversion logic.

    The `marshal` and `unmarshal` methods provide standard serialization and
    deserialization. The `marshal_stable` method ensures a deterministic output
    by re-parsing the generated JSON and re-serializing it with compact
    separators. This guarantees a consistent byte representation, which is crucial
    for operations like request signing.

    This is a text-based codec, and as such, `is_binary()` will always return False.

    Attributes:
        name (str): The name of the codec (e.g., "json").
    """

    _name: str

    def __init__(self, name: str) -> None:
        """Initializes the codec.

        Args:
            name: The name of the codec.
        """
        self._name = name

    @property
    def name(self) -> str:
        """The name of the codec, e.g. "proto", "json"."""
        return self._name

    def marshal(self, message: Any) -> bytes:
        """Marshals a protobuf message to its JSON representation.

        Args:
            message: The protobuf message to marshal.

        Returns:
            The JSON representation of the message, encoded as bytes.

        Raises:
            ValueError: If the provided message is not a protobuf message.
        """
        if not isinstance(message, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        json_str = json_format.MessageToJson(message)

        return json_str.encode()

    def unmarshal(self, data: bytes, message: Any) -> Any:
        """Unmarshals byte data into a protobuf message.

        This method decodes the byte data as a UTF-8 string and then parses it
        as JSON into the provided protobuf message type.

        Args:
            data: The byte-encoded JSON data to unmarshal.
            message: The protobuf message class to instantiate and populate.

        Returns:
            An instance of the provided protobuf message class, populated with
            the data from the JSON.

        Raises:
            ValueError: If the `message` argument is not a protobuf message class.
            google.protobuf.json_format.ParseError: If the data is not valid JSON.
        """
        obj = message()
        if not isinstance(obj, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        return json_format.Parse(data.decode(), obj, ignore_unknown_fields=True)

    def marshal_stable(self, message: Any) -> bytes:
        """Marshals a protobuf message into a stable, compact JSON byte string.

        This method provides a deterministic way to serialize a protobuf message
        to a compact JSON format. It converts the message to a JSON string,
        then re-serializes it to remove whitespace and ensure a consistent
        output. The resulting byte string is stable, meaning the same message
        will always produce the exact same byte output.

        Args:
            message (Any): The protobuf message to be marshaled. Although typed as
                Any, this must be an instance of `google.protobuf.message.Message`.

        Returns:
            bytes: A compact, stable JSON representation of the message, encoded
                as a byte string.

        Raises:
            ValueError: If the input `message` is not a protobuf message.
        """
        if not isinstance(message, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        json_str = json_format.MessageToJson(message)

        parsed = json.loads(json_str)
        compacted_json = json.dumps(parsed, separators=(",", ":"))

        return compacted_json.encode()

    def is_binary(self) -> bool:
        """Check if the codec handles binary data.

        Returns:
            bool: Always False, indicating this codec does not handle binary data.
        """
        return False


class ReadOnlyCodecs(abc.ABC):
    """Defines the interface for a read-only collection of codecs.

    This abstract base class provides a standard way to retrieve registered codecs
    by name and to list the names of all available codecs. It is designed to be
    a non-modifiable view of the available encoding and decoding mechanisms.

    Subclasses must implement the `get`, `protobuf`, and `names` methods to provide
    concrete functionality.
    """

    @abc.abstractmethod
    def get(self, name: str) -> Codec | None:
        """Gets a codec by its name.

        Args:
            name: The name of the codec to retrieve.

        Returns:
            The codec instance if found, otherwise None.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def protobuf(self) -> Codec | None:
        """Returns the Protobuf codec, if available.

        This method should be implemented by subclasses to provide a codec
        for handling Protobuf-encoded messages.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.

        Returns:
            Codec | None: A Codec instance for Protobuf, or None if not supported.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def names(self) -> list[str]:
        """Get the names of the supported codecs.

        This is an abstract method. Subclasses should implement this to return the
        names of the codecs they support (e.g., "gzip", "identity").

        Returns:
            A list of strings, where each string is a supported codec name.
        """
        raise NotImplementedError()


class CodecMap(ReadOnlyCodecs):
    """Manages a collection of codecs, mapping their names to Codec instances.

    This class provides a way to store and retrieve different encoding/decoding
    mechanisms (codecs) used in communication protocols. It allows looking up a
    specific codec by its registered name.

    Attributes:
        name_to_codec (dict[str, Codec]): A dictionary where keys are codec names
            and values are the corresponding Codec objects.
    """

    name_to_codec: dict[str, Codec]

    def __init__(self, name_to_codec: dict[str, Codec]) -> None:
        """Initializes the codec registry.

        Args:
            name_to_codec: A dictionary mapping codec names to Codec instances.
        """
        self.name_to_codec = name_to_codec

    def get(self, name: str) -> Codec | None:
        """Gets a codec by its registered name.

        Args:
            name: The name of the codec to retrieve.

        Returns:
            The codec instance if found, otherwise None.
        """
        return self.name_to_codec.get(name)

    def protobuf(self) -> Codec | None:
        """A convenience method for retrieving the protobuf codec.

        Returns:
            The protobuf codec, or None if it is not available.
        """
        return self.get(CodecNameType.PROTO)

    def names(self) -> list[str]:
        """Get the names of all registered codecs.

        Returns:
            A list of the registered codec names.
        """
        return list(self.name_to_codec.keys())
