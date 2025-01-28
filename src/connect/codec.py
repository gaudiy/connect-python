"""Module providing codec classes for serializing and deserializing protobuf messages."""

import abc
import json
from typing import Any

import google.protobuf.message
from google.protobuf import json_format


class CodecNameType:
    """CodecNameType is a class that defines constants for different codec types.

    Attributes:
        PROTO (str): Represents the "proto" codec type.
        JSON (str): Represents the "json" codec type.
        JSON_CHARSET_UTF8 (str): Represents the "json; charset=utf-8" codec type.

    """

    PROTO = "proto"
    JSON = "json"
    JSON_CHARSET_UTF8 = "json; charset=utf-8"


class Codec(abc.ABC):
    """Abstract base class for codecs.

    This class defines the interface for codecs that can serialize and deserialize
    protobuf messages. Subclasses must implement the following methods.

    """

    @abc.abstractmethod
    def name(self) -> str:
        """Return the name of the codec.

        Returns:
            str: The name of the codec.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def marshal(self, message: Any) -> bytes:
        """Serialize a protobuf message to bytes.

        Args:
            message (Any): The protobuf message to serialize.

        Returns:
            bytes: The serialized message as bytes.

        Raises:
            ValueError: If the message is not a protobuf message.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def unmarshal(self, data: bytes, message: Any) -> Any:
        """Unmarshals the given byte data into the specified message format.

        Args:
            data (bytes): The byte data to be unmarshaled.
            message (Any): The message format to unmarshal the data into.

        Returns:
            Any: The unmarshaled message.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """
        raise NotImplementedError()


class StableCodec(Codec):
    """StableCodec is an abstract base class that defines the interface for codecs.

    This class can marshal messages into a stable binary format.
    """

    @abc.abstractmethod
    def marshal_stable(self, message: Any) -> bytes:
        """Serialize the given message into a stable byte representation.

        Args:
            message (Any): The message to be serialized.

        Returns:
            bytes: The serialized byte representation of the message.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def is_binary(self) -> bool:
        """Determine if the codec is binary.

        This method should be implemented by subclasses to indicate whether the codec
        handles binary data.

        Returns:
            bool: True if the codec is binary, False otherwise.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.

        """
        raise NotImplementedError()


class ProtoBinaryCodec(StableCodec):
    """ProtoBinaryCodec is a codec for serializing and deserializing protobuf messages."""

    def name(self) -> str:
        """Return the name of the codec.

        Returns:
            str: The name of the codec, which is 'PROTO'.

        """
        return CodecNameType.PROTO

    def marshal(self, message: Any) -> bytes:
        """Serialize a protobuf message to a byte string.

        Args:
            message (Any): The protobuf message to serialize.

        Returns:
            bytes: The serialized byte string of the protobuf message.

        Raises:
            ValueError: If the provided message is not an instance of google.protobuf.message.Message.

        """
        if not isinstance(message, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        return message.SerializeToString()

    def unmarshal(self, data: bytes, message: Any) -> Any:
        """Unmarshals the given byte data into a protobuf message.

        Args:
            data (bytes): The byte data to be unmarshaled.
            message (Any): The protobuf message class to unmarshal the data into.

        Returns:
            Any: The unmarshaled protobuf message object.

        Raises:
            ValueError: If the provided message is not a protobuf message.

        """
        obj = message()
        if not isinstance(obj, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        obj.ParseFromString(data)
        return obj

    def marshal_stable(self, message: Any) -> bytes:
        """Serialize a given protobuf message to a deterministic byte string.

        Protobuf does not offer a canonical output today, so this format is not
        guaranteed to match deterministic output from other protobuf libraries.
        In addition, unknown fields may cause inconsistent output for otherwise
        equal messages.
        https://github.com/golang/protobuf/issues/1121

        Args:
            message (Any): The protobuf message to be serialized. It must be an
                           instance of `google.protobuf.message.Message`.

        Returns:
            bytes: The serialized byte string representation of the protobuf message.

        Raises:
            ValueError: If the provided message is not a protobuf message.

        """
        if not isinstance(message, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        return message.SerializeToString(deterministic=True)

    def is_binary(self) -> bool:
        """Check if the codec is binary.

        Returns:
            bool: Always returns True indicating the codec is binary.

        """
        return True


class ProtoJSONCodec(StableCodec):
    """A codec for encoding and decoding Protocol Buffers messages to and from JSON format.

    Attributes:
        _name (str): The name of the codec.

    """

    _name: str

    def __init__(self, name: str) -> None:
        """Initialize the codec with a given name.

        Args:
            name (str): The name to initialize the codec with.

        """
        self._name = name

    def name(self) -> str:
        """Return the name of the codec.

        Returns:
            str: The name of the codec.

        """
        return self._name

    def marshal(self, message: Any) -> bytes:
        """Serialize a protobuf message to a JSON string encoded as UTF-8 bytes.

        Args:
            message (Any): The protobuf message to be serialized. Must be an instance of google.protobuf.message.Message.

        Returns:
            bytes: The serialized JSON string encoded as UTF-8 bytes.

        Raises:
            ValueError: If the provided message is not a protobuf message.

        """
        if not isinstance(message, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        json_str = json_format.MessageToJson(message)

        return json_str.encode("utf-8")

    def unmarshal(self, data: bytes, message: Any) -> Any:
        """Unmarshal the given byte data into a protobuf message.

        Args:
            data (bytes): The byte data to unmarshal.
            message (Any): The protobuf message class to unmarshal the data into.

        Returns:
            Any: The unmarshaled protobuf message instance.

        Raises:
            ValueError: If the provided message is not a protobuf message.

        """
        obj = message()
        if not isinstance(obj, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        return json_format.Parse(data.decode("utf-8"), obj, ignore_unknown_fields=True)

    def marshal_stable(self, message: Any) -> bytes:
        """Serialize a protobuf message to a JSON string encoded as UTF-8 bytes in a deterministic way.

        protojson does not offer a "deterministic" field ordering, but fields
        are still ordered consistently by their index. However, protojson can
        output inconsistent whitespace for some reason, therefore it is
        suggested to use a formatter to ensure consistent formatting.
        https://github.com/golang/protobuf/issues/1373

        Args:
            message (Any): The protobuf message to be serialized. Must be an instance of google.protobuf.message.Message.

        Returns:
            bytes: The serialized JSON string encoded as UTF-8 bytes.

        Raises:
            ValueError: If the provided message is not a protobuf message.

        """
        if not isinstance(message, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        json_str = json_format.MessageToJson(message)

        parsed = json.loads(json_str)
        compacted_json = json.dumps(parsed, separators=(",", ":"))

        return compacted_json.encode("utf-8")

    def is_binary(self) -> bool:
        """Determine if the codec is binary.

        Returns:
            bool: Always returns False, indicating the codec is not binary.

        """
        return False


class ReadOnlyCodecs(abc.ABC):
    """Abstract base class for read-only codecs.

    This class defines the interface for read-only codecs, which are responsible for
    encoding and decoding data. Implementations of this class must provide concrete
    implementations for the following methods.

    """

    @abc.abstractmethod
    def get(self, name: str) -> Codec | None:
        """Retrieve a codec by its name.

        Args:
            name (str): The name of the codec to retrieve.

        Returns:
            Codec: The codec associated with the given name.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def protobuf(self) -> Codec | None:
        """Encode data using the Protocol Buffers (protobuf) codec.

        Returns:
            Codec: An instance of the Codec class configured for protobuf encoding.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def names(self) -> list[str]:
        """Return a list of names.

        Returns:
            list[str]: A list of names as strings.

        """
        raise NotImplementedError()


class CodecMap(ReadOnlyCodecs):
    """CodecMap is a class that provides a mapping from codec names to their corresponding Codec objects. It extends the ReadOnlyCodecs class."""

    name_to_codec: dict[str, Codec]

    def __init__(self, name_to_codec: dict[str, Codec]) -> None:
        """Initialize the codec mapping.

        Args:
            name_to_codec (dict[str, Codec]): A dictionary mapping codec names to their corresponding Codec objects.

        """
        self.name_to_codec = name_to_codec

    def get(self, name: str) -> Codec | None:
        """Retrieve a codec by its name.

        Args:
            name (str): The name of the codec to retrieve.

        Returns:
            Codec: The codec associated with the given name.

        Raises:
            KeyError: If the codec with the specified name does not exist.

        """
        return self.name_to_codec.get(name)

    def protobuf(self) -> Codec | None:
        """Return the Codec instance associated with Protocol Buffers (PROTO).

        This method retrieves the Codec instance that corresponds to the
        Protocol Buffers (PROTO) codec type.

        Returns:
            Codec: The Codec instance for Protocol Buffers.

        """
        return self.get(CodecNameType.PROTO)

    def names(self) -> list[str]:
        """Retrieve a list of codec names.

        Returns:
            list[str]: A list of codec names.

        """
        return list(self.name_to_codec.keys())
