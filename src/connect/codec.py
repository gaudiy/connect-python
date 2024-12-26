"""Module providing codec classes for serializing and deserializing protobuf messages."""

import abc
from typing import Any

import google.protobuf.message


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
        pass

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
        raise NotImplementedError

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
        raise NotImplementedError


class ProtoBinaryCodec(Codec):
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


class ReadOnlyCodecs(abc.ABC):
    """Abstract base class for read-only codecs.

    This class defines the interface for read-only codecs, which are responsible for
    encoding and decoding data. Implementations of this class must provide concrete
    implementations for the following methods.

    """

    @abc.abstractmethod
    def get(self, name: str) -> Codec:
        """Retrieve a codec by its name.

        Args:
            name (str): The name of the codec to retrieve.

        Returns:
            Codec: The codec associated with the given name.

        """
        pass

    @abc.abstractmethod
    def protobuf(self) -> Codec:
        """Encode data using the Protocol Buffers (protobuf) codec.

        Returns:
            Codec: An instance of the Codec class configured for protobuf encoding.

        """
        pass

    @abc.abstractmethod
    def names(self) -> list[str]:
        """Return a list of names.

        Returns:
            list[str]: A list of names as strings.

        """
        pass


class CodecMap(ReadOnlyCodecs):
    """CodecMap is a class that provides a mapping from codec names to their corresponding Codec objects. It extends the ReadOnlyCodecs class."""

    name_to_codec: dict[str, Codec]

    def __init__(self, name_to_codec: dict[str, Codec]) -> None:
        """Initialize the codec mapping.

        Args:
            name_to_codec (dict[str, Codec]): A dictionary mapping codec names to their corresponding Codec objects.

        """
        self.name_to_codec = name_to_codec

    def get(self, name: str) -> Codec:
        """Retrieve a codec by its name.

        Args:
            name (str): The name of the codec to retrieve.

        Returns:
            Codec: The codec associated with the given name.

        Raises:
            KeyError: If the codec with the specified name does not exist.

        """
        return self.name_to_codec[name]

    def protobuf(self) -> Codec:
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
