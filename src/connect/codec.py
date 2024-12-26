import abc
from typing import Any

import google.protobuf.message


class CodecNameType:
    PROTO = "proto"
    JSON = "json"
    JSON_CHARSET_UTF8 = "json; charset=utf-8"


class Codec(abc.ABC):
    """Codec class."""

    @abc.abstractmethod
    def name(self) -> str:
        pass

    @abc.abstractmethod
    def marshal(self, message: Any) -> bytes:
        raise NotImplementedError

    @abc.abstractmethod
    def unmarshal(self, data: bytes, message: Any) -> Any:
        raise NotImplementedError


class ProtoBinaryCodec(Codec):
    """ProtoBinaryCodec class."""

    def name(self) -> str:
        return CodecNameType.PROTO

    def marshal(self, message: Any) -> bytes:
        if not isinstance(message, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        return message.SerializeToString()

    def unmarshal(self, data: bytes, message: Any) -> Any:
        obj = message()
        if not isinstance(obj, google.protobuf.message.Message):
            raise ValueError("Data is not a protobuf message")

        obj.ParseFromString(data)
        return obj


class ReadOnlyCodecs(abc.ABC):
    """ReadOnlyCodecs class."""

    @abc.abstractmethod
    def get(self, name: str) -> Codec:
        pass

    @abc.abstractmethod
    def protobuf(self) -> Codec:
        pass

    @abc.abstractmethod
    def names(self) -> list[str]:
        pass


class CodecMap(ReadOnlyCodecs):
    """CodecMap class."""

    name_to_codec: dict[str, Codec]

    def __init__(self, name_to_codec: dict[str, Codec]) -> None:
        self.name_to_codec = name_to_codec

    def get(self, name: str) -> Codec:
        return self.name_to_codec[name]

    def protobuf(self) -> Codec:
        return self.get(CodecNameType.PROTO)

    def names(self) -> list[str]:
        return list(self.name_to_codec.keys())
