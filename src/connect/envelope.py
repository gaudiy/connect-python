"""Module containing the Envelope class which represents a data envelope."""

import struct
from enum import Flag


class EnvelopeFlags(Flag):
    """EnvelopeFlags is an enumeration that defines flags for an envelope.

    Attributes:
        compressed (int): Flag indicating that the envelope is compressed.
        end_stream (int): Flag indicating that the envelope marks the end of a stream.

    """

    compressed = 0b00000001
    end_stream = 0b00000010


class Envelope:
    """A class to represent an Envelope which contains data and flags.

    Attributes:
        data (bytes): The data contained in the envelope.
        flags (EnvelopeFlags): The flags associated with the envelope.
        _format (str): The format string used for struct packing and unpacking.

    """

    data: bytes
    flags: EnvelopeFlags
    _format: str = ">BI"

    def __init__(self, data: bytes, flags: EnvelopeFlags) -> None:
        """Initialize a new instance of the class.

        Args:
            data (bytes): The data to be processed.
            flags (EnvelopeFlags): The flags associated with the envelope.

        """
        self.data = data
        self.flags = flags

    def encode(self) -> bytes:
        """Encode the header and data into a byte sequence.

        Returns:
            bytes: The encoded byte sequence consisting of the header and data.

        """
        return self.encode_header(self.flags.value, self.data) + self.data

    def encode_header(self, flags: int, data: bytes) -> bytes:
        """Encode the header for a protocol message.

        Args:
            flags (int): The flags to include in the header.
            data (bytes): The data to be sent, used to determine the length.

        Returns:
            bytes: The encoded header as a byte string.

        """
        return struct.pack(self._format, flags, len(data))

    @staticmethod
    def decode_header(data: bytes) -> tuple[EnvelopeFlags, int] | None:
        """Decode the header from the given byte data.

        Args:
            data (bytes): The byte data containing the header to decode.

        Returns:
            tuple[EnvelopeFlags, int] | None: A tuple containing the decoded EnvelopeFlags and data length if the data is valid,
                                              otherwise None if the data length is less than 5 bytes.

        """
        if len(data) < 5:
            return None

        flags, data_len = struct.unpack(Envelope._format, data[:5])
        return EnvelopeFlags(flags), data_len

    @staticmethod
    def decode(data: bytes) -> "tuple[Envelope | None, int]":
        """Decode the given byte data into an Envelope object and its length.

        Args:
            data (bytes): The byte data to decode.

        Returns:
            tuple[Envelope | None, int]: A tuple containing the decoded Envelope object (or None if decoding fails)
            and the length of the data. If the data is insufficient to decode, returns (None, data_len).

        """
        header = Envelope.decode_header(data)
        if header is None:
            return None, 0

        flags, data_len = header
        if len(data) < 5 + data_len:
            return None, data_len

        return Envelope(data[5 : 5 + data_len], flags), data_len

    def is_set(self, flag: EnvelopeFlags) -> bool:
        """Check if a specific flag is set in the envelope.

        Args:
            flag (EnvelopeFlags): The flag to check.

        Returns:
            bool: True if the flag is set, False otherwise.

        """
        return flag in self.flags
