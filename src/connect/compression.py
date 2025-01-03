"""Module providing constants for different types of compression."""

import abc
import gzip
import io

COMPRESSION_GZIP = "gzip"
COMPRESSION_IDENTITY = "identity"


class Compression(abc.ABC):
    """Abstract base class for compression algorithms.

    This class defines the interface for compression and decompression methods
    that must be implemented by any concrete compression class.

    """

    @abc.abstractmethod
    def name(self) -> str:
        """Return the name of the compression algorithm.

        Returns:
            str: The name of the compression algorithm.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def compress(self, data: bytes) -> bytes:
        """Compresse the given data using a specified compression algorithm.

        Args:
            data (bytes): The data to be compressed.

        Returns:
            bytes: The compressed data.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def decompress(self, data: bytes, read_max_bytes: int) -> bytes:
        """Decompresse the given data.

        Args:
            data (bytes): The compressed data to be decompressed.
            read_max_bytes (int): The maximum number of bytes to read from the decompressed data.

        Returns:
            bytes: The decompressed data.

        """
        raise NotImplementedError()


class GZipCompression(Compression):
    """A class to handle GZip compression and decompression."""

    def __init__(self) -> None:
        """Initialize the compression object with the default compression method."""
        self.__name = COMPRESSION_GZIP

    def name(self) -> str:
        """Return the name attribute of the object.

        Returns:
            str: The name attribute.

        """
        return self.__name

    def compress(self, data: bytes) -> bytes:
        """Compresse the given data using gzip compression.

        Args:
            data (bytes): The data to be compressed.

        Returns:
            bytes: The compressed data.

        """
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as f:
            f.write(data)

        return buf.getvalue()

    def decompress(self, data: bytes, read_max_bytes: int) -> bytes:
        """Decompresse the given gzip-compressed data.

        Args:
            data (bytes): The gzip-compressed data to decompress.
            read_max_bytes (int): The maximum number of bytes to read from the decompressed data.
            If read_max_bytes is less than or equal to 0, all decompressed data will be read.

        Returns:
            bytes: The decompressed data.

        """
        read_max_bytes = read_max_bytes if read_max_bytes > 0 else -1

        buf = io.BytesIO(data)
        with gzip.GzipFile(fileobj=buf, mode="rb") as f:
            data = f.read(read_max_bytes)

        return data
