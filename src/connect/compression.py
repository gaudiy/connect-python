"""Module providing constants for different types of compression."""

import abc
import gzip
import io

COMPRESSION_GZIP = "gzip"
COMPRESSION_IDENTITY = "identity"


class Compression(abc.ABC):
    """Abstract base class for defining compression and decompression logic.

    This class provides a standard interface for different compression algorithms
    used in Connect. Subclasses are expected to implement the `name` property,
    and the `compress` and `decompress` methods.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Gets the name of the compression algorithm.

        This is an abstract method that must be implemented by subclasses.

        Raises:
            NotImplementedError: This method is not implemented in the base class.

        Returns:
            The name of the compression algorithm.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def compress(self, data: bytes) -> bytes:
        """Compresses the given data.

        This is an abstract method that must be implemented by a subclass.

        Args:
            data: The bytes to be compressed.

        Returns:
            The compressed data as bytes.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def decompress(self, data: bytes, read_max_bytes: int) -> bytes:
        """Decompresses the given data.

        Args:
            data: The compressed byte string.
            read_max_bytes: The maximum number of bytes to read from the
                decompressed data. This is a safeguard against decompression
                bombs.

        Returns:
            The decompressed byte string.

        Raises:
            NotImplementedError: This method must be implemented by a subclass.
        """
        raise NotImplementedError()


class GZipCompression(Compression):
    """Handles data compression and decompression using the GZip algorithm.

    This class implements the `Compression` interface, providing methods to compress
    and decompress byte data using the standard GZip format.

    Attributes:
        name (str): The identifier for this compression method, 'gzip'.
    """

    _name: str

    def __init__(self) -> None:
        """Initializes the compression algorithm with the GZIP name."""
        self._name = COMPRESSION_GZIP

    @property
    def name(self) -> str:
        """The name of the compression algorithm.

        Returns:
            The name of the compression algorithm.
        """
        return self._name

    def compress(self, data: bytes) -> bytes:
        """Compresses data using gzip.

        Args:
            data: The bytes to be compressed.

        Returns:
            The gzip-compressed data as bytes.
        """
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as f:
            f.write(data)

        return buf.getvalue()

    def decompress(self, data: bytes, read_max_bytes: int) -> bytes:
        """Decompresses a gzip-compressed byte string.

        Args:
            data: The compressed byte string to decompress.
            read_max_bytes: The maximum number of bytes to read from the
                decompressed stream. If this value is zero or negative, the
                entire stream is read.

        Returns:
            The decompressed data as a byte string.
        """
        read_max_bytes = read_max_bytes if read_max_bytes > 0 else -1

        buf = io.BytesIO(data)
        with gzip.GzipFile(fileobj=buf, mode="rb") as f:
            data = f.read(read_max_bytes)

        return data


def get_compression_from_name(name: str | None, compressions: list[Compression]) -> Compression | None:
    """Finds a compression algorithm by its name from a list of available compressions.

    Args:
        name: The name of the compression algorithm to search for.
        compressions: A list of available `Compression` objects.

    Returns:
        The matching `Compression` object if found, otherwise `None`.
    """
    compression = (
        next(
            (compression for compression in compressions if compression.name == name),
            None,
        )
        if name
        else None
    )
    return compression
