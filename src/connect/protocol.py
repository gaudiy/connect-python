"""Module defining the protocol handling classes and functions."""

import abc
from http import HTTPMethod

from pydantic import BaseModel, ConfigDict
from starlette.datastructures import MutableHeaders

from connect.code import Code
from connect.codec import ReadOnlyCodecs
from connect.compression import COMPRESSION_IDENTITY, Compression
from connect.connect import Spec, StreamingHandlerConn
from connect.error import ConnectError
from connect.request import Request

PROTOCOL_CONNECT = "connect"

HEADER_CONTENT_TYPE = "content-type"
HEADER_CONTENT_LENGTH = "content-length"
HEADER_HOST = "host"


class ProtocolHandlerParams(BaseModel):
    """ProtocolHandlerParams is a data model that holds parameters for handling protocol operations.

    Attributes:
        spec (Spec): The specification details for the protocol.
        codecs (ReadOnlyCodecs): The codecs used for encoding and decoding data.
        compressions (list[Compression]): A list of compression methods to be used.
        compress_min_bytes (int): The minimum number of bytes required to trigger compression.
        read_max_bytes (int): The maximum number of bytes that can be read at once.
        send_max_bytes (int): The maximum number of bytes that can be sent at once.

    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    spec: Spec
    codecs: ReadOnlyCodecs
    compressions: list[Compression]
    compress_min_bytes: int
    read_max_bytes: int
    send_max_bytes: int
    require_connect_protocol_header: bool


class ProtocolHandler(abc.ABC):
    """Abstract base class for handling different protocols."""

    @abc.abstractmethod
    def methods(self) -> list[HTTPMethod]:
        """Retrieve a list of HTTP methods.

        Returns:
            list[HTTPMethod]: A list of HTTP methods.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def content_types(self) -> list[str]:
        """Handle content types.

        This method currently does nothing and is intended to be implemented
        in the future to handle different content types as needed.

        Returns:
            None

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def can_handle_payload(self, request: Request, content_type: str) -> bool:
        """Determine if the payload of the given request can be handled based on the content type.

        Args:
            request (Request): The request object containing the payload.
            content_type (str): The content type of the payload.

        Returns:
            bool: True if the payload can be handled, False otherwise.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def conn(
        self, request: Request, response_headers: MutableHeaders, response_trailers: MutableHeaders
    ) -> StreamingHandlerConn:
        """Handle the connection for a given request and response headers.

        Args:
            request (Request): The request object containing the details of the request.
            response_headers (MutableHeaders): The mutable headers for the response.
            response_trailers (MutableHeaders): The mutable headers for the response trailers.

        Returns:
            StreamingHandlerConn: The connection handler for streaming.

        """
        raise NotImplementedError()


class Protocol(abc.ABC):
    """Abstract base class for defining a protocol.

    This class serves as a blueprint for creating protocol handlers and clients.
    Subclasses must implement the following abstract methods.

    """

    @abc.abstractmethod
    def handler(self, params: ProtocolHandlerParams) -> ProtocolHandler:
        """Handle the protocol with the given parameters.

        Args:
            params (ProtocolHandlerParams): The parameters required to handle the protocol.

        Returns:
            ProtocolHandler: An instance of ProtocolHandler based on the provided parameters.

        """
        raise NotImplementedError()

    @abc.abstractmethod
    def client(self) -> None:
        """Implement client functionality.

        This method currently does nothing and is intended to be implemented
        in the future with the necessary client-side logic.
        """
        raise NotImplementedError()


def mapped_method_handlers(handlers: list[ProtocolHandler]) -> dict[HTTPMethod, list[ProtocolHandler]]:
    """Map protocol handlers to their respective HTTP methods.

    Args:
        handlers (list[ProtocolHandler]): A list of protocol handlers.

    Returns:
        dict[HTTPMethod, list[ProtocolHandler]]: A dictionary where the keys are HTTP methods and the values are lists of protocol handlers that support those methods.

    """
    method_handlers: dict[HTTPMethod, list[ProtocolHandler]] = {}
    for handler in handlers:
        for method in handler.methods():
            method_handlers.setdefault(method, []).append(handler)

    return method_handlers


def negotiate_compression(
    available: list[Compression], sent: str | None, accept: str | None
) -> tuple[Compression | None, Compression | None]:
    """Negotiate the compression method to be used based on the available options.

    The compression method sent by the client, and the compression methods accepted
    by the server.

    Args:
        available (list[Compression]): A list of available compression methods.
        sent (str | None): The compression method sent by the client, or None if not specified.
        accept (str | None): A comma-separated string of compression methods accepted by the server, or None if not specified.
        header_name_accept_encoding (str): The name of the header used to specify the accepted compression methods.

    Returns:
        tuple[Compression | None, Compression | None]: A tuple containing the selected compression method for the request
        and the response. If no suitable compression method is found, None is returned for that position in the tuple.

    """
    request = None
    response = None

    if sent is not None and sent != COMPRESSION_IDENTITY:
        found = next((c for c in available if c.name() == sent), None)
        if found:
            request = found
        else:
            raise ConnectError(
                f"unknown compression {sent}: supported encodings are {available}",
                Code.UNIMPLEMENTED,
            )

    if accept is None or accept == "":
        response = request
    else:
        accept_names = [name.strip() for name in accept.split(",")]
        for name in accept_names:
            found = next((c for c in available if c.name() == name), None)
            if found:
                response = found
                break

    return request, response


def sorted_allow_method_value(handlers: list[ProtocolHandler]) -> str:
    """Sort the allowed methods for a list of protocol handlers.

    Args:
        handlers (list[ProtocolHandler]): A list of protocol handlers.

    Returns:
        str: A comma-separated string of the allowed methods.

    """
    methods = {method for handler in handlers for method in handler.methods()}
    return ", ".join(sorted(method.value for method in methods))


def sorted_accept_post_value(handlers: list[ProtocolHandler]) -> str:
    """Sort the allowed methods for a list of protocol handlers.

    Args:
        handlers (list[ProtocolHandler]): A list of protocol handlers.

    Returns:
        str: A comma-separated string of the allowed methods.

    """
    # methods = {method for handler in handlers for method in handler.methods()}
    content_types = {content_type for handler in handlers for content_type in handler.content_types()}
    return ", ".join(sorted(content_type for content_type in content_types))
