"""Module defining the protocol handling classes and functions."""

import abc
from enum import Enum

from pydantic import BaseModel, ConfigDict

from connect.codec import ReadOnlyCodecs
from connect.connect import Spec, StreamingHandlerConn
from connect.request import Request

HEADER_CONTENT_TYPE = "content-type"
HEADER_HOST = "host"


class HttpMethod(Enum):
    """Enum representing HTTP methods.

    Attributes:
        GET (str): The GET method requests a representation of the specified resource. Requests using GET should only retrieve data.
        POST (str): The POST method submits an entity to the specified resource, often causing a change in state or side effects on the server.
        PUT (str): The PUT method replaces all current representations of the target resource with the request payload.
        DELETE (str): The DELETE method deletes the specified resource.
        PATCH (str): The PATCH method is used to apply partial modifications to a resource.
        OPTIONS (str): The OPTIONS method is used to describe the communication options for the target resource.
        HEAD (str): The HEAD method asks for a response identical to that of a GET request, but without the response body.

    """

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    OPTIONS = "OPTIONS"
    HEAD = "HEAD"


class ProtocolHandlerParams(BaseModel):
    """ProtocolHandlerParams is a data model that defines the parameters for handling protocols.

    Attributes:
        spec (Spec): The specification object that defines the protocol details.
        codecs (ReadOnlyCodecs): The codecs used for encoding and decoding data in the protocol.

    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    spec: Spec
    codecs: ReadOnlyCodecs


class ProtocolHandler(abc.ABC):
    """Abstract base class for handling different protocols."""

    @abc.abstractmethod
    def methods(self) -> list[HttpMethod]:
        """Retrieve a list of HTTP methods.

        Returns:
            list[HttpMethod]: A list of HTTP methods.

        """
        pass

    @abc.abstractmethod
    def content_types(self) -> None:
        """Handle content types.

        This method currently does nothing and is intended to be implemented
        in the future to handle different content types as needed.

        Returns:
            None

        """
        pass

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
        raise NotImplementedError

    @abc.abstractmethod
    async def conn(self, request: Request) -> StreamingHandlerConn:
        """Handle an incoming connection request.

        Args:
            request (Request): The incoming connection request.

        Returns:
            StreamingHandlerConn: The connection handler for streaming.

        """
        pass


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
        pass

    @abc.abstractmethod
    def client(self) -> None:
        """Implement client functionality.

        This method currently does nothing and is intended to be implemented
        in the future with the necessary client-side logic.
        """
        pass


def mapped_method_handlers(handlers: list[ProtocolHandler]) -> dict[HttpMethod, list[ProtocolHandler]]:
    """Map protocol handlers to their respective HTTP methods.

    Args:
        handlers (list[ProtocolHandler]): A list of protocol handlers.

    Returns:
        dict[HttpMethod, list[ProtocolHandler]]: A dictionary where the keys are HTTP methods and the values are lists of protocol handlers that support those methods.

    """
    method_handlers: dict[HttpMethod, list[ProtocolHandler]] = {}
    for handler in handlers:
        for method in handler.methods():
            method_handlers.setdefault(method, []).append(handler)

    return method_handlers
