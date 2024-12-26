"""Provides classes and functions for handling protocol connections.

Handles data serialization/deserialization using the Connect protocol.
"""

from typing import Any

from connect.codec import Codec
from connect.connect import Spec, StreamingHandlerConn, StreamType
from connect.protocol import HttpMethod, Protocol, ProtocolHandler, ProtocolHandlerParams
from connect.request import Request

CONNECT_UNARY_HEADER_COMPRESSION = "content-Encoding"
CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION = "accept-Encoding"
CONNECT_UNARY_CONTENT_TYPE_PREFIX = "application/"
CONNECT_STREAMING_CONTENT_TYPE_PREFIX = "application/connect+"
CONNECT_UNARY_COMPRESSION_QUERY_PARAMETER = "compression"


def connect_codec_from_content_type(stream_type: StreamType, content_type: str) -> str:
    """Extract the codec from the content type based on the stream type.

    Args:
        stream_type (StreamType): The type of stream (Unary or Streaming).
        content_type (str): The content type string from which to extract the codec.

    Returns:
        str: The extracted codec from the content type.

    """
    if stream_type == StreamType.Unary:
        return content_type[len(CONNECT_UNARY_CONTENT_TYPE_PREFIX) :]

    return content_type[len(CONNECT_STREAMING_CONTENT_TYPE_PREFIX) :]


class ConnectHandler(ProtocolHandler):
    """A handler for managing protocol connections.

    Attributes:
        params (ProtocolHandlerParams): Parameters for the protocol handler.
        __methods (list[HttpMethod]): List of HTTP methods supported by the handler.
        accept (list[str]): List of accepted content types.

    """

    params: ProtocolHandlerParams
    __methods: list[HttpMethod]
    accept: list[str]

    def __init__(self, params: ProtocolHandlerParams, methods: list[HttpMethod], accept: list[str]) -> None:
        """Initialize the ProtocolConnect instance.

        Args:
            params (ProtocolHandlerParams): The parameters for the protocol handler.
            methods (list[HttpMethod]): A list of HTTP methods.
            accept (list[str]): A list of accepted content types.

        """
        self.params = params
        self.__methods = methods
        self.accept = accept

    def methods(self) -> list[HttpMethod]:
        """Return the list of HTTP methods.

        Returns:
            list[HttpMethod]: A list of HTTP methods.

        """
        return self.__methods

    def content_types(self) -> None:
        """Handle content types.

        This method currently does nothing and serves as a placeholder for future
        implementation related to content types.

        """
        pass

    def can_handle_payload(self, request: Request, content_type: str) -> bool:
        """Check if the handler can handle the payload."""
        if HttpMethod(request.method) == HttpMethod.GET:
            pass

        return content_type in self.accept

    async def conn(self, request: Request) -> StreamingHandlerConn:
        """Handle the connection for the given request and return a StreamingHandlerConn object.

        Args:
            request (Request): The incoming request object.

        Returns:
            StreamingHandlerConn: The connection handler for the request.

        Raises:
            ValueError: If the request method is not supported or if the codec cannot be determined.

        Note:
            - For Unary stream type, handles GET and other HTTP methods differently.
            - Retrieves compression and encoding information from request headers.
            - Parses the request body and determines the codec based on content type.
            - Currently, only Unary stream type is supported. Streaming support is to be added.

        """
        _query = request.url.query
        if self.params.spec.stream_type == StreamType.Unary:
            if HttpMethod(request.method) == HttpMethod.GET:
                # TODO(tsubakiky): Get the compression from the query parameter
                pass
            else:
                _content_encoding = request.headers.get(CONNECT_UNARY_HEADER_COMPRESSION, "")

            _accept_encoding = request.headers.get(CONNECT_UNARY_HEADER_ACCEPT_COMPRESSION, "")
        # TODO(tsubakiky): Add validations

        request_body: bytes
        if HttpMethod(request.method) == HttpMethod.GET:
            pass
        else:
            request_body = await request.body()
            content_type = request.headers.get("content-type", "")
            codec_name = connect_codec_from_content_type(self.params.spec.stream_type, content_type)

        codec = self.params.codecs.get(codec_name)
        if self.params.spec.stream_type == StreamType.Unary:
            conn = ConnectUnaryHandlerConn(
                marshaler=ConnectMarshaler(codec=codec), unmarshaler=ConnectUnmarshaler(body=request_body, codec=codec)
            )
        else:
            # TODO(tsubakiky): Add streaming support
            pass

        return conn


class ProtocolConnect(Protocol):
    """ProtocolConnect is a class that implements the Protocol interface for handling connection protocols."""

    def __init__(self) -> None:
        """Initialize the class instance."""
        pass

    def handler(self, params: ProtocolHandlerParams) -> ConnectHandler:
        """Handle the creation of a ConnectHandler based on the provided ProtocolHandlerParams.

        Args:
            params (ProtocolHandlerParams): The parameters required to create the ConnectHandler.

        Returns:
            ConnectHandler: An instance of ConnectHandler configured with the appropriate methods and content types.

        """
        methods = [HttpMethod.POST]

        if params.spec.stream_type == StreamType.Unary:
            methods.append(HttpMethod.GET)

        content_types: list[str] = []
        for name in params.codecs.names():
            if params.spec.stream_type == StreamType.Unary:
                content_types.append(CONNECT_UNARY_CONTENT_TYPE_PREFIX + name)
                continue

            content_types.append(CONNECT_STREAMING_CONTENT_TYPE_PREFIX + name)

        return ConnectHandler(params, methods=methods, accept=content_types)

    def client(self) -> None:
        """Handle the client connection."""
        pass


class ConnectUnmarshaler:
    """A class to handle the unmarshaling of data using a specified codec.

    Attributes:
        codec (Codec): The codec used for unmarshaling the data.
        body (bytes): The raw data to be unmarshaled.

    """

    codec: Codec
    body: bytes

    def __init__(self, body: bytes, codec: Codec) -> None:
        """Initialize the ProtocolConnect object.

        Args:
            body (bytes): The body of the protocol message.
            codec (Codec): The codec used for encoding/decoding the message.

        """
        self.body = body
        self.codec = codec

    def unmarshal(self, message: Any) -> Any:
        """Unmarshals the given message using the codec.

        Args:
            message (Any): The message to be unmarshaled.

        Returns:
            Any: The unmarshaled object.

        """
        obj = self.codec.unmarshal(self.body, message)
        return obj


class ConnectMarshaler:
    """A class responsible for marshaling messages using a specified codec.

    Attributes:
        codec (Codec): The codec used for marshaling messages.

    """

    codec: Codec

    def __init__(self, codec: Codec) -> None:
        """Initialize the ConnectMarshaler with the given codec.

        Args:
            codec (Codec): The codec used for marshaling messages.

        """
        self.codec = codec

    def marshal(self, message: Any) -> bytes:
        """Serialize the given message into bytes using the codec's marshal method.

        Args:
            message (Any): The message to be serialized.

        Returns:
            bytes: The serialized message in bytes.

        """
        return self.codec.marshal(message)


class ConnectUnaryHandlerConn(StreamingHandlerConn):
    """ConnectUnaryHandlerConn is a handler connection class for unary RPCs in the Connect protocol.

    Attributes:
        marshaler (ConnectMarshaler): An instance of ConnectMarshaler used to marshal messages.
        unmarshaler (ConnectUnmarshaler): An instance of ConnectUnmarshaler used to unmarshal messages.

    """

    marshaler: ConnectMarshaler
    unmarshaler: ConnectUnmarshaler

    def __init__(self, marshaler: ConnectMarshaler, unmarshaler: ConnectUnmarshaler) -> None:
        """Initialize the protocol connection with the given marshaler and unmarshaler.

        Args:
            marshaler (ConnectMarshaler): The marshaler to serialize data.
            unmarshaler (ConnectUnmarshaler): The unmarshaler to deserialize data.

        """
        self.marshaler = marshaler
        self.unmarshaler = unmarshaler

    def spec(self) -> Spec:
        """Retrieve the specification for the protocol.

        This method should be implemented by subclasses to provide the specific
        details of the protocol's specification.

        Returns:
            Spec: The specification of the protocol.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.

        """
        raise NotImplementedError

    def peer(self) -> Any:
        """Return the peer information.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.

        Returns:
            Any: The peer information.

        """
        raise NotImplementedError

    def receive(self, message: Any) -> Any:
        """Receives a message, unmarshals it, and returns the resulting object.

        Args:
            message (Any): The message to be unmarshaled.

        Returns:
            Any: The unmarshaled object.

        """
        obj = self.unmarshaler.unmarshal(message)
        return obj

    def request_header(self) -> Any:
        """Generate and return the request header.

        Returns:
            Any: The request header.

        """
        pass

    def send(self, message: Any) -> bytes:
        """Send a message by marshaling it into bytes.

        Args:
            message (Any): The message to be sent.

        Returns:
            bytes: The marshaled message in bytes.

        """
        data = self.marshaler.marshal(message)
        return data

    def response_header(self) -> Any:
        """Retrieve the response header.

        Returns:
            Any: The response header.

        """
        pass

    def response_trailer(self) -> Any:
        """Handle response trailers.

        This method is intended to be overridden in subclasses to provide
        specific functionality for processing response trailers.

        Returns:
            Any: The processed response trailer data.

        """
        pass
