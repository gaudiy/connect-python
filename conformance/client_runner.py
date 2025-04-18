"""Module implements a client runner for conformance testing."""

import asyncio
import collections
import logging
import ssl
import struct
import sys
import traceback
from collections.abc import AsyncGenerator
from typing import Any

from connect.connect import StreamRequest, UnaryRequest
from connect.error import ConnectError
from connect.headers import Headers
from connect.options import ClientOptions
from connect.session import AsyncClientSession
from google.protobuf import any_pb2
from google.protobuf.internal.containers import RepeatedCompositeFieldContainer

from gen.connectrpc.conformance.v1 import client_compat_pb2, config_pb2, service_pb2
from gen.connectrpc.conformance.v1.conformancev1connect import service_connect
from tls import new_client_tls_config

logger = logging.getLogger("conformance.runner")


def read_request() -> client_compat_pb2.ClientCompatRequest | None:
    """Read a serialized `ClientCompatRequest` message from standard input.

    This function reads a 4-byte header from standard input to determine the
    length of the serialized message. It then reads the specified number of
    bytes and deserializes the data into a `ClientCompatRequest` object.

    Returns:
        client_compat_pb2.ClientCompatRequest | None: The deserialized
        `ClientCompatRequest` object if data is successfully read and parsed,
        or `None` if no data is available.

    Raises:
        Exception: If the header or message data is incomplete (short read).

    """
    data = sys.stdin.buffer.read(4)
    if not data:
        return None

    if len(data) < 4:
        raise Exception("short read (header)")

    ll = struct.unpack(">I", data)[0]
    msg = client_compat_pb2.ClientCompatRequest()
    data = sys.stdin.buffer.read(ll)
    if len(data) < ll:
        raise Exception("short read (request)")

    msg.ParseFromString(data)
    return msg


def write_response(msg: client_compat_pb2.ClientCompatResponse) -> None:
    """Serialize a ClientCompatResponse message and write it to the standard output.

    The function first serializes the given message into a binary string using
    the `SerializeToString` method. It then calculates the length of the serialized
    data and writes it as a 4-byte big-endian integer to the standard output. Finally,
    it writes the serialized data itself and flushes the output buffer.

    Args:
        msg (client_compat_pb2.ClientCompatResponse): The protocol buffer message
            to be serialized and written to the standard output.

    Returns:
        None

    """
    data = msg.SerializeToString()
    ll = struct.pack(">I", len(data))
    sys.stdout.buffer.write(ll)
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()


async def unpack_requests(request_messages: RepeatedCompositeFieldContainer[any_pb2.Any]) -> AsyncGenerator[Any]:
    """Asynchronously unpacks a sequence of protobuf Any messages into their respective request types.

    This function iterates over a collection of `Any` protobuf messages, determines their
    corresponding request type based on the `TypeName` field, unpacks them into the appropriate
    message type, and yields the unpacked request objects.

    Args:
        request_messages (RepeatedCompositeFieldContainer[any_pb2.Any]): A collection of protobuf
            `Any` messages to be unpacked.

    Yields:
        Any: The unpacked request object of the appropriate type.

    Raises:
        KeyError: If the `TypeName` of an `Any` message does not match any of the predefined
            request types in the `req_types` mapping.

    """
    for any in request_messages:
        req_types = {
            "connectrpc.conformance.v1.IdempotentUnaryRequest": service_pb2.IdempotentUnaryRequest,
            "connectrpc.conformance.v1.UnaryRequest": service_pb2.UnaryRequest,
            "connectrpc.conformance.v1.UnimplementedRequest": service_pb2.UnimplementedRequest,
            "connectrpc.conformance.v1.ServerStreamRequest": service_pb2.ServerStreamRequest,
            "connectrpc.conformance.v1.ClientStreamRequest": service_pb2.ClientStreamRequest,
            "connectrpc.conformance.v1.BidiStreamRequest": service_pb2.BidiStreamRequest,
        }

        req_type = req_types[any.TypeName()]
        req = req_type()
        any.Unpack(req)
        yield req


def to_pb_headers(headers: Headers) -> list[service_pb2.Header]:
    """Convert a Headers object into a list of service_pb2.Header objects.

    Args:
        headers (Headers): A collection of headers where each key is a string
            and each value is a string representing the header value.

    Returns:
        list[service_pb2.Header]: A list of service_pb2.Header objects, where
            each object contains a header name and a list of its associated values.

    """
    h_dict: dict[str, list[str]] = collections.defaultdict(list)
    for key, value in headers.items():
        h_dict[key].append(value)

    return [
        service_pb2.Header(
            name=key,
            value=values,
        )
        for key, values in h_dict.items()
    ]


async def handle_message(msg: client_compat_pb2.ClientCompatRequest) -> client_compat_pb2.ClientCompatResponse:
    """Handle a client compatibility request and returns a response.

    This asynchronous function processes a `ClientCompatRequest` message, performs
    the necessary HTTP or gRPC calls based on the provided configuration, and
    returns a `ClientCompatResponse` message.

    Args:
        msg (client_compat_pb2.ClientCompatRequest): The client compatibility
            request containing details such as HTTP version, TLS configuration,
            request headers, stream type, and other parameters.

    Returns:
        client_compat_pb2.ClientCompatResponse: The response containing the test
            name, payloads, HTTP status code, response headers, response trailers,
            or error details if an exception occurs.

    Raises:
        ValueError: If the provided stream type is unsupported.

    Behavior:
        - Determines the HTTP version (HTTP/1.1 or HTTP/2) based on the request.
        - Configures TLS settings if server or client certificates are provided.
        - Constructs the base URL for the request.
        - Introduces a delay if `request_delay_ms` is specified.
        - Handles different stream types (unary, client-streaming, server-streaming,
          full-duplex, half-duplex) and processes the request accordingly.
        - Captures and returns errors in the response if exceptions occur.

    Note:
        - This function uses an asynchronous HTTP client session (`AsyncClientSession`)
          for making requests.
        - Compression (e.g., gzip) is applied if specified in the request.
        - Headers and trailers are converted to protobuf-compatible formats.

    """
    reqs = unpack_requests(msg.request_messages)
    http1 = msg.http_version in [
        config_pb2.HTTP_VERSION_1,
        config_pb2.HTTP_VERSION_UNSPECIFIED,
    ]
    http2 = msg.http_version in [
        config_pb2.HTTP_VERSION_2,
        config_pb2.HTTP_VERSION_UNSPECIFIED,
    ]

    if msg.server_tls_cert:
        if msg.client_tls_creds:
            ssl_context = new_client_tls_config(
                msg.server_tls_cert, msg.client_tls_creds.cert, msg.client_tls_creds.key
            )
        else:
            ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ssl_context.load_verify_locations(cadata=msg.server_tls_cert.decode("utf-8"))

        proto = "https"
    else:
        ssl_context = None
        proto = "http"

    url = f"{proto}://{msg.host}:{msg.port}"

    async with AsyncClientSession(http1=http1, http2=http2, ssl_context=ssl_context) as session:
        payloads = []
        try:
            options = ClientOptions()
            if msg.compression == config_pb2.COMPRESSION_GZIP:
                options.request_compression_name = "gzip"

            client = service_connect.ConformanceServiceClient(base_url=url, session=session, options=options)
            if msg.stream_type == config_pb2.STREAM_TYPE_UNARY:
                if msg.request_delay_ms > 0:
                    await asyncio.sleep(msg.request_delay_ms / 1000.0)

                abort_event = asyncio.Event()
                req = await anext(reqs)

                if msg.HasField("cancel") and msg.cancel.HasField("after_close_send_ms"):

                    async def delayed_abort() -> None:
                        await asyncio.sleep(msg.cancel.after_close_send_ms / 1000)
                        abort_event.set()

                    asyncio.create_task(delayed_abort())

                header = Headers()
                for h in msg.request_headers:
                    if key := header.get(h.name.lower()):
                        header[key] = f"{header[key]}, {', '.join(h.value)}"
                    else:
                        header[h.name.lower()] = ", ".join(h.value)

                resp = await getattr(client, msg.method)(
                    UnaryRequest(
                        message=req,
                        headers=header,
                        timeout=msg.timeout_ms / 1000,
                        abort_event=abort_event,
                    ),
                )
                payloads.append(resp.message.payload)

                return client_compat_pb2.ClientCompatResponse(
                    test_name=msg.test_name,
                    response=client_compat_pb2.ClientResponseResult(
                        payloads=payloads,
                        http_status_code=200,
                        response_headers=to_pb_headers(resp.headers),
                        response_trailers=to_pb_headers(resp.trailers),
                    ),
                )
            elif msg.stream_type == config_pb2.STREAM_TYPE_CLIENT_STREAM:
                abort_event = asyncio.Event()
                header = Headers()
                for h in msg.request_headers:
                    if key := header.get(h.name.lower()):
                        header[key] = f"{header[key]}, {', '.join(h.value)}"
                    else:
                        header[h.name.lower()] = ", ".join(h.value)

                async def _reqs() -> AsyncGenerator[service_pb2.ClientStreamRequest]:
                    async for req in reqs:
                        if msg.request_delay_ms > 0:
                            await asyncio.sleep(msg.request_delay_ms / 1000.0)
                        yield req

                    if msg.HasField("cancel") and msg.cancel.HasField("before_close_send"):
                        abort_event.set()
                    elif msg.HasField("cancel") and msg.cancel.HasField("after_close_send_ms"):

                        async def delayed_abort() -> None:
                            await asyncio.sleep(msg.cancel.after_close_send_ms / 1000)
                            abort_event.set()

                        asyncio.create_task(delayed_abort())

                async with getattr(client, msg.method)(
                    StreamRequest(
                        messages=_reqs(), headers=header, timeout=msg.timeout_ms / 1000, abort_event=abort_event
                    ),
                ) as resp:
                    async for message in resp.messages:
                        payloads.append(message.payload)

                return client_compat_pb2.ClientCompatResponse(
                    test_name=msg.test_name,
                    response=client_compat_pb2.ClientResponseResult(
                        payloads=payloads,
                        http_status_code=200,
                        response_headers=to_pb_headers(resp.headers),
                        response_trailers=to_pb_headers(resp.trailers),
                    ),
                )
            elif msg.stream_type == config_pb2.STREAM_TYPE_SERVER_STREAM:
                abort_event = asyncio.Event()
                if msg.request_delay_ms > 0:
                    await asyncio.sleep(msg.request_delay_ms / 1000.0)

                header = Headers()
                for h in msg.request_headers:
                    if key := header.get(h.name.lower()):
                        header[key] = f"{header[key]}, {', '.join(h.value)}"
                    else:
                        header[h.name.lower()] = ", ".join(h.value)

                async with getattr(client, msg.method)(
                    StreamRequest(
                        messages=reqs, headers=header, timeout=msg.timeout_ms / 1000, abort_event=abort_event
                    ),
                ) as resp:
                    if msg.HasField("cancel") and msg.cancel.HasField("after_close_send_ms"):

                        async def delayed_abort() -> None:
                            await asyncio.sleep(msg.cancel.after_close_send_ms / 1000)
                            abort_event.set()

                        asyncio.create_task(delayed_abort())

                    async for message in resp.messages:
                        payloads.append(message.payload)
                        if len(payloads) == msg.cancel.after_num_responses:
                            abort_event.set()

                return client_compat_pb2.ClientCompatResponse(
                    test_name=msg.test_name,
                    response=client_compat_pb2.ClientResponseResult(
                        payloads=payloads,
                        http_status_code=200,
                        response_headers=to_pb_headers(resp.headers),
                        response_trailers=to_pb_headers(resp.trailers),
                    ),
                )

            elif (
                msg.stream_type == config_pb2.STREAM_TYPE_FULL_DUPLEX_BIDI_STREAM
                or msg.stream_type == config_pb2.STREAM_TYPE_HALF_DUPLEX_BIDI_STREAM
            ):
                abort_event = asyncio.Event()
                if msg.request_delay_ms > 0:
                    await asyncio.sleep(msg.request_delay_ms / 1000.0)

                header = Headers()
                for h in msg.request_headers:
                    if key := header.get(h.name.lower()):
                        header[key] = f"{header[key]}, {', '.join(h.value)}"
                    else:
                        header[h.name.lower()] = ", ".join(h.value)

                async with getattr(client, msg.method)(
                    StreamRequest(
                        messages=reqs, headers=header, timeout=msg.timeout_ms / 1000, abort_event=abort_event
                    ),
                ) as resp:
                    if msg.HasField("cancel") and msg.cancel.HasField("before_close_send"):
                        abort_event.set()

                    if msg.HasField("cancel") and msg.cancel.HasField("after_close_send_ms"):

                        async def delayed_abort() -> None:
                            await asyncio.sleep(msg.cancel.after_close_send_ms / 1000)
                            abort_event.set()

                        asyncio.create_task(delayed_abort())

                    if (
                        msg.HasField("cancel")
                        and msg.cancel.HasField("after_num_responses")
                        and msg.cancel.after_num_responses == 0
                    ):
                        abort_event.set()

                    async for message in resp.messages:
                        payloads.append(message.payload)
                        if len(payloads) == msg.cancel.after_num_responses:
                            abort_event.set()

                return client_compat_pb2.ClientCompatResponse(
                    test_name=msg.test_name,
                    response=client_compat_pb2.ClientResponseResult(
                        payloads=payloads,
                        http_status_code=200,
                        response_headers=to_pb_headers(resp.headers),
                        response_trailers=to_pb_headers(resp.trailers),
                    ),
                )
            else:
                raise ValueError(f"Unsupported stream type: {msg.stream_type}")

        except ConnectError as e:
            return client_compat_pb2.ClientCompatResponse(
                test_name=msg.test_name,
                response=client_compat_pb2.ClientResponseResult(
                    payloads=payloads,
                    error=service_pb2.Error(
                        code=getattr(config_pb2, f"CODE_{e.code.name.upper()}"),
                        message=e.raw_message,
                        details=[d.pb_any for d in e.details],
                    ),
                    http_status_code=200,
                    response_headers=to_pb_headers(e.metadata),
                    response_trailers=to_pb_headers(e.metadata),
                ),
            )

        except Exception as e:
            return client_compat_pb2.ClientCompatResponse(
                test_name=msg.test_name,
                error=client_compat_pb2.ClientErrorResult(message=str(e)),
            )


if __name__ == "__main__":
    if "--debug" in sys.argv:
        logging.debug("Debug mode enabled")

    async def run_message(req: client_compat_pb2.ClientCompatRequest) -> None:
        """Run the message handler for a given request."""
        try:
            resp = await handle_message(req)
        except Exception as e:
            resp = client_compat_pb2.ClientCompatResponse(
                test_name=req.test_name,
                error=client_compat_pb2.ClientErrorResult(message="".join(traceback.format_exception(e))),
            )

        write_response(resp)

    async def read_requests() -> None:
        """Read requests from standard input and process them asynchronously."""
        loop = asyncio.get_event_loop()
        while req := await loop.run_in_executor(None, read_request):
            loop.create_task(run_message(req))

    asyncio.run(read_requests())
