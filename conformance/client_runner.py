import asyncio
import collections
import logging
import ssl
import struct
import sys
import time
import traceback
from collections.abc import AsyncGenerator
from typing import Any

from gen.connectrpc.conformance.v1 import client_compat_pb2, config_pb2, service_pb2
from gen.connectrpc.conformance.v1.conformancev1connect import service_connect
from google.protobuf import any_pb2
from google.protobuf.internal.containers import RepeatedCompositeFieldContainer
from tls import new_client_tls_config

from connect.connect import StreamRequest, UnaryRequest
from connect.error import ConnectError
from connect.headers import Headers
from connect.options import ClientOptions
from connect.session import AsyncClientSession

logger = logging.getLogger("conformance.runner")


def read_request() -> client_compat_pb2.ClientCompatRequest | None:
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
    data = msg.SerializeToString()
    ll = struct.pack(">I", len(data))
    sys.stdout.buffer.write(ll)
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()


async def unpack_requests(request_messages: RepeatedCompositeFieldContainer[any_pb2.Any]) -> AsyncGenerator[Any]:
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

    if msg.request_delay_ms > 0:
        time.sleep(msg.request_delay_ms / 1000.0)

    async with AsyncClientSession(http1=http1, http2=http2, ssl_context=ssl_context) as session:
        payloads = []
        try:
            options = ClientOptions()
            if msg.compression == config_pb2.COMPRESSION_GZIP:
                options.request_compression_name = "gzip"

            client = service_connect.ConformanceServiceClient(base_url=url, session=session, options=options)
            if msg.stream_type == config_pb2.STREAM_TYPE_UNARY:
                req = await anext(reqs)

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
            elif (
                msg.stream_type == config_pb2.STREAM_TYPE_CLIENT_STREAM
                or msg.stream_type == config_pb2.STREAM_TYPE_SERVER_STREAM
                or msg.stream_type == config_pb2.STREAM_TYPE_FULL_DUPLEX_BIDI_STREAM
                or msg.stream_type == config_pb2.STREAM_TYPE_HALF_DUPLEX_BIDI_STREAM
            ):
                header = Headers()
                for h in msg.request_headers:
                    if key := header.get(h.name.lower()):
                        header[key] = f"{header[key]}, {', '.join(h.value)}"
                    else:
                        header[h.name.lower()] = ", ".join(h.value)

                resp = await getattr(client, msg.method)(
                    StreamRequest(
                        messages=reqs,
                        headers=header,
                        timeout=msg.timeout_ms / 1000,
                    ),
                )

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

    loop = asyncio.new_event_loop()

    async def run_message(req: client_compat_pb2.ClientCompatRequest) -> None:
        try:
            resp = await handle_message(req)
        except Exception as e:
            resp = client_compat_pb2.ClientCompatResponse(
                test_name=req.test_name,
                error=client_compat_pb2.ClientErrorResult(message="".join(traceback.format_exception(e))),
            )

        write_response(resp)

    async def read_requests() -> None:
        while req := await loop.run_in_executor(None, read_request):
            loop.create_task(run_message(req))

    loop.run_until_complete(read_requests())
    logger.info("All done")
