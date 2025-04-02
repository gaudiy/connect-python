"""Module implements a server runner for conformance testing."""

import logging
import os
import socket
import ssl
import sys
import tempfile
import threading
import time
from concurrent.futures import as_completed
from typing import cast

import anyio
import hypercorn
import hypercorn.asyncio.run
import hypercorn.typing
from anyio import from_thread

from gen.connectrpc.conformance.v1 import config_pb2
from gen.connectrpc.conformance.v1.server_compat_pb2 import ServerCompatRequest, ServerCompatResponse
from server import app

logger = logging.getLogger("conformance.runner")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("conformance_server.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def find_free_port() -> int:
    """Find and returns a free port on the local machine.

    This function creates a temporary socket, binds it to the loopback address
    ("127.0.0.1") with an ephemeral port (port 0), and retrieves the assigned port
    number. The socket is then closed, making the port available for use.

    Returns:
        int: A free port number on the local machine.

    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def create_ssl_context(request: ServerCompatRequest) -> tuple[str, str, str | None]:
    """Create an SSL context by writing server and client credentials to temporary files.

    Args:
        request (ServerCompatRequest): An object containing server credentials and
                                       optional client TLS certificate.

    Returns:
        tuple[str, str, str | None]: A tuple containing the file paths to the server
                                     certificate, server key, and optionally the client
                                     CA certificate. The third element will be `None`
                                     if no client certificate is provided.

    """
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()

    # Write certificate to file
    cert_path = os.path.join(temp_dir, "cert.pem")
    with open(cert_path, "wb") as f:
        f.write(request.server_creds.cert)

    # Write key to file
    key_path = os.path.join(temp_dir, "key.pem")
    with open(key_path, "wb") as f:
        f.write(request.server_creds.key)

    # If client certificate is required, write it to a file too
    client_ca_path = None
    if request.client_tls_cert:
        client_ca_path = os.path.join(temp_dir, "client_ca.pem")
        with open(client_ca_path, "wb") as f:
            f.write(request.client_tls_cert)

    return cert_path, key_path, client_ca_path


def start_server(request: ServerCompatRequest) -> ServerCompatResponse:
    """Start a server with the specified configuration and returns the server details.

    Args:
        request (ServerCompatRequest): The server compatibility request containing
            configuration details such as HTTP version, TLS usage, and server credentials.

    Returns:
        ServerCompatResponse: A response object containing the server's host, port,
        and optional PEM certificate if TLS is enabled.

    Raises:
        Exception: If an error occurs while starting the server.

    Notes:
        - The server binds to a free port on localhost (127.0.0.1).
        - Supports both HTTP/1.1 and HTTP/2 protocols, with HTTP/2 as the default.
        - Configures TLS if `request.use_tls` is True, using the provided server credentials.
        - Notifies the caller asynchronously after a short delay by writing the response to stdout.
        - Uses `anyio` and `hypercorn` for asynchronous server management.

    """
    # Find a free port
    port = find_free_port()
    host = "127.0.0.1"

    config = hypercorn.Config()
    config.bind = [f"{host}:{port}"]

    if request.http_version == config_pb2.HTTP_VERSION_1:
        config.alpn_protocols = ["http/1.1"]
    else:  # Defaults to HTTP/2
        config.alpn_protocols = ["h2", "http/1.1"]

    # Configure TLS if needed
    if request.use_tls:
        cert_path, key_path, ca_certs_path = create_ssl_context(request)
        config.certfile = cert_path
        config.keyfile = key_path
        if ca_certs_path:
            config.ca_certs = ca_certs_path
            config.verify_mode = ssl.CERT_REQUIRED

    response = ServerCompatResponse(
        host=host, port=port, pem_cert=request.server_creds.cert if request.use_tls else None
    )

    def notify_caller() -> None:
        time.sleep(0.1)
        write_message_to_stdout(response)

    threading.Thread(target=notify_caller).start()

    shutdown_event = anyio.Event()

    async def _start_server(
        config: hypercorn.config.Config,
        app: hypercorn.typing.ASGIFramework,
        shutdown_event: anyio.Event,
    ) -> None:
        if not shutdown_event.is_set():
            await hypercorn.asyncio.serve(app, config, shutdown_trigger=shutdown_event.wait)

    with from_thread.start_blocking_portal() as portal:
        future = portal.start_task_soon(
            _start_server,
            config,
            cast(hypercorn.typing.ASGIFramework, app),
            shutdown_event,
        )

        for f in as_completed([future]):
            try:
                f.result()
            except Exception as e:
                logger.error(f"Error starting server: {e}", exc_info=True)
                raise

    return response


def read_message_from_stdin() -> ServerCompatRequest:
    """Read a serialized ServerCompatRequest message from standard input.

    This function reads a 4-byte integer from stdin to determine the size of the
    incoming message, then reads the specified number of bytes to retrieve the
    serialized message. The message is deserialized into a ServerCompatRequest
    object using the FromString method.

    Returns:
        ServerCompatRequest: The deserialized request object.

    Raises:
        Exception: If an error occurs while reading from stdin or deserializing
        the message, the exception is logged and re-raised.

    """
    try:
        request_size = int.from_bytes(sys.stdin.buffer.read(4), byteorder="big")
        request_buf = sys.stdin.buffer.read(request_size)
        request = ServerCompatRequest.FromString(request_buf)
        return request
    except Exception as e:
        logger.error(f"Error reading message from stdin: {e}", exc_info=True)
        raise


def write_message_to_stdout(response: ServerCompatResponse) -> None:
    """Write a serialized response message to the standard output (stdout) in a specific format.

    The function serializes the given `ServerCompatResponse` object into a byte string,
    calculates its size, and writes both the size (as a 4-byte big-endian integer) and
    the serialized byte string to stdout. This is typically used for inter-process
    communication where the size of the message is sent first to indicate the length
    of the subsequent data.

    Args:
        response (ServerCompatResponse): The response object to be serialized and written to stdout.

    Returns:
        None

    """
    response_buf = response.SerializeToString()
    response_size = len(response_buf)
    sys.stdout.buffer.write(response_size.to_bytes(length=4, byteorder="big"))
    sys.stdout.buffer.write(response_buf)
    sys.stdout.buffer.flush()


def main() -> None:
    """Run the server."""
    try:
        request = read_message_from_stdin()

        start_server(request)

    except EOFError:
        logger.info("EOF reached, stopping server.")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
