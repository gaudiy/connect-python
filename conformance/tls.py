import os
import ssl
import tempfile

import cryptography.x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key


def new_client_tls_config(ca_cert: bytes, client_cert: bytes, client_key: bytes) -> ssl.SSLContext:
    if not ca_cert:
        raise ValueError("ca_cert is empty")

    with tempfile.NamedTemporaryFile(delete=False) as ca_temp:
        ca_temp.write(ca_cert)
        ca_temp_name = ca_temp.name

    try:
        cryptography.x509.load_pem_x509_certificate(ca_cert, default_backend())
    except Exception as e:
        os.unlink(ca_temp_name)
        raise ValueError("failed to parse CA cert from given data") from e

    has_client_cert = len(client_cert) != 0
    has_client_key = len(client_key) != 0

    cert_temp_name: str | None = None
    key_temp_name: str | None = None

    if has_client_cert and has_client_key:
        with tempfile.NamedTemporaryFile(delete=False) as cert_temp:
            cert_temp.write(client_cert)
            cert_temp_name = cert_temp.name

        with tempfile.NamedTemporaryFile(delete=False) as key_temp:
            key_temp.write(client_key)
            key_temp_name = key_temp.name

        try:
            cryptography.x509.load_pem_x509_certificate(client_cert, default_backend())
            load_pem_private_key(client_key, password=None, backend=default_backend())
        except Exception as e:
            os.unlink(ca_temp_name)
            if cert_temp_name:
                os.unlink(cert_temp_name)

            if key_temp_name:
                os.unlink(key_temp_name)

            raise ValueError(f"failed to parse client cert/key: {str(e)}") from e

    elif has_client_cert:
        os.unlink(ca_temp_name)
        raise ValueError("client_cert is not empty but client_key is")

    elif has_client_key:
        os.unlink(ca_temp_name)
        raise ValueError("client_key is not empty but client_cert is")

    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.load_verify_locations(cafile=ca_temp_name)

    if has_client_cert and has_client_key and cert_temp_name and key_temp_name:
        context.load_cert_chain(certfile=cert_temp_name, keyfile=key_temp_name)

        os.unlink(cert_temp_name)
        os.unlink(key_temp_name)

    context.minimum_version = ssl.TLSVersion.TLSv1_2

    os.unlink(ca_temp_name)

    return context
