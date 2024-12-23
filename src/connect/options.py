"""Options for the UniversalHandler class."""

from pydantic import BaseModel

from connect.interceptor import Interceptor


class UniversalHandlerOptions(BaseModel):
    """Options for the UniversalHandler class."""

    interceptors: list[Interceptor]


class ConnectOptions(UniversalHandlerOptions):
    """Options for the connect command."""

    ...
