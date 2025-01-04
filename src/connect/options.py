"""Options for the UniversalHandler class."""

from pydantic import BaseModel, ConfigDict

from connect.interceptor import Interceptor


class UniversalHandlerOptions(BaseModel):
    """Options for the UniversalHandler class."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    interceptors: list[Interceptor]


class ConnectOptions(UniversalHandlerOptions):
    """Options for the connect command."""

    ...
