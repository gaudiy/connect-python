"""Options for the connect command."""

from connect.options import UniversalHandlerOptions


class ConnectRouterOptions(UniversalHandlerOptions):
    """Options for the connect router command."""

    connect: bool | None = None
