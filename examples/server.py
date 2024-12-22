from connect.request import Request
from connect.response import Response


class PingServiceHandler:
    def ping(self, request: Request) -> Response:
        raise NotImplementedError("Method not implemented")


class PingService(PingServiceHandler):
    def ping(self, request: Request) -> Response:
        return Response("Pong", status_code=200)
