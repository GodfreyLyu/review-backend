"""The Thrift stack: TCP socket -> bounded framed transport -> binary protocol."""

from llm_contract.generated import LlmService
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from app.errors import InferenceUnavailable
from app.inference.settings import InferenceSettings

MAX_FRAME_BYTES = 2 * 1024 * 1024


def framed(socket: TSocket.TSocket) -> TTransport.TFramedTransport:
    return TTransport.TFramedTransport(socket, max_frame_size=MAX_FRAME_BYTES)


def binary(transport: TTransport.TFramedTransport) -> TBinaryProtocol.TBinaryProtocol:
    return TBinaryProtocol.TBinaryProtocol(
        transport,
        strictRead=True,
        strictWrite=True,
        string_length_limit=MAX_FRAME_BYTES,
        container_length_limit=64,
    )


class Connection:
    """One connection per short RPC; generated clients are never shared across threads.

    A class context manager also works with Thrift's immutable generated exceptions.
    """

    def __init__(self, settings: InferenceSettings) -> None:
        socket = TSocket.TSocket(settings.thrift_host, settings.thrift_port)
        socket.setTimeout(round(settings.thrift_socket_timeout_seconds * 1000))
        self.transport = framed(socket)

    def __enter__(self) -> LlmService.Client:
        try:
            self.transport.open()
        except Exception as exc:
            self.transport.close()
            if isinstance(exc, (OSError, TTransport.TTransportException)):
                # No generated RPC has run yet, so no request was sent on this connection.
                raise InferenceUnavailable() from None
            raise
        return LlmService.Client(binary(self.transport))

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.transport.close()
