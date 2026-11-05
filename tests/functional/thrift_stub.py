"""Backend-only contract double: real framed/binary TCP, no inference implementation."""

import contextlib
import socketserver
import threading
import time

from llm_contract.generated import LlmService
from llm_contract.generated.ttypes import (
    GenerationOptions,
    GenerationResult,
    GenerationSnapshot,
    GenerationState,
    Health,
    Limits,
    LlmError,
    ModelInfo,
    TokenUsage,
)
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport


class Handler:
    def __init__(self):
        self.instance_id = "backend-functional-instance"
        self.requests = {}
        self.starts = []
        self.cancels = []
        self.delay = 0.0
        self.unreachable = False
        self.lock = threading.Lock()
        self.revision = "sha256:" + "a" * 64

    def getModelInfo(self):
        return ModelInfo(
            self.instance_id,
            "test-model",
            self.revision,
            GenerationOptions(384, 0.0, 1.0, 1),
            Limits(1, 0, 1000, 3600000, 600000, 64000, 32000, 32768),
        )

    def getHealth(self):
        return Health(True, False, 0, 0)

    def _snapshot(self, request):
        return GenerationSnapshot(request.request_id, request.instance_id, GenerationState.RUNNING)

    def startGeneration(self, request):
        if request.instance_id != self.instance_id:
            raise LlmError("instance_changed", "Instance changed", True)
        with self.lock:
            if request.request_id not in self.requests:
                self.starts.append(request)
                self.requests[request.request_id] = [request, time.monotonic() + self.delay, False]
        return self.getGeneration(request.instance_id, request.request_id)

    def getGeneration(self, instance_id, request_id):
        if self.unreachable:
            raise LlmError("unavailable", "Controlled lookup failure", True)
        if instance_id != self.instance_id:
            raise LlmError("instance_changed", "Instance changed", False)
        with self.lock:
            entry = self.requests.get(request_id)
            if entry is None:
                raise LlmError("not_found", "Unknown request", False)
            request, ready_at, cancelled = entry
            if cancelled:
                return GenerationSnapshot(
                    request_id,
                    instance_id,
                    GenerationState.CANCELLED,
                    error=LlmError("cancelled", "Cancelled", False),
                )
            if time.monotonic() < ready_at:
                return self._snapshot(request)
            content = "The average function handles values; guard empty values before division."
            result = GenerationResult(
                content, "test-model", self.revision, TokenUsage(20, 12), "stop", 10, 2
            )
            return GenerationSnapshot(
                request_id,
                instance_id,
                GenerationState.SUCCEEDED,
                result=result,
                expires_at_ms=int(time.time() * 1000) + 3600000,
            )

    def cancelGeneration(self, instance_id, request_id):
        if self.unreachable:
            raise LlmError("unavailable", "Controlled cancel failure", True)
        with self.lock:
            self.cancels.append((instance_id, request_id))
            if request_id in self.requests:
                self.requests[request_id][2] = True
        return self.getGeneration(instance_id, request_id)


class RpcConnection(socketserver.BaseRequestHandler):
    def handle(self):
        sock = TSocket.TSocket()
        sock.setHandle(self.request)
        transport = TTransport.TFramedTransport(sock)
        protocol = TBinaryProtocol.TBinaryProtocol(transport, strictRead=True, strictWrite=True)
        try:
            while True:
                self.server.processor.process(protocol, protocol)
        except Exception:
            pass
        finally:
            transport.close()


class RpcServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


@contextlib.contextmanager
def running_stub():
    handler = Handler()
    server = RpcServer(("127.0.0.1", 0), RpcConnection)
    server.processor = LlmService.Processor(handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield handler, server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
