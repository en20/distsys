"""Servidor gRPC SourceControl das fontes controláveis."""

from __future__ import annotations

import logging
from concurrent import futures
from typing import TYPE_CHECKING, Callable

import grpc

from sources.base import InvalidArgument, Unsupported
from sources.pb import pb, pb_grpc

if TYPE_CHECKING:
    from sources.base import DataSource


class SourceControlServicer(pb_grpc.SourceControlServicer):
    """Traduz cada RPC para o comando correspondente da fonte."""

    def __init__(self, source: "DataSource"):
        self._source = source
        self._log = logging.getLogger(source.config.source_id)

    def _call(self, context: grpc.ServicerContext, description: str, fn: Callable[[], "pb.SourceStatus"]):
        self._log.info("comando gRPC %s (de %s)", description, context.peer())
        try:
            return fn()
        except InvalidArgument as exc:
            self._log.warning("comando %s rejeitado: %s", description, exc)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except Unsupported as exc:
            self._log.warning("comando %s não suportado: %s", description, exc)
            context.abort(grpc.StatusCode.UNIMPLEMENTED, str(exc))

    def GetStatus(self, request, context):
        return self._call(context, "GetStatus()", self._source.status)

    def SetPower(self, request, context):
        return self._call(context, f"SetPower(on={request.on})", lambda: self._source.set_power(request.on))

    def SetSendInterval(self, request, context):
        return self._call(context, f"SetSendInterval({request.send_interval_s:g}s)",
                          lambda: self._source.set_send_interval(request.send_interval_s))

    def SetThreshold(self, request, context):
        return self._call(context, f"SetThreshold({request.threshold:g})",
                          lambda: self._source.set_threshold(request.threshold))

    def SetParameter(self, request, context):
        return self._call(context, f"SetParameter({request.name}={request.value:g})",
                          lambda: self._source.set_parameter(request.name, request.value))

    def SimulateFailure(self, request, context):
        mode = pb.FailureMode.Name(request.mode) if request.mode in pb.FailureMode.values() else request.mode
        return self._call(context, f"SimulateFailure({mode}, {request.duration_s}s)",
                          lambda: self._source.simulate_failure(request.mode, request.duration_s))


def start_server(source: "DataSource", host: str, port: int) -> tuple[grpc.Server, int]:
    """Sobe o servidor gRPC da fonte e devolve (servidor, porta efetiva)."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    pb_grpc.add_SourceControlServicer_to_server(SourceControlServicer(source), server)
    try:
        bound = server.add_insecure_port(f"{host}:{port}")
    except RuntimeError as exc:
        raise OSError(f"não foi possível abrir a porta gRPC {host}:{port}: {exc}") from exc
    if bound == 0:
        raise OSError(f"não foi possível abrir a porta gRPC {host}:{port}")
    server.start()
    return server, bound
