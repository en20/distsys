"""Cliente gRPC do Gateway e handler de comandos das fontes controláveis (#13, requisito 1b.ii).

Interface para os outros:
    commands.execute(source_id, Command.power(False)) -> SourceStatus   # servidor TCP (LG, #15)
    commands.get_status(source_id) -> SourceStatus                      # estado atual via gRPC
    commands.sync_status_async(source_id)                               # descoberta (on_controllable_discovered)

Erros viram `CommandError(kind, message)` com mensagem pronta para o Cliente. Toda chamada
tem deadline; fonte que não responde é marcada OFFLINE no registro.

Teste manual direto numa fonte, sem Gateway:
    python -m gateway.commands 127.0.0.1:50051 status
    python -m gateway.commands 127.0.0.1:50051 off | on | interval 5 | param vazao_alvo 30
    python -m gateway.commands 127.0.0.1:50052 threshold 20 | fail silent 30 | fail crash | fail none
"""

import argparse
import logging
import threading
from collections.abc import Callable, Sequence
from concurrent import futures
from dataclasses import dataclass
from enum import Enum

import grpc

from gateway.registry import SourceRecord, SourceRegistry
from pb import sources_pb2, sources_pb2_grpc

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT_S = 2.0
REJECTED_CODES = {
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.FAILED_PRECONDITION,
    grpc.StatusCode.OUT_OF_RANGE,
    grpc.StatusCode.UNIMPLEMENTED,
}
FAILURE_MODES = {
    "none": sources_pb2.FAILURE_NONE,
    "silent": sources_pb2.FAILURE_SILENT,
    "out_of_range": sources_pb2.FAILURE_OUT_OF_RANGE,
    "crash": sources_pb2.FAILURE_CRASH,
}

ChannelFactory = Callable[[str], grpc.Channel]


class CommandKind(str, Enum):
    STATUS = "status"
    POWER = "power"
    INTERVAL = "interval"
    THRESHOLD = "threshold"
    PARAMETER = "parameter"
    FAILURE = "failure"


@dataclass(frozen=True)
class Command:
    """Comando para uma fonte controlável; use os construtores. Ex.: Command.interval(5.0)"""

    kind: CommandKind
    on: bool = False
    value: float = 0.0
    name: str = ""
    failure_mode: int = sources_pb2.FAILURE_NONE
    duration_s: int = 0

    @classmethod
    def status(cls) -> "Command":
        return cls(CommandKind.STATUS)

    @classmethod
    def power(cls, on: bool) -> "Command":
        return cls(CommandKind.POWER, on=on)

    @classmethod
    def interval(cls, seconds: float) -> "Command":
        return cls(CommandKind.INTERVAL, value=seconds)

    @classmethod
    def threshold(cls, value: float) -> "Command":
        return cls(CommandKind.THRESHOLD, value=value)

    @classmethod
    def parameter(cls, name: str, value: float) -> "Command":
        return cls(CommandKind.PARAMETER, name=name, value=value)

    @classmethod
    def failure(cls, mode: int, duration_s: int = 0) -> "Command":
        return cls(CommandKind.FAILURE, failure_mode=mode, duration_s=duration_s)

    @classmethod
    def parse(cls, tokens: Sequence[str]) -> "Command":
        """Comando em texto. Ex.: ["interval", "5"], ["off"], ["param", "vazao_alvo", "30"], ["fail", "silent", "20"]

        Lança ValueError com o uso correto quando o texto não é um comando válido.
        """
        if not tokens:
            raise ValueError(f"comando vazio; {USAGE}")
        verb, args = tokens[0].lower(), list(tokens[1:])
        try:
            if verb == "status" and not args:
                return cls.status()
            if verb in ("on", "off") and not args:
                return cls.power(verb == "on")
            if verb == "interval" and len(args) == 1:
                return cls.interval(float(args[0]))
            if verb == "threshold" and len(args) == 1:
                return cls.threshold(float(args[0]))
            if verb == "param" and len(args) == 2:
                return cls.parameter(args[0], float(args[1]))
            if verb == "fail" and 1 <= len(args) <= 2 and args[0].lower() in FAILURE_MODES:
                return cls.failure(FAILURE_MODES[args[0].lower()], int(args[1]) if len(args) == 2 else 0)
        except ValueError:
            pass
        raise ValueError(f"comando inválido {' '.join(tokens)!r}; {USAGE}")

    def describe(self) -> str:
        """Texto curto para logs e histórico. Ex.: "SetSendInterval(5s)" """
        match self.kind:
            case CommandKind.STATUS:
                return "GetStatus"
            case CommandKind.POWER:
                return f"SetPower({'on' if self.on else 'off'})"
            case CommandKind.INTERVAL:
                return f"SetSendInterval({self.value:g}s)"
            case CommandKind.THRESHOLD:
                return f"SetThreshold({self.value:g})"
            case CommandKind.PARAMETER:
                return f"SetParameter({self.name}={self.value:g})"
            case CommandKind.FAILURE:
                return f"SimulateFailure({_failure_name(self.failure_mode)}, {self.duration_s}s)"


USAGE = ("use: status | on | off | interval <s> | threshold <valor> | param <nome> <valor> | "
         "fail <none|silent|out_of_range|crash> [duração_s]")


class CommandErrorKind(str, Enum):
    UNKNOWN_SOURCE = "FONTE_DESCONHECIDA"
    NOT_CONTROLLABLE = "NAO_CONTROLAVEL"
    REJECTED = "REJEITADO"         # a fonte recusou (argumento inválido, comando não suportado)
    UNAVAILABLE = "INDISPONIVEL"   # fonte não respondeu dentro do deadline


class CommandError(Exception):
    """Falha de um comando, com mensagem pronta para mostrar no Cliente."""

    def __init__(self, kind: CommandErrorKind, message: str) -> None:
        super().__init__(message)
        self.kind = kind
        self.message = message


class CommandService:
    """Executa comandos nas fontes controláveis via gRPC e mantém o registro atualizado. Thread-safe.

    Reaproveita um canal gRPC por fonte; se a fonte reaparecer em outro IP/porta, o canal é trocado.
    Ex.:
        commands = CommandService(registry, storage=storage)
        commands.execute("valvula-1", Command.power(False))  # -> SourceStatus(powered_on=False, ...)
    """

    def __init__(
        self,
        registry: SourceRegistry,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        storage=None,
        channel_factory: ChannelFactory = grpc.insecure_channel,
    ) -> None:
        self._registry = registry
        self._timeout_s = timeout_s
        self._storage = storage
        self._channel_factory = channel_factory
        self._channels: dict[str, tuple[str, grpc.Channel, sources_pb2_grpc.SourceControlStub]] = {}
        self._lock = threading.Lock()
        # GetStatus da descoberta roda fora da thread UDP: uma fonte lenta não atrasa a recepção.
        self._background = futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="grpc-sync")

    def execute(self, source_id: str, command: Command) -> sources_pb2.SourceStatus:
        """Envia o comando e devolve o estado atualizado da fonte. Lança CommandError."""
        record = self._controllable_record(source_id)
        stub = self._stub_for(record)
        method, request = _rpc_for(stub, command)
        try:
            status = method(request, timeout=self._timeout_s)
        except grpc.RpcError as exc:
            raise self._translate_error(source_id, command, exc) from exc
        self._registry.update_status(source_id, status)
        if command.kind == CommandKind.FAILURE and command.failure_mode == sources_pb2.FAILURE_CRASH:
            self._registry.mark_offline(source_id, "processo encerrado por falha simulada")
            self._drop_channel(source_id)
        if command.kind != CommandKind.STATUS:
            log.info("Comando %s → %s: ok", command.describe(), source_id)
            self._record_event(source_id, command.describe())
        return status

    def get_status(self, source_id: str) -> sources_pb2.SourceStatus:
        return self.execute(source_id, Command.status())

    def sync_status_async(self, source_id: str) -> futures.Future:
        """GetStatus em segundo plano (ligar em `on_controllable_discovered`); erros só vão para o log."""
        return self._background.submit(self._sync_status, source_id)

    def close(self) -> None:
        self._background.shutdown(wait=False, cancel_futures=True)
        with self._lock:
            channels, self._channels = list(self._channels.values()), {}
        for _target, channel, _stub in channels:
            channel.close()

    # ----- internos -------------------------------------------------------------

    def _controllable_record(self, source_id: str) -> SourceRecord:
        record = self._registry.get(source_id)
        if record is None:
            raise CommandError(CommandErrorKind.UNKNOWN_SOURCE, f"fonte {source_id!r} não encontrada")
        if not record.controllable or not record.grpc_port:
            raise CommandError(CommandErrorKind.NOT_CONTROLLABLE,
                               f"fonte {source_id!r} ({record.source_type}) não é controlável: só envia leituras")
        return record

    def _stub_for(self, record: SourceRecord) -> sources_pb2_grpc.SourceControlStub:
        target = f"{record.ip}:{record.grpc_port}"
        with self._lock:
            cached = self._channels.get(record.source_id)
            if cached is not None and cached[0] == target:
                return cached[2]
            channel = self._channel_factory(target)
            stub = sources_pb2_grpc.SourceControlStub(channel)
            self._channels[record.source_id] = (target, channel, stub)
        if cached is not None:
            cached[1].close()  # fonte mudou de endereço (reiniciou em outro lugar)
        return stub

    def _drop_channel(self, source_id: str) -> None:
        with self._lock:
            cached = self._channels.pop(source_id, None)
        if cached is not None:
            cached[1].close()

    def _translate_error(self, source_id: str, command: Command, exc: grpc.RpcError) -> CommandError:
        code, details = exc.code(), exc.details() or ""
        if code in REJECTED_CODES:
            log.warning("Comando %s → %s recusado pela fonte: %s", command.describe(), source_id, details)
            return CommandError(CommandErrorKind.REJECTED, f"fonte {source_id!r} recusou {command.describe()}: {details}")
        self._registry.mark_offline(source_id, f"gRPC {code.name} em {command.describe()}")
        self._drop_channel(source_id)
        self._record_event(source_id, f"{command.describe()} falhou: {code.name}")
        return CommandError(CommandErrorKind.UNAVAILABLE,
                            f"fonte {source_id!r} não respondeu ({code.name}); marcada como OFFLINE")

    def _record_event(self, source_id: str, detail: str) -> None:
        if self._storage is not None:
            self._storage.record_event(source_id, "comando", detail)

    def _sync_status(self, source_id: str) -> None:
        try:
            status = self.get_status(source_id)
        except CommandError as exc:
            log.warning("GetStatus inicial de %s falhou: %s", source_id, exc.message)
            return
        log.info("Estado de %s sincronizado via gRPC: %s", source_id, describe_status(status))


def describe_status(status: sources_pb2.SourceStatus) -> str:
    """Resumo de uma linha. Ex.: "ligada, intervalo 5s, limiar 30, ventilacao=50" """
    parts = ["ligada" if status.powered_on else "desligada", f"intervalo {status.send_interval_s:g}s"]
    if status.HasField("threshold"):
        parts.append(f"limiar {status.threshold:g}")
    parts += [f"{name}={value:g}" for name, value in sorted(status.parameters.items())]
    if status.failure != sources_pb2.FAILURE_NONE:
        parts.append(f"FALHA {_failure_name(status.failure)}")
    return ", ".join(parts)


def _failure_name(mode: int) -> str:
    return sources_pb2.FailureMode.Name(mode) if mode in sources_pb2.FailureMode.values() else str(mode)


def _rpc_for(stub: sources_pb2_grpc.SourceControlStub, command: Command):
    match command.kind:
        case CommandKind.STATUS:
            return stub.GetStatus, sources_pb2.Empty()
        case CommandKind.POWER:
            return stub.SetPower, sources_pb2.PowerRequest(on=command.on)
        case CommandKind.INTERVAL:
            return stub.SetSendInterval, sources_pb2.IntervalRequest(interval_s=command.value)
        case CommandKind.THRESHOLD:
            return stub.SetThreshold, sources_pb2.ThresholdRequest(threshold=command.value)
        case CommandKind.PARAMETER:
            return stub.SetParameter, sources_pb2.ParameterRequest(name=command.name, value=command.value)
        case CommandKind.FAILURE:
            return stub.SimulateFailure, sources_pb2.FailureRequest(mode=command.failure_mode,
                                                                    duration_s=command.duration_s)
    raise ValueError(f"comando desconhecido: {command.kind!r}")


def main() -> None:
    """Envia um comando direto a uma fonte (sem Gateway) — para testar as fontes controláveis."""
    parser = argparse.ArgumentParser(description="Comando gRPC direto para uma fonte controlável")
    parser.add_argument("target", help="ip:porta gRPC da fonte, ex.: 127.0.0.1:50051")
    parser.add_argument("command", nargs="+", help=USAGE)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    args = parser.parse_args()
    try:
        command = Command.parse(args.command)
    except ValueError as exc:
        parser.error(str(exc))
    with grpc.insecure_channel(args.target) as channel:
        method, request = _rpc_for(sources_pb2_grpc.SourceControlStub(channel), command)
        try:
            status = method(request, timeout=args.timeout)
        except grpc.RpcError as exc:
            raise SystemExit(f"{command.describe()} falhou: {exc.code().name}: {exc.details()}") from None
    print(f"{status.source_id}: {describe_status(status)}")


if __name__ == "__main__":
    main()
