"""Construtores de mensagens protobuf com valores padrão para os testes."""

from gateway.readings import MetricValue, StoredReading
from pb import sources_pb2


def make_announcement(
    source_id: str = "s1",
    interval_s: float = 2.0,
    controllable: bool = False,
    powered_on: bool = True,
    ip: str = "10.0.0.7",
) -> sources_pb2.SourceAnnouncement:
    return sources_pb2.SourceAnnouncement(
        source_id=source_id,
        type="SENSOR_TESTE",
        ip=ip,
        grpc_port=50051 if controllable else 0,
        controllable=controllable,
        initial_state=sources_pb2.SourceStatus(powered_on=powered_on, send_interval_s=interval_s),
        request_id=1,
    )


def make_reading(source_id: str = "s1", value: float = 42.0, is_event: bool = False) -> sources_pb2.SensorReading:
    return sources_pb2.SensorReading(
        source_id=source_id,
        type="SENSOR_TESTE",
        timestamp_ms=1,
        measurements=[sources_pb2.Measurement(metric="umidade", value=value, unit="%")],
        is_event=is_event,
    )


def make_stored(
    source_id: str = "s1",
    received_ms: int = 1_000_000,
    is_event: bool = False,
    source_type: str = "SENSOR_TESTE",
    **metrics: float,
) -> StoredReading:
    """Leitura já armazenada; cada kwarg vira uma grandeza. Ex.: make_stored("s1", 5_000, umidade=30.0)"""
    values = metrics or {"umidade": 42.0}
    return StoredReading(
        source_id=source_id,
        source_type=source_type,
        received_ms=received_ms,
        source_timestamp_ms=received_ms,
        is_event=is_event,
        measurements=tuple(MetricValue(metric, value, "%") for metric, value in values.items()),
    )
