"""Construtores de mensagens protobuf com valores padrão para os testes."""

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
