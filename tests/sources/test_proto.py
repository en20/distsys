"""Contrato protobuf das fontes (proto/sources.proto)."""

from sources.pb import pb, pb_grpc


def test_announcement_roundtrip_in_envelope():
    status = pb.SourceStatus(
        info=pb.SourceInfo(source_id="valvula-1", type=pb.IRRIGATION_VALVE, location="talhao-1", controllable=True),
        powered_on=True, send_interval_s=15, parameters={"vazao_alvo_lpm": 12},
    )
    message = pb.SourceMessage(announcement=pb.SourceAnnouncement(
        info=status.info, ip="10.0.0.5", grpc_port=50051, initial_state=status,
        measurements=["vazao", "volume"], request_id=7))

    decoded = pb.SourceMessage.FromString(message.SerializeToString())

    assert decoded.WhichOneof("payload") == "announcement"
    a = decoded.announcement
    assert (a.info.source_id, a.info.type, a.ip, a.grpc_port, a.request_id) == \
        ("valvula-1", pb.IRRIGATION_VALVE, "10.0.0.5", 50051, 7)
    assert a.initial_state.parameters["vazao_alvo_lpm"] == 12
    assert not a.initial_state.HasField("threshold")
    assert list(a.measurements) == ["vazao", "volume"]


def test_reading_roundtrip_with_event_and_multiple_measurements():
    reading = pb.SensorReading(info=pb.SourceInfo(source_id="estacao-1", type=pb.WEATHER_STATION),
                               seq=3, timestamp_ms=1_700_000_000_000, powered_on=True,
                               is_event=True, event_message="teste")
    reading.measurements.add(name="temperatura", value=25.5, unit="°C")
    reading.measurements.add(name="chuva", value=0.0, unit="mm/h")

    decoded = pb.SourceMessage.FromString(pb.SourceMessage(reading=reading).SerializeToString())

    assert decoded.WhichOneof("payload") == "reading"
    r = decoded.reading
    assert r.is_event and r.event_message == "teste" and r.seq == 3
    assert {m.name: (m.value, m.unit) for m in r.measurements} == {
        "temperatura": (25.5, "°C"), "chuva": (0.0, "mm/h")}


def test_threshold_is_optional_and_distinguishes_zero():
    status = pb.SourceStatus(threshold=0.0)
    assert status.HasField("threshold") and status.threshold == 0.0
    assert not pb.SourceStatus().HasField("threshold")


def test_source_control_service_has_all_commands():
    service = pb.DESCRIPTOR.services_by_name["SourceControl"]
    methods = {m.name: (m.input_type.name, m.output_type.name) for m in service.methods}
    assert methods == {
        "GetStatus": ("StatusRequest", "SourceStatus"),
        "SetPower": ("PowerRequest", "SourceStatus"),
        "SetSendInterval": ("IntervalRequest", "SourceStatus"),
        "SetThreshold": ("ThresholdRequest", "SourceStatus"),
        "SetParameter": ("ParameterRequest", "SourceStatus"),
        "SimulateFailure": ("FailureRequest", "SourceStatus"),
    }
    assert hasattr(pb_grpc, "SourceControlStub")


def test_source_types_cover_controllable_and_continuous():
    names = set(pb.SourceType.keys()) - {"SOURCE_TYPE_UNSPECIFIED"}
    assert names == {"IRRIGATION_VALVE", "RESERVOIR_LEVEL", "SOIL_MOISTURE", "WEATHER_STATION"}
