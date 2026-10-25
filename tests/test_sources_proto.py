from pb import sources_pb2
from tests.builders import make_announcement, make_reading


def test_envelope_carries_announcement_through_serialization() -> None:
    wire = sources_pb2.SourceMessage(announcement=make_announcement("valvula-1")).SerializeToString()
    decoded = sources_pb2.SourceMessage.FromString(wire)
    assert decoded.WhichOneof("payload") == "announcement"
    assert decoded.announcement.source_id == "valvula-1"


def test_envelope_carries_multi_metric_reading() -> None:
    reading = make_reading()
    reading.measurements.add(metric="temperatura", value=25.5, unit="°C")
    decoded = sources_pb2.SourceMessage.FromString(sources_pb2.SourceMessage(reading=reading).SerializeToString())
    assert decoded.WhichOneof("payload") == "reading"
    assert [m.metric for m in decoded.reading.measurements] == ["umidade", "temperatura"]


def test_threshold_presence_distinguishes_unset_from_zero() -> None:
    assert not sources_pb2.SourceStatus().HasField("threshold")
    assert sources_pb2.SourceStatus(threshold=0.0).HasField("threshold")


def test_source_control_service_exposes_all_rpcs() -> None:
    methods = sources_pb2.DESCRIPTOR.services_by_name["SourceControl"].methods_by_name
    assert set(methods) == {"GetStatus", "SetPower", "SetSendInterval", "SetThreshold", "SetParameter", "SimulateFailure"}
    assert all(m.output_type.name == "SourceStatus" for m in methods.values())
