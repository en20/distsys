import pytest

from sources.config import build_arg_parser, config_from_args


def parse(*argv: str):
    return config_from_args(build_arg_parser("teste", default_type="PROBE").parse_args(list(argv)))


def test_defaults_make_a_continuous_source() -> None:
    config = parse("--id", "umidade-1")
    assert config.source_type == "PROBE"
    assert config.interval_s == 15.0
    assert not config.controllable


def test_grpc_port_makes_source_controllable() -> None:
    assert parse("--id", "valvula-1", "--grpc-port", "50051").controllable


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (["--id", "x", "--interval", "0"], "--interval"),
        (["--id", "x", "--grpc-port", "70000"], "--grpc-port"),
        (["--id", "  "], "--id"),
    ],
)
def test_invalid_values_are_rejected_with_offending_value(argv: list[str], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse(*argv)
