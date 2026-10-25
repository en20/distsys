"""Envia comandos gRPC diretamente a uma fonte controlável (sem Gateway).

    python -m sources.tools.control localhost:50051 status
    python -m sources.tools.control localhost:50051 off
    python -m sources.tools.control localhost:50051 on
    python -m sources.tools.control localhost:50051 interval 5
    python -m sources.tools.control localhost:50052 threshold 80
    python -m sources.tools.control localhost:50051 param vazao_alvo_lpm 20
    python -m sources.tools.control localhost:50051 fail silent 10
    python -m sources.tools.control localhost:50051 fail none
"""

from __future__ import annotations

import argparse
import sys

import grpc

from sources.pb import pb, pb_grpc

FAILURE_MODES = {"none": pb.FAILURE_NONE, "silent": pb.FAILURE_SILENT,
                 "out_of_range": pb.FAILURE_OUT_OF_RANGE, "crash": pb.FAILURE_CRASH}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0],
                                     formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    parser.add_argument("target", help="host:porta do servidor gRPC da fonte")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    sub.add_parser("on")
    sub.add_parser("off")
    sub.add_parser("interval").add_argument("seconds", type=float)
    sub.add_parser("threshold").add_argument("value", type=float)
    p = sub.add_parser("param")
    p.add_argument("name")
    p.add_argument("value", type=float)
    p = sub.add_parser("fail")
    p.add_argument("mode", choices=FAILURE_MODES)
    p.add_argument("duration", type=int, nargs="?", default=0, help="segundos (0 = até 'fail none')")
    args = parser.parse_args(argv)

    with grpc.insecure_channel(args.target) as channel:
        stub = pb_grpc.SourceControlStub(channel)
        calls = {
            "status": lambda: stub.GetStatus(pb.StatusRequest(), timeout=3),
            "on": lambda: stub.SetPower(pb.PowerRequest(on=True), timeout=3),
            "off": lambda: stub.SetPower(pb.PowerRequest(on=False), timeout=3),
            "interval": lambda: stub.SetSendInterval(pb.IntervalRequest(send_interval_s=args.seconds), timeout=3),
            "threshold": lambda: stub.SetThreshold(pb.ThresholdRequest(threshold=args.value), timeout=3),
            "param": lambda: stub.SetParameter(pb.ParameterRequest(name=args.name, value=args.value), timeout=3),
            "fail": lambda: stub.SimulateFailure(
                pb.FailureRequest(mode=FAILURE_MODES[args.mode], duration_s=args.duration), timeout=3),
        }
        try:
            status = calls[args.command]()
        except grpc.RpcError as exc:
            print(f"erro {exc.code().name}: {exc.details()}", file=sys.stderr)
            return 1
    print(describe_status(status))
    return 0


def describe_status(status: "pb.SourceStatus") -> str:
    lines = [
        f"fonte:      {status.info.source_id} ({pb.SourceType.Name(status.info.type)}, {status.info.location})",
        f"ligada:     {'sim' if status.powered_on else 'não'}",
        f"intervalo:  {status.send_interval_s:g}s",
    ]
    if status.HasField("threshold"):
        lines.append(f"limiar:     {status.threshold:g}")
    for name, value in sorted(status.parameters.items()):
        lines.append(f"parâmetro:  {name} = {value:g}")
    failure = pb.FailureMode.Name(status.failure_mode)
    if status.failure_until_ms:
        failure += f" (até epoch {status.failure_until_ms} ms)"
    lines.append(f"falha:      {failure}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
