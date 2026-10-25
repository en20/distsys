"""Gateway mínimo para testar as fontes sem o Gateway real.

Envia DiscoveryRequest ao grupo multicast (na partida e a cada --rediscover
segundos) e imprime os anúncios e as leituras que chegam por UDP.

    python -m sources.tools.gateway_stub --port 6000
"""

from __future__ import annotations

import argparse
import socket
import time

from google.protobuf.message import DecodeError

from sources.net import DEFAULT_MULTICAST_GROUP, DEFAULT_MULTICAST_PORT
from sources.pb import pb


def send_discovery(sock: socket.socket, group: str, mcast_port: int, udp_port: int, request_id: int,
                   gateway_ip: str = "") -> None:
    request = pb.DiscoveryRequest(gateway_ip=gateway_ip, gateway_udp_port=udp_port, request_id=request_id,
                                  timestamp_ms=int(time.time() * 1000))
    sock.sendto(request.SerializeToString(), (group, mcast_port))


def describe(message: "pb.SourceMessage") -> str:
    kind = message.WhichOneof("payload")
    if kind == "announcement":
        a = message.announcement
        s = a.initial_state
        extra = f" limiar={s.threshold:g}" if s.HasField("threshold") else ""
        return (f"ANÚNCIO  {a.info.source_id:<16} {pb.SourceType.Name(a.info.type):<16} {a.ip}:{a.grpc_port} "
                f"controlável={a.info.controllable} ligada={s.powered_on} intervalo={s.send_interval_s:g}s{extra} "
                f"grandezas={list(a.measurements)}")
    r = message.reading
    values = ", ".join(f"{m.name}={m.value:g}{m.unit}" for m in r.measurements) or "(desligada)"
    tag = f"EVENTO   {r.event_message} | " if r.is_event else "LEITURA  "
    return f"{tag}{r.info.source_id:<16} #{r.seq:<4} {values}"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--port", type=int, default=6000, help="porta UDP do gateway (padrão: 6000)")
    parser.add_argument("--mcast-group", default=DEFAULT_MULTICAST_GROUP)
    parser.add_argument("--mcast-port", type=int, default=DEFAULT_MULTICAST_PORT)
    parser.add_argument("--rediscover", type=float, default=30.0,
                        help="reenvia a descoberta a cada N segundos (0 = só na partida)")
    args = parser.parse_args(argv)

    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(("", args.port))
    udp.settimeout(0.5)
    mcast = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mcast.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)

    request_id = 0
    next_discovery = 0.0
    print(f"gateway de teste: UDP {args.port}, descoberta em {args.mcast_group}:{args.mcast_port} (Ctrl+C para sair)")
    try:
        while True:
            if time.monotonic() >= next_discovery:
                request_id += 1
                send_discovery(mcast, args.mcast_group, args.mcast_port, args.port, request_id)
                print(f"{time.strftime('%H:%M:%S')} DESCOBERTA enviada (request_id={request_id})")
                next_discovery = time.monotonic() + args.rediscover if args.rediscover > 0 else float("inf")
            try:
                data, sender = udp.recvfrom(65535)
            except socket.timeout:
                continue
            message = pb.SourceMessage()
            try:
                message.ParseFromString(data)
            except DecodeError:
                print(f"pacote inválido de {sender}")
                continue
            if message.WhichOneof("payload"):
                print(f"{time.strftime('%H:%M:%S')} {describe(message)}")
    except KeyboardInterrupt:
        pass
    finally:
        udp.close()
        mcast.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
