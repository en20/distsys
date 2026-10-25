# Gestão de Ambiente Inteligente — Trabalho 1

Sistema distribuído de monitoramento e controle (Distribuição de Processos e Dados — Prof. Paulo A. L. Rego). Especificação em [`docs/especificacao-trabalho-1.pdf`](docs/especificacao-trabalho-1.pdf); acompanhamento na issue #1.

> **Domínio de aplicação:** _a definir no kickoff (#2)._ O contrato das fontes usa o tipo como string justamente para não depender dessa escolha.

## Arquitetura

```
Cliente (Node.js) ──TCP (protobuf)── Gateway (Python)
                                      │  ▲
                     gRPC (comandos)  │  │ UDP (fluxo de dados, protobuf)
                                      ▼  │
                  Fontes controláveis      Fontes contínuas
                (servidor gRPC + UDP)     (só UDP)        — Python
          ...... descoberta via UDP multicast (todas as fontes) ......
```

| O quê | Valor |
|---|---|
| Grupo multicast de descoberta | `224.1.1.1:5007` |
| Porta UDP do Gateway (anúncios + leituras) | `6000` |
| Porta TCP do Gateway (Cliente) | `7000` |
| Portas gRPC das fontes controláveis | `50051`, `50052`, ... |
| Porta HTTP do dashboard (extra) | `8080` |

## Como rodar

Requisitos: Python 3.11+ e Node.js 20+.

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
./scripts/gen_proto.sh          # gera pb/sources_pb2*.py a partir de proto/
./scripts/run_all.sh            # sobe Gateway + fontes, logs em logs/*.log
tail -f logs/*.log
./scripts/stop_all.sh
```

Componentes individuais:

```bash
.venv/bin/python -m gateway.main --rediscovery 10
.venv/bin/python -m sources.base --id probe-1 --type PROBE --interval 5    # fonte de teste (só descoberta)
```

Testes: `.venv/bin/python -m pytest`

### Multicast e firewall

O `run_all.sh` usa a loopback (`DISTSYS_MCAST_IF=127.0.0.1`), que funciona em qualquer máquina. Para rodar em **várias máquinas** da mesma rede, use o IP da placa (`DISTSYS_MCAST_IF=192.168.0.10 ./scripts/run_all.sh`, ou `--mcast-if` em cada processo) e libere as portas no firewall — com `ufw`:

```bash
sudo ufw allow 5007/udp && sudo ufw allow 6000/udp
```

## Descoberta (UDP multicast)

1. O Gateway, ao iniciar e a cada `--rediscovery` segundos, envia `DiscoveryRequest` para `224.1.1.1:5007` com o IP/porta UDP onde quer receber respostas (requisito 4a).
2. Cada fonte, inscrita no grupo, guarda o endereço do Gateway e responde por UDP unicast com `SourceMessage{announcement}`: tipo, IP, porta gRPC e estado inicial (requisitos 2b e 4b).
3. A fonte só envia leituras depois de conhecer o Gateway. Se o Gateway reiniciar, a próxima redescoberta atualiza o endereço.

A redescoberta periódica cobre fontes que sobem depois do Gateway e fontes reiniciadas.

## Estado das fontes e detecção de falhas

O registro do Gateway (`gateway/registry.py`) mantém cada fonte em um de três estados:

| Estado | Quando |
|---|---|
| `ONLINE` | anúncio ou leitura recebidos recentemente |
| `OFFLINE` | nada recebido por 3 × o intervalo de envio (mínimo 5 s), ou gRPC indisponível |
| `DESLIGADA` | a fonte reportou `powered_on = false`; não expira por silêncio |

Uma fonte `OFFLINE` volta a `ONLINE` assim que manda uma leitura ou responde à redescoberta.

## Contrato das fontes — `proto/sources.proto`

| Mensagem | Canal | Conteúdo |
|---|---|---|
| `DiscoveryRequest` | multicast, Gateway → fontes | `gateway_ip`, `gateway_udp_port`, `request_id`, `timestamp_ms` |
| `SourceAnnouncement` | UDP, fonte → Gateway | `source_id`, `type`, `ip`, `grpc_port` (0 = não controlável), `controllable`, `initial_state`, `request_id` |
| `SensorReading` | UDP, fonte → Gateway | `source_id`, `type`, `timestamp_ms`, `measurements[]` (`metric`, `value`, `unit`), `is_event` |
| `SourceMessage` | UDP, fonte → Gateway | envelope `oneof { announcement, reading }` — tudo que chega na porta 6000 |
| `SourceStatus` | descoberta e gRPC | `powered_on`, `send_interval_s`, `threshold` (opcional), `parameters` (mapa do domínio), `failure`, `failure_until_ms` |

Serviço gRPC `SourceControl` (fontes controláveis) — toda RPC devolve o `SourceStatus` atualizado:

| RPC | Request |
|---|---|
| `GetStatus` | `Empty` |
| `SetPower` | `PowerRequest { on }` |
| `SetSendInterval` | `IntervalRequest { interval_s }` |
| `SetThreshold` | `ThresholdRequest { threshold }` |
| `SetParameter` | `ParameterRequest { name, value }` |
| `SimulateFailure` | `FailureRequest { mode, duration_s }` — `FAILURE_SILENT`, `FAILURE_OUT_OF_RANGE`, `FAILURE_CRASH` |

## Estrutura e interfaces entre os módulos

```
proto/sources.proto       contrato Gateway ↔ fontes                     Marcelo
common/                   sockets, threads de fundo, logging            Marcelo
sources/config.py         argumentos das fontes                         Marcelo
sources/discovery.py      resposta à descoberta, endereço do Gateway    Marcelo
sources/base.py           SourceProcess (classe base das fontes)        Marcelo
gateway/discovery.py      DiscoveryRequest + registro dos anúncios      Marcelo
gateway/registry.py       registro de fontes + detecção de falhas       Marcelo
gateway/udp_endpoint.py   socket UDP do Gateway, despacho pelo oneof    Marcelo
gateway/main.py           liga todos os módulos                         Marcelo
scripts/                  gen_proto.sh, run_all.sh, stop_all.sh         Marcelo
tests/                    pytest
```

Pontos de encaixe para os outros fluxos:

| Para | Use |
|---|---|
| Fontes (Jon, Enzo) | estender `sources.base.SourceProcess`; sobrescrever `current_status()`; enviar leituras só quando `gateway_address()` não for `None`; reaproveitar `self.udp_sock` |
| Receptor UDP (Enzo, #12) | callback `on_reading(reading, sender)` do `GatewayUdpEndpoint` — substitui o `RegistryReadingSink` em `gateway/main.py`, chamando `registry.touch()` |
| Comandos gRPC (Jon, #13) | `registry.update_status()` / `registry.mark_offline()`; `on_controllable_discovered(source_id)` do `AnnouncementHandler` para o `GetStatus` inicial |
| Servidor TCP (LG, #15) | `registry.list()` / `registry.get()` — devolvem cópias, seguras para ler fora do lock |

## Equipe

| Pessoa | Responsabilidade |
|---|---|
| Marcelo | contratos, descoberta, registro, infraestrutura |
| Enzo | fluxo de dados (UDP), consultas, persistência |
| Jon | fontes e controle (gRPC) |
| LG | Cliente e protocolo TCP (Node.js) |
