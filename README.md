# Gestão de Ambiente Inteligente — Trabalho 1

Sistema distribuído de monitoramento e controle (Distribuição de Processos e Dados — Prof. Paulo A. L. Rego). Especificação em [`docs/especificacao-trabalho-1.pdf`](docs/especificacao-trabalho-1.pdf); acompanhamento na issue #1.

> **Domínio de aplicação:** agricultura de precisão (fontes do Enzo; falta registrar no kickoff, #2). O contrato das fontes usa o tipo como string, então não depende dessa escolha.

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
.venv/bin/python -m gateway.main --rediscovery 10 --db data/gateway.db
.venv/bin/python -m sources.valve --id valvula-1 --grpc-port 50051 --interval 5          # controlável
.venv/bin/python -m sources.reservoir --id reservatorio-1 --grpc-port 50052 --interval 5   # controlável + evento
.venv/bin/python -m sources.soil_moisture --id umidade-1 --interval 15                     # contínua
.venv/bin/python -m sources.weather_station --id estacao-1 --interval 15                   # contínua, 4 grandezas
.venv/bin/python -m sources.base --id probe-1 --type PROBE --interval 5    # fonte de teste (só descoberta)
.venv/bin/python -m sources.simulated --id temp-1 --type SENSOR_TEMP --metric temperatura --unit °C \
    --base 25 --min -10 --max 50 --interval 5                              # fonte de teste com envio periódico
.venv/bin/python -m gateway.commands 127.0.0.1:50051 status                 # comando gRPC direto numa fonte controlável
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

## Fluxo de dados (UDP)

```
fonte: sample() ──PeriodicPublisher──▶ UdpSender ──SourceMessage{reading}──▶ Gateway :6000
Gateway: GatewayUdpEndpoint ──▶ ReadingReceiver ──▶ registry.touch()   (estado ONLINE, última leitura)
                                               ├──▶ ReadingStore       (memória, 25 h, consultas)
                                               ├──▶ SqliteStorage      (disco, histórico)
                                               └──▶ is_event → ALARME no log + contagem por fonte
```

- **Fonte** (`sources/udp_sender.py`): `PeriodicPublisher` chama a função de amostragem a cada intervalo e envia pelo `UdpSender`. Nada sai antes da descoberta. `set_interval()` vale já para o próximo envio, `pause()`/`resume()` atendem o `SetPower` e `UdpSender.send(leitura, is_event=True)` faz o envio imediato por limiar (requisito 5c).
- **Valores simulados** (`sources/generator.py`): `ValueGenerator` = base + tendência + deriva lenta + ruído, limitado à faixa física. `spike()` força um pico (útil para disparar o limiar na demo).
- **Gateway** (`gateway/readings.py`): as janelas de tempo usam o relógio do Gateway na chegada (`received_ms`), então fontes com relógio errado não bagunçam as consultas. Leitura de fonte desconhecida é guardada e antecipa a redescoberta.

## Consultas agregadas (requisito 5d)

`gateway/queries.py` — genéricas por grandeza (`metric`), servem para qualquer domínio. Cada resultado traz o valor, a fonte associada, o ranking por fonte e uma frase pronta (`description`) para o Cliente exibir.

| Tipo | Resultado | Exemplo |
|---|---|---|
| `media` | média na janela | média de temperatura na última hora |
| `desvio_padrao` | desvio padrão amostral | desvio padrão de CO2 nas últimas 24 h |
| `minimo` / `maximo` | menor/maior leitura e a fonte | pico de consumo nas últimas 24 h |
| `soma` | soma das leituras | consumo de água em 24 h |
| `menor_media` / `maior_media` | fonte com menor/maior média | talhão mais seco, rack mais quente |
| `maior_variacao` | fonte com maior desvio padrão | fonte com maior variação nas leituras |
| `contagem_eventos` | alarmes (`is_event`) por fonte | leito com mais alarmes na última hora |

Filtros: `QueryFilter(metric, source_id, source_type)` + janela em segundos (até 30 dias). Janela vazia devolve `value = None` e "sem dados", sem erro. Janelas maiores que a retenção em memória são calculadas sobre o SQLite.

## Persistência (extra)

`gateway/storage.py` grava cada leitura em SQLite (`data/gateway.db`, `--db ""` desliga). As escritas vão para uma fila e uma thread grava em lote, então o receptor UDP nunca espera o disco. Ao reiniciar, o Gateway recarrega as últimas 25 h para a memória e as consultas continuam funcionando.

| Tabela | Conteúdo |
|---|---|
| `readings` | `source_id`, `source_type`, `received_ms`, `source_timestamp_ms`, `is_event` |
| `measurements` | `reading_id`, `metric`, `value`, `unit` (uma linha por grandeza) |
| `source_events` | comandos e mudanças de estado: `timestamp_ms`, `source_id`, `kind`, `detail` |

## Fontes do domínio — agricultura de precisão

Implementação do Enzo (branch `enzo/fontes-de-dados`) adaptada ao contrato acima: cada fonte estende `sources.datasource.DataSource`, que por sua vez estende o `SourceProcess` (descoberta) e envia pelo `UdpSender`.

| Fonte | Módulo | Tipo | Controlável | Grandezas | Comandos |
|---|---|---|---|---|---|
| Válvula de irrigação | `sources.valve` | `IRRIGATION_VALVE` | sim (`50051`, `50053`) | `vazao` (L/min), `volume` (L desde a leitura anterior) | ligar/desligar (abrir/fechar), intervalo, `vazao_alvo_lpm` (0–100), falha |
| Nível do reservatório | `sources.reservoir` | `RESERVOIR_LEVEL` | sim (`50052`) | `nivel` (%) | ligar/desligar, **limiar** (0–100 %), intervalo, falha |
| Umidade do solo | `sources.soil_moisture` | `SOIL_MOISTURE` | não | `umidade_solo` (%) | — |
| Estação meteorológica | `sources.weather_station` | `WEATHER_STATION` | não | `temperatura` (°C), `umidade_ar` (%), `chuva` (mm/h), `vento` (km/h) | — |

- **Evento imediato (requisito 5c):** o reservatório simula o nível a cada 1 s (consumo da irrigação + bomba que liga abaixo de 20 % e desliga acima de 85 %). Quando o nível **cai abaixo do limiar**, envia na hora uma leitura com `is_event = true`; quando volta acima de limiar + 1 %, envia outro evento ("nível normalizado"). Os dois aparecem como ALARME no Gateway.
- **Consumo de água:** `volume` é o que a válvula liberou desde a leitura anterior; a consulta `soma` sobre `volume` dá o consumo.
- **Umidade do solo:** cada `--id` tem média e variabilidade próprias, estáveis entre execuções.
- **Desligada** não envia leituras; o registro mostra `DESLIGADA` (estado vindo do gRPC), que não expira.
- **Comandos inválidos:** intervalo fora de 0,5–3600 s ou parâmetro/limiar fora da faixa → `INVALID_ARGUMENT`; limiar/parâmetro em fonte que não tem → `UNIMPLEMENTED`.

### Simulação de falhas (#8) — roteiro da demo

Com tudo no ar (`./scripts/run_all.sh`; acompanhe `logs/gateway.log`):

| Passo | Comando | O que aparece no Gateway |
|---|---|---|
| 1. Evento imediato | `./scripts/simulate_failure.sh event` | `ALARME de reservatorio-1` |
| 2. Fonte silenciosa | `./scripts/simulate_failure.sh silent 30` | `valvula-1: ONLINE → OFFLINE` após 3 × intervalo; volta a `ONLINE` sozinha no fim |
| 3. Queda de uma fonte contínua | `./scripts/simulate_failure.sh kill umidade-2` | `umidade-2: ONLINE → OFFLINE`; as outras seguem normais |
| 4. Valores absurdos | `./scripts/simulate_failure.sh garbage 30` | leituras da válvula fora da faixa (ex.: `vazao=150L/min`) |
| 5. Queda do processo | `./scripts/simulate_failure.sh crash` | `valvula-1: ONLINE → OFFLINE` |
| 6. Recuperação | `./scripts/simulate_failure.sh restart` | `valvula-1: OFFLINE → ONLINE` na próxima redescoberta |
| 7. Limpar | `./scripts/simulate_failure.sh recover` | limiar volta a 30 % (gera o evento "nível normalizado") |

O script fala direto com o gRPC das fontes (`python -m gateway.commands`); com o Cliente (#17) pronto, os mesmos comandos passam pelo Gateway. A fonte em falha `silent` também ignora a redescoberta (`SourceProcess.answers_discovery()`), senão o Gateway nunca a marcaria OFFLINE.

## Comandos para as fontes controláveis (gRPC) — `gateway/commands.py`

`CommandService.execute(source_id, Command.xxx(...))` procura a fonte no registro, chama a RPC com deadline de 2 s e devolve o `SourceStatus` novo, que também vai para `registry.update_status`. Os erros chegam como `CommandError(kind, message)`, com a mensagem pronta para o Cliente:

| `kind` | Quando | Efeito no registro |
|---|---|---|
| `FONTE_DESCONHECIDA` | id não está no registro | — |
| `NAO_CONTROLAVEL` | fonte contínua | — |
| `REJEITADO` | a fonte recusou (`INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`, `UNIMPLEMENTED`) | continua como estava |
| `INDISPONIVEL` | sem resposta no deadline / conexão recusada | `mark_offline` |

- Um canal gRPC por fonte, reaproveitado; se a fonte reaparecer em outro IP/porta, o canal é trocado.
- Ao descobrir uma fonte controlável nova, o Gateway faz um `GetStatus` em segundo plano (fora da thread UDP) para sincronizar o estado.
- `SimulateFailure(CRASH)` bem-sucedido marca a fonte OFFLINE na hora.
- Cada comando vai para o histórico (`storage.record_event(source_id, "comando", ...)`).

Para testar uma fonte sem Gateway: `python -m gateway.commands <ip:porta> status | on | off | interval <s> | threshold <v> | param <nome> <v> | fail <none|silent|out_of_range|crash> [s]`.

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
gateway/readings.py       receptor de leituras + séries em memória      Enzo
gateway/queries.py        consultas agregadas                           Enzo
gateway/storage.py        persistência SQLite + histórico               Enzo
gateway/main.py           liga todos os módulos                         Marcelo
sources/udp_sender.py     UdpSender + PeriodicPublisher                 Enzo
sources/generator.py      gerador de valores simulados                  Enzo
sources/simulated.py      fonte contínua genérica (testes/demo)         Enzo
sources/datasource.py     base das fontes do domínio (DataSource)       Enzo (adaptado por Jon)
sources/control.py        servidor gRPC SourceControl                   Enzo (adaptado por Jon)
sources/cli.py            linha de comando das fontes                   Enzo (adaptado por Jon)
sources/valve.py, reservoir.py, soil_moisture.py, weather_station.py   Enzo
gateway/commands.py       cliente gRPC + handler de comandos             Jon
scripts/                  gen_proto.sh, run_all.sh, stop_all.sh         Marcelo
                          simulate_failure.sh                           Jon
tests/                    pytest
```

Pontos de encaixe para os outros fluxos:

| Para | Use |
|---|---|
| Novas fontes do domínio | estender `sources.datasource.DataSource` (definir `TYPE`, `MEASUREMENTS`, `PARAMETERS`/`THRESHOLD` e os hooks `update`/`measure`/`check_event`); fonte genérica de teste: `sources.base.SourceProcess` + `UdpSender`/`PeriodicPublisher` (exemplo em `sources/simulated.py`) |
| Consultas (LG, #15) | `gateway.queries.run(tipo, QueryFilter(...), janela_s)` → `QueryResult` (`value`, `source_id`, `per_source`, `description`); erro de parâmetro = `QueryError` |
| Histórico (LG, #15) | `gateway.storage.history(source_id, inicio_ms, fim_ms, metric=None, limit=None)` → leituras em ordem cronológica (`None` se rodando com `--db ""`) |
| Comandos/estado (Jon #13, Marcelo #11) | `gateway.storage.record_event(source_id, "comando", "SetPower(off)")` — opcional, aparece no histórico |
| Comandos (LG, #15) | `gateway.commands.execute(source_id, Command.power(False))` → `SourceStatus`; erro = `CommandError` (`kind`, `message`). Construtores: `status()`, `power(on)`, `interval(s)`, `threshold(v)`, `parameter(nome, v)`, `failure(modo, s)`; `Command.parse(["interval", "5"])` aceita texto |
| Servidor TCP (LG, #15) | `registry.list()` / `registry.get()` — devolvem cópias, seguras para ler fora do lock |

## Equipe

| Pessoa | Responsabilidade |
|---|---|
| Marcelo | contratos, descoberta, registro, infraestrutura |
| Enzo | fluxo de dados (UDP), consultas, persistência, slides |
| Jon | fontes e controle (gRPC) |
| LG | Cliente e protocolo TCP (Node.js) |
