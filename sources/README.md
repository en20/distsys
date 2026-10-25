# Fontes de dados (Pessoa A)

Domínio: **Agricultura de precisão**. Linguagem: **Python 3.11+** (`grpcio`, `protobuf`).
Cada fonte é um **processo separado**.

| Fonte | Módulo | Tipo (`SourceType`) | Controlável | Grandezas enviadas | Comandos gRPC |
|---|---|---|---|---|---|
| Válvula de irrigação | `sources.valve` | `IRRIGATION_VALVE` | sim (porta padrão 50051) | `vazao` (L/min), `volume` (L desde a leitura anterior) | ligar/desligar (abrir/fechar), `vazao_alvo_lpm`, intervalo, falha |
| Nível do reservatório | `sources.reservoir` | `RESERVOIR_LEVEL` | sim (porta padrão 50052) | `nivel` (%) | ligar/desligar, **limiar** (%), intervalo, falha |
| Umidade do solo | `sources.soil_moisture` | `SOIL_MOISTURE` | não | `umidade_solo` (%) | — |
| Estação meteorológica | `sources.weather_station` | `WEATHER_STATION` | não | `temperatura` (°C), `umidade_ar` (%), `chuva` (mm/h), `vento` (km/h) | — |

- **Evento imediato (requisito 5c):** o sensor do reservatório amostra o nível a cada 1 s. Quando o nível
  **cai abaixo do limiar**, ele envia na hora uma leitura com `is_event = true` e `event_message`, sem esperar
  o envio periódico. Quando o nível volta ao normal (limiar + 1 %), envia outro evento.
- **Consumo de água:** o campo `volume` da válvula é a quantidade de litros usada desde a leitura anterior.
  Somando os `volume` de um talhão numa janela de tempo, obtém-se o consumo de água.
- **Umidade do solo:** cada instância (cada `--id`) tem média e variabilidade próprias. Isso deixa
  "talhão mais seco" e "fonte com maior variação" com resultados claros.

## Como rodar

Na raiz do repositório:

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python scripts/gen_proto.py          # gera proto/sources_pb2*.py (rodar de novo se mudar o .proto)

scripts/run_sources.sh               # sobe 2 válvulas, 1 reservatório, 3 sensores de umidade, 1 estação
INTERVAL=5 scripts/run_sources.sh    # o mesmo, enviando a cada 5 s
```

Para subir uma fonte só, use `python -m sources.<módulo> --help`. Opções principais:
`--id`, `--location`, `--interval`, `--grpc-port` (controláveis), `--mcast-group`, `--mcast-port`,
`--advertise-ip` (força o IP anunciado; útil com várias placas de rede) e `--seed`.

### Testar sem o Gateway real

```bash
python -m sources.tools.gateway_stub --port 6000              # descobre as fontes e imprime o que chega
python -m sources.tools.control localhost:50051 status         # comandos gRPC diretos
python -m sources.tools.control localhost:50051 off
python -m sources.tools.control localhost:50051 param vazao_alvo_lpm 20
python -m sources.tools.control localhost:50052 interval 5
python -m sources.tools.control localhost:50052 threshold 80   # dispara evento imediato
```

### Testes automatizados

```bash
pytest tests/sources
```

## Protocolo (`proto/sources.proto`)

### Portas padrão

| O quê | Valor |
|---|---|
| Grupo multicast de descoberta | `224.1.1.1:5007` |
| Porta UDP do Gateway | definida pelo Gateway no `DiscoveryRequest` (sugestão: `6000`) |
| gRPC das fontes controláveis | `50051` (válvula), `50052` (reservatório), ... |

### 1. Descoberta (UDP multicast)

```
Gateway --DiscoveryRequest--> 224.1.1.1:5007 (todas as fontes)
Fonte   --SourceMessage{announcement: SourceAnnouncement}--> gateway_ip:gateway_udp_port (UDP unicast)
```

- O Gateway envia um `DiscoveryRequest { gateway_ip, gateway_udp_port, request_id, timestamp_ms }` ao ser
  iniciado. Recomenda-se repetir periodicamente, para encontrar fontes que subirem depois.
- Se `gateway_ip` vier vazio, a fonte usa o IP de origem do pacote multicast.
- Cada fonte responde com um `SourceAnnouncement`, que leva:
  - `info`: `source_id`, `type`, `location` e `controllable`;
  - `ip`: o IP da fonte;
  - `grpc_port`: a porta do serviço gRPC, ou 0 se a fonte não for controlável;
  - `initial_state`: um `SourceStatus`;
  - `measurements`: os nomes das grandezas que a fonte envia;
  - `request_id`: o mesmo do pedido respondido.
- A fonte **só começa a enviar leituras depois de ser descoberta**. A primeira leitura sai logo após o anúncio.
- Um novo `DiscoveryRequest`, de outro endereço ou porta (por exemplo, o Gateway reiniciou), faz a fonte
  passar a enviar para o novo destino.

### 2. Fluxo de dados (UDP unicast → Gateway)

Toda mensagem que a fonte envia ao Gateway é um `SourceMessage` com `oneof payload { announcement | reading }`.
Por isso, anúncios e leituras chegam na mesma porta UDP.

`SensorReading`:

| Campo | Significado |
|---|---|
| `info` | identificação da fonte (id, tipo, local) |
| `seq` | contador por fonte, começando em 1 (permite detectar perdas no UDP) |
| `timestamp_ms` | epoch em ms |
| `measurements[]` | `{name, value, unit}`; **vazio quando a fonte está desligada** |
| `powered_on` | `false` → leitura funciona só como *heartbeat* (a fonte está viva, mas desligada) |
| `is_event` / `event_message` | envio imediato por evento (limiar) |

Uma fonte desligada **continua mandando heartbeats** no intervalo configurado. Assim, o Gateway consegue
distinguir uma fonte *desligada* de uma fonte *fora do ar*: esta última simplesmente para de enviar.

### 3. Comandos (gRPC `SourceControl`, só fontes controláveis)

Todos os RPCs devolvem o `SourceStatus` atualizado: `info`, `powered_on`, `send_interval_s`, `threshold`
(só se a fonte tiver), `parameters`, `failure_mode` e `failure_until_ms`.

| RPC | Request | Efeito |
|---|---|---|
| `GetStatus` | `StatusRequest {}` | só consulta |
| `SetPower` | `PowerRequest { on }` | liga/desliga (válvula: abre/fecha) |
| `SetSendInterval` | `IntervalRequest { send_interval_s }` | 0,5 a 3600 s; vale já para o próximo envio |
| `SetThreshold` | `ThresholdRequest { threshold }` | limiar do reservatório (0 a 100 %) |
| `SetParameter` | `ParameterRequest { name, value }` | válvula: `vazao_alvo_lpm` (0 a 100) |
| `SimulateFailure` | `FailureRequest { mode, duration_s }` | ver abaixo; `duration_s = 0` dura até `FAILURE_NONE` |

Códigos de erro:
- `INVALID_ARGUMENT`: valor fora da faixa, parâmetro desconhecido ou modo de falha inválido.
- `UNIMPLEMENTED`: comando que não existe para aquele tipo de fonte (por exemplo, limiar na válvula).
- `UNAVAILABLE`: a fonte está fora do ar.

O Gateway deve usar *deadline* (timeout) nas chamadas.

## Simulação de falhas (roteiro para o vídeo)

| Modo | O que acontece |
|---|---|
| `FAILURE_SILENT` | a fonte para de enviar **qualquer coisa** por UDP (leituras, heartbeats e respostas à descoberta). O gRPC continua respondendo, então dá para encerrar a falha |
| `FAILURE_OUT_OF_RANGE` | envia valores fisicamente impossíveis (ex.: nível 150 %) |
| `FAILURE_CRASH` | responde ao comando e **encerra o processo** (código de saída 1) |
| `FAILURE_NONE` | volta ao normal |

Passo a passo:

1. `scripts/run_sources.sh` e o Gateway rodando. As fontes aparecem ONLINE no Cliente.
2. **Queda de processo:** `kill <pid>` (ou Ctrl+C no terminal da fonte), ou pelo comando
   `python -m sources.tools.control localhost:50051 fail crash`.
   - O Gateway para de receber leituras e marca a fonte como OFFLINE.
   - Um comando para a fonte retorna erro `UNAVAILABLE`.
3. **Volta:** suba a fonte de novo, por exemplo `python -m sources.valve --id valvula-1 --location talhao-1`.
   Ela responde à próxima descoberta do Gateway e volta a aparecer ONLINE.
4. **Falha temporária sem matar o processo:**
   - Rode `python -m sources.tools.control localhost:50051 fail silent 30`.
   - A fonte some por 30 s e depois volta sozinha.
5. **Sensor com defeito:**
   - Rode `python -m sources.tools.control localhost:50052 fail out_of_range 20`.
   - O Gateway e o Cliente recebem níveis acima de 100 % durante 20 s.
