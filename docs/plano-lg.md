# Plano do fluxo LG — Cliente ↔ Gateway (TCP)

Branch: `lg/cliente-gateway-tcp` (criada a partir de `marcelo/fundacao-descoberta-registro`).
Issues cobertas: #9, #15, #17 e #18 (extra).

Legenda: `[x]` feito e verificado · `[~]` feito, verificado **só com fakes ou parcialmente** · `[ ]` pendente.

Verificação desta rodada: `pytest` → 72 passaram · `npm test` (em `client/`) → 61 passaram · CLI executado contra o Gateway Python real · dashboard aberto num Edge headless.

## Premissas que precisam de confirmação da equipe

Não vi o kickoff (#2) nem o código dos outros fluxos. Estas decisões são minhas:

- [ ] `client.proto` é independente de `sources.proto`.
- [ ] Consulta identificada por **string** (`query_type`). Nomes usados em `client/query-presets.js`: `media`, `maximo`, `minimo`, `contagem_eventos` — **chutes**; precisam bater com o #14.
- [ ] Assinaturas esperadas dos módulos (README, "Pontos de encaixe"):
  - `commands.execute(source_id, CommandRequest) -> SourceStatus` (Jon, #13)
  - `queries.run(query_type, QueryFilter, window_s) -> QueryResult` (Enzo, #14)
  - `storage.history(source_id, start_ms, end_ms) -> leituras` (Enzo, #16)
- [ ] `gateway/main.py` (do Marcelo) recebeu só `--tcp-port` e o `ClientTcpServer`; os módulos dos outros entram no `build_gateway`.
- [ ] Revisão do `.proto` por Enzo, Jon e Marcelo (a issue #9 pede; **depende de vocês**).

## #9 — Contrato protobuf e framing

- [x] `proto/client.proto` (`ClientRequest` / `GatewayResponse`, listagem, comandos, consultas, histórico, erros)
- [x] `gateway/framing.py` e `client/framing.js` (4 bytes big-endian + payload; limite de 16 MiB)
- [x] Testes Python do framing (grande, várias seguidas, byte a byte, EOF no meio, tamanho absurdo, protobuf corrompido)
- [x] Testes Node do framing
- [x] Teste Node ↔ Python (`tests/test_node_python_interop.py`): 25 pedidos em paralelo + mensagem de 3 MB nos dois sentidos
- [x] README: seção "Contrato do Cliente"

## #15 — Servidor TCP e despacho

- [x] `gateway/tcp_server.py`: vários clientes simultâneos; cliente que cai ou manda lixo não derruba o Gateway
- [x] `gateway/client_dispatcher.py`: validação, erros estruturados, módulos por injeção
- [x] Testes com fakes (dispatcher e servidor)
- [x] Ligado em `gateway/main.py` (`--tcp-port`, padrão 7000)
- [~] `ListSources` verificado ponta a ponta com fontes reais. **`Command`, `Query` e `History` só foram verificados com fakes**, porque os módulos do Jon e do Enzo não existem neste branch; no Gateway real eles respondem `ERROR_UNSUPPORTED`.

## #17 — Cliente CLI

- [x] `client/gateway-client.js` separado para o dashboard reaproveitar
- [x] Menu: listar, ligar/desligar, frequência, limiar, parâmetro, simular falha, consultas, histórico
- [x] Validação de entrada (fonte inexistente, valor inválido, fonte não controlável)
- [x] Gateway fora do ar: mensagem clara e reconexão a cada ação; leituras repetidas até 3 vezes; comandos não são reenviados
- [x] Testes do menu contra Gateway falso
- [~] Executado contra o Gateway real: conexão, listagem de fontes descobertas e validações funcionaram; os comandos retornaram "não suportado" (ver acima)

## #18 (extra) — Dashboard web

- [x] `client/web/server.js` (Express): `/api/sources`, `/api/sources/:id/command`, `/api/query`, `/api/history/:id`, `/api/config`
- [x] Lista de fontes com polling de 3 s, destaque de OFFLINE e de eventos, log de eventos e mudanças de estado
- [x] Gráfico de séries temporais (Chart.js, servido localmente), com histórico do Gateway quando existir e leituras acumuladas quando não
- [x] Formulários de comando e painel de consultas agregadas
- [x] Testes das rotas e da lógica de séries
- [~] Verificado num Edge headless: página carrega, renderiza, escapa HTML malicioso, gráfico é instanciado, formulário de comando envia e mostra o erro. **Não verifiquei visualmente um gráfico com vários pontos e eventos em vermelho.**

## Fora do meu alcance / pendente

- [ ] Teste de integração com `commands`, `queries` e `storage` reais (#20), quando os outros fluxos existirem
- [ ] Alinhar os nomes das consultas com o #14
- [ ] Nenhum commit nem push foi feito
