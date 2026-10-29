'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { GatewayClient } = require('../gateway-client');
const { startFakeGateway } = require('../testing/fake-gateway');
const { runMenu } = require('../cli/menu');
const { InputClosed } = require('../cli/prompt');

const SENSOR = { sourceId: 'sensor-1', type: 'SENSOR', state: 'SOURCE_ONLINE', poweredOn: true, controllable: false, sendIntervalS: 5, lastValues: [{ metric: 'umidade', value: 40, unit: '%' }] };
const VALVE = { sourceId: 'valvula-1', type: 'VALVULA', state: 'SOURCE_ONLINE', poweredOn: true, controllable: true, grpcPort: 50051, sendIntervalS: 10, parameters: { vazao: 2 } };

function fakeGatewayHandler(request) {
  if (request.request === 'listSources') return { listSources: { sources: [SENSOR, VALVE] } };
  if (request.request === 'command') {
    const { sourceId, command } = request.command;
    return { command: { success: true, message: `comando '${command}' aplicado na fonte ${sourceId}`, source: { ...VALVE, poweredOn: request.command.power?.on ?? true } } };
  }
  if (request.request === 'query') {
    return { query: { queryType: request.query.queryType, value: 12.5, samples: 3, details: [{ sourceId: 'sensor-1', type: 'SENSOR', metric: 'umidade', value: 12.5, samples: 3 }] } };
  }
  if (request.request === 'history') {
    return { history: { sourceId: request.history.sourceId, points: [{ timestampMs: 1_700_000_000_000, isEvent: false, measurements: [{ metric: 'umidade', value: 40, unit: '%' }] }] } };
  }
  return { error: { code: 'ERROR_INTERNAL', message: 'x' } };
}

/** Roda o menu com `answers` roteirizadas; devolve tudo que foi impresso. */
async function drive(answers, handler = fakeGatewayHandler) {
  const gateway = await startFakeGateway(handler);
  const client = new GatewayClient({ host: '127.0.0.1', port: gateway.port, requestTimeoutMs: 500 });
  const queue = [...answers];
  const output = [];
  const ctx = {
    client,
    retryDelayMs: 1,
    color: false,
    print: (text) => output.push(text),
    ask: async (question) => {
      output.push(question);
      if (queue.length === 0) throw new InputClosed();
      return queue.shift();
    },
  };
  try {
    await runMenu(ctx);
  } catch (error) {
    if (!(error instanceof InputClosed)) throw error;
  } finally {
    client.close();
    await gateway.close();
  }
  return { text: output.join('\n'), requests: gateway.requests };
}

const sentCommands = (requests) => requests.filter((r) => r.request === 'command').map((r) => r.command);

test('listar fontes mostra a tabela com os dois tipos de fonte', async () => {
  const { text } = await drive(['1', '0']);
  assert.match(text, /sensor-1\s+SENSOR\s+ONLINE\s+sim\s+não/);
  assert.match(text, /valvula-1\s+VALVULA\s+ONLINE\s+sim\s+sim/);
});

test('desligar e ligar: envia o comando power certo e mostra o novo estado', async () => {
  const { text, requests } = await drive(['2', 'valvula-1', 'd', '2', '2', 'l', '0']);
  const commands = sentCommands(requests);
  assert.deepEqual(commands.map((c) => [c.sourceId, c.power.on]), [['valvula-1', false], ['valvula-1', true]]);
  assert.match(text, /✔ comando 'power' aplicado/);
  assert.match(text, /desligada/);
});

test('alterar frequência envia intervalS numérico (aceita vírgula)', async () => {
  const { requests } = await drive(['3', '2', '2,5', '0']); // fonte escolhida pelo número da listagem
  assert.equal(sentCommands(requests)[0].interval.intervalS, 2.5);
});

test('limiar aceita zero e negativo', async () => {
  const { requests } = await drive(['4', 'valvula-1', '-3', '4', 'valvula-1', '0', '0']);
  assert.deepEqual(sentCommands(requests).map((c) => c.threshold.threshold), [-3, 0]);
});

test('parâmetro específico do domínio', async () => {
  const { text, requests } = await drive(['5', 'valvula-1', 'vazao', '7.5', '0']);
  assert.match(text, /Parâmetros atuais: vazao=2/);
  assert.deepEqual(sentCommands(requests)[0].parameter, { name: 'vazao', value: 7.5 });
});

test('simular falha: silenciosa com duração, queda sem duração', async () => {
  const { requests } = await drive(['6', 'valvula-1', '1', '30', '6', 'valvula-1', '3', '0']);
  const [silent, crash] = sentCommands(requests).map((c) => c.failure);
  assert.deepEqual(silent, { mode: 'FAILURE_SILENT', durationS: 30 });
  assert.deepEqual(crash, { mode: 'FAILURE_CRASH', durationS: 0 });
});

test('entradas inválidas não chegam ao Gateway e o menu continua', async () => {
  const { text, requests } = await drive([
    '3', 'valvula-1', '0', // intervalo zero
    '3', 'valvula-1', 'abc', // não numérico
    '3', 'sensor-1', // fonte não controlável
    '3', 'fantasma', // fonte inexistente
    '2', 'valvula-1', 'talvez', // resposta inválida
    '6', 'valvula-1', '9', // falha inexistente
    '99', // opção inexistente
    '1', '0',
  ]);
  assert.deepEqual(sentCommands(requests), []);
  assert.match(text, /intervalo: deve ser maior que zero/);
  assert.match(text, /"abc" não é um número válido/);
  assert.match(text, /"sensor-1" não é controlável/);
  assert.match(text, /fonte "fantasma" não encontrada/);
  assert.match(text, /responda "l"/);
  assert.match(text, /tipo de falha inválido/);
  assert.match(text, /opção inválida/);
  assert.match(text, /valvula-1\s+VALVULA/); // ainda listou depois de tantos erros
});

test('consulta agregada envia filtros e janela e mostra o resultado', async () => {
  const { text, requests } = await drive(['7', '1', '', 'SENSOR', 'umidade', '60', '0']);
  const query = requests.find((r) => r.request === 'query').query;
  assert.deepEqual(
    { queryType: query.queryType, sourceType: query.sourceType, metric: query.metric, windowS: query.windowS },
    { queryType: 'media', sourceType: 'SENSOR', metric: 'umidade', windowS: 60 },
  );
  assert.match(text, /Consulta "media": 12.5/);
});

test('consulta com nome digitado pelo usuário', async () => {
  const { requests } = await drive(['7', '5', 'minha_consulta', 'sensor-1', '', '', '0']);
  const query = requests.find((r) => r.request === 'query').query;
  assert.equal(query.queryType, 'minha_consulta');
  assert.equal(query.sourceId, 'sensor-1');
});

test('histórico de uma fonte', async () => {
  const { text, requests } = await drive(['8', 'sensor-1', '10', '0']);
  const history = requests.find((r) => r.request === 'history').history;
  assert.equal(history.sourceId, 'sensor-1');
  assert.ok(history.fromMs > 0);
  assert.match(text, /Histórico de "sensor-1": 1 leituras/);
});

test('erro do Gateway aparece legível e o menu continua', async () => {
  const handler = (request) =>
    request.request === 'command'
      ? { error: { code: 'ERROR_SOURCE_UNAVAILABLE', message: 'gRPC UNAVAILABLE' } }
      : fakeGatewayHandler(request);
  const { text } = await drive(['2', 'valvula-1', 'l', '1', '0'], handler);
  assert.match(text, /✖ fonte indisponível: gRPC UNAVAILABLE/);
  assert.match(text, /valvula-1\s+VALVULA/);
});

test('Gateway sem resposta na listagem: tenta reconectar 3 vezes e então explica', async () => {
  const { text, requests } = await drive(['1', '0'], () => null);
  assert.equal(requests.length, 3);
  assert.match(text, /tentando reconectar \(1\/2\)/);
  assert.match(text, /tentando reconectar \(2\/2\)/);
  assert.match(text, /✖ Gateway não respondeu/);
});

test('comando sem resposta não é reenviado automaticamente', async () => {
  const handler = (request) => (request.request === 'command' ? null : fakeGatewayHandler(request));
  const { requests } = await drive(['2', 'valvula-1', 'l', '0'], handler);
  assert.equal(sentCommands(requests).length, 1);
});

test('fim da entrada (EOF) encerra sem erro', async () => {
  const { text } = await drive(['1']);
  assert.match(text, /Opção: /);
});
