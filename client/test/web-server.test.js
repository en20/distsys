'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { GatewayClient } = require('../gateway-client');
const { startFakeGateway } = require('../testing/fake-gateway');
const { createApp, parseCommand } = require('../web/server');

const VALVE = { sourceId: 'valvula-1', type: 'VALVULA', state: 'SOURCE_ONLINE', poweredOn: true, controllable: true, sendIntervalS: 10 };

function defaultHandler(request) {
  switch (request.request) {
    case 'listSources':
      return { listSources: { sources: [VALVE] } };
    case 'command':
      if (request.command.sourceId === 'fantasma') return { error: { code: 'ERROR_NOT_FOUND', message: 'fonte não encontrada' } };
      if (request.command.sourceId === 'offline') return { error: { code: 'ERROR_SOURCE_UNAVAILABLE', message: 'gRPC UNAVAILABLE' } };
      return { command: { success: true, message: 'ok', source: { ...VALVE, poweredOn: request.command.power?.on ?? true } } };
    case 'query':
      return { query: { queryType: request.query.queryType, value: 5, samples: 2, details: [] } };
    case 'history':
      return { history: { sourceId: request.history.sourceId, points: [{ timestampMs: 1000, isEvent: false, measurements: [{ metric: 'm', value: 1, unit: 'u' }] }] } };
    default:
      return { error: { code: 'ERROR_INTERNAL', message: 'x' } };
  }
}

async function withApp(run, handler = defaultHandler) {
  const gateway = await startFakeGateway(handler);
  const client = new GatewayClient({ host: '127.0.0.1', port: gateway.port, requestTimeoutMs: 300 });
  const server = createApp(client).listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  const call = async (path, options) => {
    const response = await fetch(base + path, options);
    return { status: response.status, body: await response.json().catch(() => null), response };
  };
  const post = (path, body) => call(path, { method: 'POST', headers: { 'content-type': 'application/json' }, body: typeof body === 'string' ? body : JSON.stringify(body) });
  try {
    await run({ call, post, gateway, base });
  } finally {
    client.close();
    await new Promise((resolve) => server.close(resolve));
    await gateway.close();
  }
}

test('GET /api/sources traduz para ListSources no Gateway', () =>
  withApp(async ({ call }) => {
    const { status, body } = await call('/api/sources');
    assert.equal(status, 200);
    assert.equal(body.sources[0].sourceId, 'valvula-1');
  }));

test('POST /api/sources/:id/command envia o comando e devolve o estado atualizado', () =>
  withApp(async ({ post, gateway }) => {
    const { status, body } = await post('/api/sources/valvula-1/command', { power: { on: false } });
    assert.equal(status, 200);
    assert.equal(body.source.poweredOn, false);
    assert.equal(gateway.requests[0].command.sourceId, 'valvula-1');
    assert.equal(gateway.requests[0].command.power.on, false);
  }));

test('id da fonte com caracteres especiais chega intacto ao Gateway', () =>
  withApp(async ({ post, gateway }) => {
    await post(`/api/sources/${encodeURIComponent('a b/ç')}/command`, { power: { on: true } });
    assert.equal(gateway.requests[0].command.sourceId, 'a b/ç');
  }));

test('comandos inválidos viram 400 e não chegam ao Gateway', () =>
  withApp(async ({ post, gateway }) => {
    const invalid = [
      {},
      { power: { on: 'sim' } },
      { interval: { intervalS: 0 } },
      { interval: { intervalS: '5' } },
      { threshold: { threshold: null } },
      { parameter: { name: '', value: 1 } },
      { failure: { mode: 'FAILURE_XYZ' } },
      { failure: { mode: 'FAILURE_SILENT', durationS: -1 } },
      { power: { on: true }, interval: { intervalS: 1 } },
      { desconhecido: {} },
      [],
    ];
    for (const body of invalid) {
      const { status } = await post('/api/sources/valvula-1/command', body);
      assert.equal(status, 400, JSON.stringify(body));
    }
    assert.equal((await post('/api/sources/valvula-1/command', '{quebrado')).status, 400);
    assert.equal(gateway.requests.length, 0);
  }));

test('erros do Gateway ganham status HTTP e mantêm código e mensagem', () =>
  withApp(async ({ post }) => {
    const notFound = await post('/api/sources/fantasma/command', { power: { on: true } });
    assert.equal(notFound.status, 404);
    assert.deepEqual(notFound.body.error, { code: 'ERROR_NOT_FOUND', message: 'fonte não encontrada' });
    assert.equal((await post('/api/sources/offline/command', { power: { on: true } })).status, 502);
  }));

test('Gateway fora do ar vira 503 com GATEWAY_UNAVAILABLE', async () => {
  const gateway = await startFakeGateway(() => null);
  const { port } = gateway;
  await gateway.close();
  const client = new GatewayClient({ host: '127.0.0.1', port });
  const server = createApp(client).listen(0, '127.0.0.1');
  await new Promise((resolve) => server.once('listening', resolve));
  try {
    const response = await fetch(`http://127.0.0.1:${server.address().port}/api/sources`);
    assert.equal(response.status, 503);
    assert.equal((await response.json()).error.code, 'GATEWAY_UNAVAILABLE');
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('GET /api/query repassa filtros e janela', () =>
  withApp(async ({ call, gateway }) => {
    const { status, body } = await call('/api/query?queryType=media&sourceType=SENSOR&metric=umidade&windowS=60');
    assert.equal(status, 200);
    assert.equal(body.value, 5);
    const sent = gateway.requests[0].query;
    assert.deepEqual([sent.queryType, sent.sourceType, sent.metric, sent.windowS], ['media', 'SENSOR', 'umidade', 60]);
  }));

test('GET /api/query valida parâmetros', () =>
  withApp(async ({ call, gateway }) => {
    assert.equal((await call('/api/query')).status, 400);
    assert.equal((await call('/api/query?queryType=media&windowS=abc')).status, 400);
    assert.equal((await call('/api/query?queryType=media&windowS=-1')).status, 400);
    assert.equal((await call('/api/query?queryType=media&metric=a&metric=b')).status, 400);
    assert.equal(gateway.requests.length, 0);
  }));

test('GET /api/history/:id repassa o intervalo', () =>
  withApp(async ({ call, gateway }) => {
    const { status, body } = await call('/api/history/valvula-1?fromMs=500&toMs=2000');
    assert.equal(status, 200);
    assert.equal(body.points.length, 1);
    const sent = gateway.requests[0].history;
    assert.deepEqual([sent.sourceId, sent.fromMs, sent.toMs], ['valvula-1', 500, 2000]);
  }));

test('GET /api/config lista as consultas oferecidas', () =>
  withApp(async ({ call }) => {
    const { body } = await call('/api/config');
    assert.ok(body.queries.length >= 3);
    assert.ok(body.queries.every((q) => q.name && q.label));
  }));

test('serve a página, os scripts e o Chart.js localmente', () =>
  withApp(async ({ base }) => {
    for (const [path, type] of [['/', 'text/html'], ['/app.js', 'javascript'], ['/series.js', 'javascript'], ['/style.css', 'text/css'], ['/vendor/chart.umd.js', 'javascript']]) {
      const response = await fetch(base + path);
      assert.equal(response.status, 200, path);
      assert.match(response.headers.get('content-type'), new RegExp(type), path);
    }
    assert.equal((await fetch(`${base}/api/inexistente`)).status, 404);
  }));

test('parseCommand normaliza o nome do parâmetro e a duração padrão da falha', () => {
  assert.deepEqual(parseCommand({ parameter: { name: ' vazao ', value: 2 } }), { parameter: { name: 'vazao', value: 2 } });
  assert.deepEqual(parseCommand({ failure: { mode: 'FAILURE_CRASH' } }), { failure: { mode: 'FAILURE_CRASH', durationS: 0 } });
});
