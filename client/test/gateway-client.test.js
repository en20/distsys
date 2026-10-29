'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { GatewayClient, GatewayError, GatewayUnavailableError } = require('../gateway-client');
const { startFakeGateway } = require('../testing/fake-gateway');

const SOURCE = { sourceId: 'sensor-1', type: 'SENSOR_TESTE', controllable: false, state: 'SOURCE_ONLINE' };

async function withGateway(handler, run) {
  const gateway = await startFakeGateway(handler);
  const client = new GatewayClient({ host: '127.0.0.1', port: gateway.port, requestTimeoutMs: 500 });
  try {
    await run(client, gateway);
  } finally {
    client.close();
    await gateway.close();
  }
}

test('listSources devolve as fontes decodificadas', () =>
  withGateway(
    () => ({ listSources: { sources: [SOURCE] } }),
    async (client) => {
      const sources = await client.listSources();
      assert.equal(sources.length, 1);
      assert.equal(sources[0].sourceId, 'sensor-1');
      assert.equal(sources[0].state, 'SOURCE_ONLINE');
      assert.equal('threshold' in sources[0], false); // limiar ausente continua ausente
    },
  ));

test('sendCommand envia o oneof certo e devolve o estado atualizado', () =>
  withGateway(
    (request) => ({ command: { success: true, message: 'ok', source: { ...SOURCE, poweredOn: request.command.power.on } } }),
    async (client, gateway) => {
      const result = await client.sendCommand('sensor-1', { power: { on: true } });
      assert.equal(gateway.requests[0].command.sourceId, 'sensor-1');
      assert.equal(gateway.requests[0].command.command, 'power');
      assert.equal(result.source.poweredOn, true);
    },
  ));

test('ErrorResponse vira GatewayError com código e mensagem', () =>
  withGateway(
    () => ({ error: { code: 'ERROR_NOT_FOUND', message: "fonte 'x' não encontrada" } }),
    async (client) => {
      await assert.rejects(client.sendCommand('x', { power: { on: true } }), (error) => {
        assert.ok(error instanceof GatewayError);
        assert.equal(error.code, 'ERROR_NOT_FOUND');
        assert.match(error.message, /não encontrada/);
        return true;
      });
    },
  ));

test('pedidos simultâneos são correlacionados pelo requestId, mesmo respondidos fora de ordem', () => {
  const held = [];
  return withGateway(
    (request, { socket }) => {
      held.push({ request, socket });
      return null; // responde manualmente abaixo
    },
    async (client, gateway) => {
      const { encodeFrame } = require('../framing');
      const { encodeResponse } = require('../protocol');
      const first = client.query({ queryType: 'media' });
      const second = client.query({ queryType: 'maximo' });
      while (held.length < 2) await new Promise((resolve) => setTimeout(resolve, 5));
      for (const { request, socket } of [...held].reverse()) {
        const value = request.query.queryType === 'media' ? 1 : 2;
        socket.write(encodeFrame(encodeResponse({ requestId: request.requestId, query: { queryType: request.query.queryType, value } })));
      }
      assert.equal((await first).value, 1);
      assert.equal((await second).value, 2);
    },
  );
});

test('Gateway fora do ar: rejeita com GatewayUnavailableError e o endereço na mensagem', async () => {
  const gateway = await startFakeGateway(() => null);
  const { port } = gateway;
  await gateway.close(); // porta agora sem ninguém escutando
  const client = new GatewayClient({ host: '127.0.0.1', port });
  await assert.rejects(client.listSources(), (error) => {
    assert.ok(error instanceof GatewayUnavailableError);
    assert.match(error.message, new RegExp(`127.0.0.1:${port}`));
    return true;
  });
});

test('sem resposta dentro do prazo rejeita com GatewayUnavailableError', () =>
  withGateway(
    () => null,
    async (client) => {
      await assert.rejects(client.listSources(), GatewayUnavailableError);
    },
  ));

test('conexão derrubada com pedido em andamento rejeita o pedido e a chamada seguinte reconecta', () =>
  withGateway(
    (request, { socket }) => {
      if (request.request === 'query') {
        socket.destroy();
        return null;
      }
      return { listSources: { sources: [SOURCE] } };
    },
    async (client) => {
      let disconnected = 0;
      client.on('disconnected', () => disconnected++);
      await assert.rejects(client.query({ queryType: 'media' }), GatewayUnavailableError);
      assert.equal(disconnected, 1);
      assert.equal((await client.listSources()).length, 1); // reconectou sozinho
    },
  ));

test('número inválido no comando é recusado antes de ir para a rede', () =>
  withGateway(
    () => ({ listSources: { sources: [] } }),
    async (client, gateway) => {
      await assert.rejects(client.sendCommand('s', { interval: { intervalS: NaN } }), TypeError);
      assert.equal(gateway.requests.length, 0);
    },
  ));
