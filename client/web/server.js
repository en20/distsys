'use strict';
// Dashboard web (#18, extra): o navegador fala HTTP com este Express e o Express fala TCP + protobuf
// com o Gateway, reaproveitando gateway-client.js do CLI. Requisito 1b.i continua valendo.
//   node web/server.js [--port 8080] [--gateway-host localhost] [--gateway-port 7000] [--bind 127.0.0.1]
//
// Rotas: GET /api/sources · POST /api/sources/:id/command · GET /api/query · GET /api/history/:id
//        GET /api/config (consultas oferecidas) · estáticos em web/public

const path = require('node:path');
const { parseArgs } = require('node:util');
const express = require('express');
const { GatewayClient, GatewayError, GatewayUnavailableError, DEFAULT_HOST, DEFAULT_PORT } = require('../gateway-client');
const { QUERY_PRESETS } = require('../query-presets');

const DEFAULT_WEB_PORT = 8080;
const FAILURE_MODES = ['FAILURE_NONE', 'FAILURE_SILENT', 'FAILURE_OUT_OF_RANGE', 'FAILURE_CRASH'];

const HTTP_STATUS_FOR_CODE = {
  ERROR_INVALID_REQUEST: 400,
  ERROR_NOT_FOUND: 404,
  ERROR_NOT_CONTROLLABLE: 409,
  ERROR_SOURCE_UNAVAILABLE: 502,
  ERROR_UNSUPPORTED: 501,
  ERROR_INTERNAL: 500,
};

/** Erro de validação da requisição HTTP (responde 400 sem consultar o Gateway). */
class BadRequest extends Error {}

function createApp(client) {
  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '10kb' }));

  app.get('/api/config', (req, res) => {
    res.json({ queries: QUERY_PRESETS, gateway: client.address });
  });

  app.get('/api/sources', handle(async () => ({ sources: await client.listSources() })));

  app.post(
    '/api/sources/:id/command',
    handle(async (req) => client.sendCommand(req.params.id, parseCommand(req.body))),
  );

  app.get(
    '/api/query',
    handle(async (req) => {
      const { queryType = '', sourceId = '', sourceType = '', metric = '' } = req.query;
      if (![queryType, sourceId, sourceType, metric].every((v) => typeof v === 'string')) throw new BadRequest('parâmetros inválidos');
      if (!queryType) throw new BadRequest('queryType é obrigatório');
      return client.query({ queryType, sourceId, sourceType, metric, windowS: parseCount(req.query.windowS, 'windowS') });
    }),
  );

  app.get(
    '/api/history/:id',
    handle(async (req) => {
      const metric = typeof req.query.metric === 'string' ? req.query.metric : '';
      return client.history({
        sourceId: req.params.id,
        fromMs: parseCount(req.query.fromMs, 'fromMs'),
        toMs: parseCount(req.query.toMs, 'toMs'),
        metric,
      });
    }),
  );

  // O pacote não exporta dist/chart.umd.js; achamos a pasta dist pela entrada principal (dist/chart.cjs).
  const chartBundle = path.join(path.dirname(require.resolve('chart.js')), 'chart.umd.js');
  app.use('/vendor/chart.umd.js', (req, res) => res.sendFile(chartBundle));
  app.use(express.static(path.join(__dirname, 'public')));
  app.use('/api', (req, res) => res.status(404).json({ error: { code: 'NOT_FOUND', message: 'rota inexistente' } }));
  // eslint-disable-next-line no-unused-vars -- Express só reconhece error handlers com 4 parâmetros
  app.use((error, req, res, next) => {
    const status = error.status === 400 || error.status === 413 ? error.status : 500;
    res.status(status).json({ error: { code: status === 500 ? 'INTERNAL' : 'BAD_REQUEST', message: status === 500 ? 'erro interno do dashboard' : 'corpo da requisição inválido' } });
  });
  return app;
}

/** Converte um handler async em rota Express: sucesso → JSON; erros → status HTTP + {error:{code,message}}. */
function handle(fn) {
  return async (req, res) => {
    try {
      res.json(await fn(req));
    } catch (error) {
      const { status, body } = toHttpError(error);
      res.status(status).json(body);
    }
  };
}

function toHttpError(error) {
  if (error instanceof BadRequest || error instanceof TypeError) {
    return { status: 400, body: { error: { code: 'BAD_REQUEST', message: error.message } } };
  }
  if (error instanceof GatewayError) {
    return { status: HTTP_STATUS_FOR_CODE[error.code] ?? 500, body: { error: { code: error.code, message: error.message } } };
  }
  if (error instanceof GatewayUnavailableError) {
    return { status: 503, body: { error: { code: 'GATEWAY_UNAVAILABLE', message: error.message } } };
  }
  console.error(error);
  return { status: 500, body: { error: { code: 'INTERNAL', message: 'erro interno do dashboard' } } };
}

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function requireNumber(value, label) {
  if (typeof value !== 'number' || !Number.isFinite(value)) throw new BadRequest(`${label}: número inválido`);
  return value;
}

/** Aceita exatamente um comando no formato do gateway-client: { power: {on} }, { interval: {intervalS} }, ... */
function parseCommand(body) {
  if (!isPlainObject(body)) throw new BadRequest('corpo da requisição deve ser um objeto JSON');
  const keys = Object.keys(body);
  if (keys.length !== 1) throw new BadRequest('informe exatamente um comando (power, interval, threshold, parameter ou failure)');
  const [kind] = keys;
  const args = body[kind];
  if (!isPlainObject(args)) throw new BadRequest(`${kind}: argumentos inválidos`);
  switch (kind) {
    case 'power':
      if (typeof args.on !== 'boolean') throw new BadRequest('power.on deve ser true ou false');
      return { power: { on: args.on } };
    case 'interval': {
      const intervalS = requireNumber(args.intervalS, 'interval.intervalS');
      if (intervalS <= 0) throw new BadRequest('o intervalo de envio deve ser maior que zero');
      return { interval: { intervalS } };
    }
    case 'threshold':
      return { threshold: { threshold: requireNumber(args.threshold, 'threshold.threshold') } };
    case 'parameter':
      if (typeof args.name !== 'string' || args.name.trim() === '') throw new BadRequest('parameter.name é obrigatório');
      return { parameter: { name: args.name.trim(), value: requireNumber(args.value, 'parameter.value') } };
    case 'failure': {
      if (!FAILURE_MODES.includes(args.mode)) throw new BadRequest(`failure.mode deve ser um de: ${FAILURE_MODES.join(', ')}`);
      const durationS = args.durationS === undefined ? 0 : requireNumber(args.durationS, 'failure.durationS');
      if (!Number.isInteger(durationS) || durationS < 0) throw new BadRequest('failure.durationS deve ser inteiro >= 0');
      return { failure: { mode: args.mode, durationS } };
    }
    default:
      throw new BadRequest(`comando desconhecido: ${kind}`);
  }
}

/** Query string numérica opcional: ausente/vazio → 0; senão inteiro >= 0. */
function parseCount(value, label) {
  if (value === undefined || value === '') return 0;
  const number = typeof value === 'string' ? Number(value) : NaN;
  if (!Number.isInteger(number) || number < 0) throw new BadRequest(`${label}: deve ser um inteiro maior ou igual a zero`);
  return number;
}

function main() {
  const { values } = parseArgs({
    options: {
      port: { type: 'string', default: String(DEFAULT_WEB_PORT) },
      bind: { type: 'string', default: '127.0.0.1' },
      'gateway-host': { type: 'string', default: process.env.DISTSYS_GATEWAY_HOST ?? DEFAULT_HOST },
      'gateway-port': { type: 'string', default: process.env.DISTSYS_GATEWAY_PORT ?? String(DEFAULT_PORT) },
    },
  });
  const client = new GatewayClient({ host: values['gateway-host'], port: Number(values['gateway-port']) });
  const app = createApp(client);
  // Sem autenticação: por padrão só aceita conexões da própria máquina (use --bind 0.0.0.0 para abrir).
  const server = app.listen(Number(values.port), values.bind, () => {
    console.log(`Dashboard em http://${values.bind}:${values.port} (Gateway em ${client.address})`);
  });
  const shutdown = () => {
    client.close();
    server.close(() => process.exit(0));
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) main();

module.exports = { createApp, parseCommand };
