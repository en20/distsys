'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { parseNumber, parseNonNegativeInteger, findSource, requireControllable, UserError } = require('../cli/validate');
const { renderSources, renderTable, renderQueryResult, renderHistory, describeError } = require('../cli/format');
const { GatewayError, GatewayUnavailableError } = require('../gateway-client');

const SENSOR = {
  sourceId: 'sensor-1',
  type: 'SENSOR_UMIDADE',
  state: 'SOURCE_ONLINE',
  poweredOn: true,
  controllable: false,
  sendIntervalS: 5,
  lastValues: [{ metric: 'umidade', value: 41.5, unit: '%' }],
  failure: 'FAILURE_NONE',
  parameters: {},
};
const VALVE = { ...SENSOR, sourceId: 'valvula-1', type: 'VALVULA', controllable: true, threshold: 0, lastValues: [], state: 'SOURCE_POWERED_OFF', poweredOn: false };

test('parseNumber aceita ponto e vírgula decimal', () => {
  assert.equal(parseNumber('2,5', { label: 'x' }), 2.5);
  assert.equal(parseNumber(' 7 ', { label: 'x' }), 7);
});

test('parseNumber recusa vazio, texto, Infinity e (se pedido) zero/negativo', () => {
  for (const bad of ['', 'abc', 'Infinity', 'NaN', '1e999']) assert.throws(() => parseNumber(bad, { label: 'x' }), UserError, bad);
  assert.throws(() => parseNumber('0', { label: 'intervalo', positive: true }), /maior que zero/);
  assert.throws(() => parseNumber('-3', { label: 'intervalo', positive: true }), UserError);
  assert.equal(parseNumber('-3', { label: 'limiar' }), -3);
});

test('parseNonNegativeInteger recusa fracionário e negativo', () => {
  assert.equal(parseNonNegativeInteger('30', { label: 'x' }), 30);
  assert.throws(() => parseNonNegativeInteger('1.5', { label: 'x' }), UserError);
  assert.throws(() => parseNonNegativeInteger('-1', { label: 'x' }), UserError);
});

test('findSource aceita id exato ou número da listagem e recusa o resto', () => {
  const sources = [SENSOR, VALVE];
  assert.equal(findSource(sources, 'valvula-1'), VALVE);
  assert.equal(findSource(sources, '1'), SENSOR);
  assert.throws(() => findSource(sources, '3'), /não encontrada/);
  assert.throws(() => findSource(sources, 'fantasma'), /não encontrada/);
  assert.throws(() => findSource(sources, ''), UserError);
});

test('requireControllable recusa fonte sem gRPC com mensagem clara', () => {
  assert.equal(requireControllable(VALVE), VALVE);
  assert.throws(() => requireControllable(SENSOR), /sensor-1.*não é controlável/);
});

test('renderSources mostra id, tipo, estado, ligada, controlável, último valor, intervalo e limiar', () => {
  const text = renderSources([SENSOR, VALVE]);
  const [header, , sensorLine, valveLine] = text.split('\n');
  for (const column of ['ID', 'TIPO', 'ESTADO', 'LIGADA', 'CONTROLÁVEL', 'ÚLTIMO VALOR', 'INTERVALO', 'LIMIAR']) {
    assert.ok(header.includes(column), column);
  }
  assert.match(sensorLine, /sensor-1\s+SENSOR_UMIDADE\s+ONLINE\s+sim\s+não\s+umidade=41.5 %\s+5 s\s+-/);
  assert.match(valveLine, /valvula-1\s+VALVULA\s+DESLIGADA\s+não\s+sim\s+-\s+5 s\s+0/); // limiar 0 é diferente de ausente
});

test('renderSources sem fontes explica que a descoberta ainda pode estar em andamento', () => {
  assert.match(renderSources([]), /Nenhuma fonte/);
});

test('cor ANSI só envolve a célula do estado e não desalinha as colunas', () => {
  const colored = renderSources([SENSOR, { ...SENSOR, sourceId: 'ONLINE', state: 'SOURCE_OFFLINE' }], { color: true });
  const plain = renderSources([SENSOR, { ...SENSOR, sourceId: 'ONLINE', state: 'SOURCE_OFFLINE' }]);
  assert.equal(colored.replace(/\x1b\[\d+m/g, ''), plain);
  assert.ok(colored.split('\n')[3].startsWith('2  ONLINE'), 'id não pode ser colorido');
});

test('renderTable alinha pelas células mais largas', () => {
  const [header, , row] = renderTable(['A', 'BBBB'], [['xxxxxx', 'y']]).split('\n');
  assert.equal(header.indexOf('BBBB'), row.indexOf('y'));
});

test('renderQueryResult e renderHistory', () => {
  const query = renderQueryResult({
    queryType: 'media',
    value: 21.5,
    samples: 4,
    details: [{ sourceId: 's1', type: 'T', metric: 'umidade', value: 21.5, samples: 4 }],
  });
  assert.match(query, /Consulta "media": 21.5 {2}\(4 leituras\)/);
  assert.match(query, /s1\s+T\s+umidade\s+21.5\s+4/);
  assert.match(renderHistory({ sourceId: 's1', points: [] }), /Sem leituras/);
  const history = renderHistory({ sourceId: 's1', points: [{ timestampMs: 1_700_000_000_000, isEvent: true, measurements: [{ metric: 'umidade', value: 9, unit: '%' }] }] });
  assert.match(history, /1 leituras/);
  assert.match(history, /umidade=9 %\s+evento/);
});

test('describeError traduz cada tipo de erro sem stack trace', () => {
  assert.equal(describeError(new UserError('fonte "x" não encontrada')), '✖ fonte "x" não encontrada');
  assert.match(describeError(new GatewayError('ERROR_NOT_CONTROLLABLE', 'fonte s não aceita comandos')), /fonte não controlável: fonte s não aceita comandos/);
  assert.match(describeError(new GatewayUnavailableError('Gateway indisponível em h:1')), /Gateway indisponível em h:1\n.*reaberta/);
  assert.match(describeError(new Error('boom')), /erro inesperado: boom/);
});
