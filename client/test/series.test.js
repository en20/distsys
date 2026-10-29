'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { SeriesStore } = require('../web/public/series');

const m = (metric, value, unit = 'u') => ({ metric, value, unit });

test('acumula leituras por fonte e grandeza, ordenadas no tempo', () => {
  const store = new SeriesStore();
  store.addReading('s1', 3000, [m('temp', 30), m('umid', 50, '%')]);
  store.addReading('s1', 1000, [m('temp', 10)]);
  store.addReading('s1', 2000, [m('temp', 20)]);
  assert.deepEqual(store.metrics('s1'), ['temp', 'umid']);
  assert.deepEqual(store.points('s1', 'temp').map((p) => [p.x, p.y]), [[1000, 10], [2000, 20], [3000, 30]]);
  assert.equal(store.unit('s1', 'umid'), '%');
});

test('leitura repetida (mesmo timestamp) é ignorada — o polling devolve a mesma leitura várias vezes', () => {
  const store = new SeriesStore();
  assert.equal(store.addReading('s1', 1000, [m('temp', 10)]), true);
  assert.equal(store.addReading('s1', 1000, [m('temp', 10)]), false);
  assert.equal(store.points('s1', 'temp').length, 1);
});

test('fonte sem leitura (timestamp 0) e valores não finitos não entram', () => {
  const store = new SeriesStore();
  assert.equal(store.addReading('s1', 0, [m('temp', 10)]), false);
  assert.equal(store.addReading('s1', 1000, [m('temp', NaN)]), false);
  assert.deepEqual(store.metrics('s1'), []);
});

test('marca pontos de evento', () => {
  const store = new SeriesStore();
  store.addReading('s1', 1000, [m('temp', 10)], false);
  store.addReading('s1', 2000, [m('temp', 99)], true);
  assert.deepEqual(store.points('s1', 'temp').map((p) => p.event), [false, true]);
});

test('histórico do Gateway se mescla com leituras ao vivo sem duplicar', () => {
  const store = new SeriesStore();
  store.addReading('s1', 2000, [m('temp', 20)]);
  store.mergeHistory('s1', [
    { timestampMs: 1000, measurements: [m('temp', 10)], isEvent: false },
    { timestampMs: 2000, measurements: [m('temp', 20)], isEvent: false },
  ]);
  assert.deepEqual(store.points('s1', 'temp').map((p) => p.x), [1000, 2000]);
});

test('limite de pontos descarta os mais antigos', () => {
  const store = new SeriesStore(3);
  for (let t = 1; t <= 5; t++) store.addReading('s1', t * 1000, [m('temp', t)]);
  assert.deepEqual(store.points('s1', 'temp').map((p) => p.x), [3000, 4000, 5000]);
});

test('fontes diferentes não se misturam e fonte desconhecida devolve vazio', () => {
  const store = new SeriesStore();
  store.addReading('a', 1000, [m('temp', 1)]);
  store.addReading('b', 1000, [m('temp', 2)]);
  assert.equal(store.points('a', 'temp')[0].y, 1);
  assert.equal(store.points('b', 'temp')[0].y, 2);
  assert.deepEqual(store.points('x', 'temp'), []);
  assert.deepEqual(store.metrics('x'), []);
});
