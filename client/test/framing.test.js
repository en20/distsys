'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { encodeFrame, FrameDecoder, FramingError, MAX_FRAME_BYTES } = require('../framing');

const bytes = (text) => Buffer.from(text);

test('encodeFrame prefixa o tamanho em 4 bytes big-endian', () => {
  const frame = encodeFrame(bytes('abc'));
  assert.deepEqual([...frame.subarray(0, 4)], [0, 0, 0, 3]);
  assert.equal(frame.subarray(4).toString(), 'abc');
});

test('uma mensagem completa em um chunk', () => {
  const decoder = new FrameDecoder();
  const frames = decoder.push(encodeFrame(bytes('ola')));
  assert.deepEqual(frames.map(String), ['ola']);
  assert.equal(decoder.pendingBytes, 0);
});

test('várias mensagens num único chunk mantêm as fronteiras', () => {
  const decoder = new FrameDecoder();
  const chunk = Buffer.concat(['um', 'dois', 'tres'].map((t) => encodeFrame(bytes(t))));
  assert.deepEqual(decoder.push(chunk).map(String), ['um', 'dois', 'tres']);
});

test('mensagem fragmentada byte a byte só sai quando completa', () => {
  const decoder = new FrameDecoder();
  const wire = encodeFrame(bytes('fragmentada'));
  const out = [];
  for (let i = 0; i < wire.length; i++) {
    const frames = decoder.push(wire.subarray(i, i + 1));
    if (i < wire.length - 1) assert.equal(frames.length, 0);
    out.push(...frames);
  }
  assert.deepEqual(out.map(String), ['fragmentada']);
});

test('chunk que termina no meio da próxima mensagem guarda o resto', () => {
  const decoder = new FrameDecoder();
  const wire = Buffer.concat([encodeFrame(bytes('primeira')), encodeFrame(bytes('segunda'))]);
  const cut = encodeFrame(bytes('primeira')).length + 2; // 2 bytes do cabeçalho da segunda
  assert.deepEqual(decoder.push(wire.subarray(0, cut)).map(String), ['primeira']);
  assert.equal(decoder.pendingBytes, 2);
  assert.deepEqual(decoder.push(wire.subarray(cut)).map(String), ['segunda']);
});

test('mensagem grande (3 MB) chegando em chunks de 64 KB', () => {
  const payload = Buffer.alloc(3_000_000, 7);
  const wire = encodeFrame(payload);
  const decoder = new FrameDecoder();
  const out = [];
  for (let offset = 0; offset < wire.length; offset += 65536) out.push(...decoder.push(wire.subarray(offset, offset + 65536)));
  assert.equal(out.length, 1);
  assert.equal(out[0].length, 3_000_000);
  assert.ok(out[0].equals(payload));
});

test('mensagem vazia é válida', () => {
  const decoder = new FrameDecoder();
  assert.deepEqual(decoder.push(encodeFrame(Buffer.alloc(0))).map((f) => f.length), [0]);
});

test('tamanho acima do limite é recusado antes de acumular o payload', () => {
  const header = Buffer.alloc(4);
  header.writeUInt32BE(MAX_FRAME_BYTES + 1);
  assert.throws(() => new FrameDecoder().push(header), FramingError);
});

test('encodeFrame recusa payload acima do limite', () => {
  assert.throws(() => encodeFrame(Buffer.alloc(MAX_FRAME_BYTES + 1)), FramingError);
});
