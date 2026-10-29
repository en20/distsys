'use strict';
// Framing das mensagens protobuf sobre TCP (#9): 4 bytes big-endian com o tamanho + payload.
// Contraparte em Python: gateway/framing.py.
//
//   socket.write(encodeFrame(bytes));
//   const decoder = new FrameDecoder();
//   socket.on('data', (chunk) => decoder.push(chunk).forEach(handlePayload));

const HEADER_BYTES = 4;
const MAX_FRAME_BYTES = 16 * 1024 * 1024; // recusa tamanhos absurdos antes de acumular memória

class FramingError extends Error {}

/** Prefixa `payload` (Uint8Array/Buffer) com o tamanho em 4 bytes big-endian. */
function encodeFrame(payload) {
  if (payload.length > MAX_FRAME_BYTES) {
    throw new FramingError(`mensagem de ${payload.length} bytes excede o limite de ${MAX_FRAME_BYTES}`);
  }
  const frame = Buffer.allocUnsafe(HEADER_BYTES + payload.length);
  frame.writeUInt32BE(payload.length, 0);
  frame.set(payload, HEADER_BYTES);
  return frame;
}

/**
 * Remonta mensagens a partir dos chunks do evento 'data': um chunk pode trazer meia mensagem
 * ou várias. push() devolve só as mensagens completas, na ordem em que chegaram.
 */
class FrameDecoder {
  constructor() {
    this.chunks = [];
    this.buffered = 0;
    this.expected = null; // tamanho do payload em andamento (null = ainda sem cabeçalho completo)
  }

  push(chunk) {
    this.chunks.push(chunk);
    this.buffered += chunk.length;
    const frames = [];
    for (;;) {
      if (this.expected === null) {
        if (this.buffered < HEADER_BYTES) break;
        this.expected = this.consume(HEADER_BYTES).readUInt32BE(0);
        if (this.expected > MAX_FRAME_BYTES) {
          throw new FramingError(`tamanho de mensagem inválido: ${this.expected} bytes`);
        }
      }
      if (this.buffered < this.expected) break; // junta mais chunks sem copiar nada ainda
      frames.push(this.consume(this.expected));
      this.expected = null;
    }
    return frames;
  }

  /** Bytes de uma mensagem ainda incompleta (> 0 quando a conexão cai no meio de um frame). */
  get pendingBytes() {
    return this.buffered + (this.expected === null ? 0 : HEADER_BYTES);
  }

  consume(size) {
    const all = this.chunks.length === 1 ? this.chunks[0] : Buffer.concat(this.chunks, this.buffered);
    const taken = all.subarray(0, size);
    const rest = all.subarray(size);
    this.chunks = rest.length ? [rest] : [];
    this.buffered = rest.length;
    return taken;
  }
}

module.exports = { HEADER_BYTES, MAX_FRAME_BYTES, FramingError, encodeFrame, FrameDecoder };
