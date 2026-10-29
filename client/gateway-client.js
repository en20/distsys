'use strict';
// Conexão TCP com o Gateway (#17): framing + protobuf + correlação por request_id.
// Compartilhado pelo CLI (cli/index.js) e pelo dashboard (web/server.js, #18).
//
//   const client = new GatewayClient({ host: 'localhost', port: 7000 });
//   const sources = await client.listSources();
//   await client.sendCommand('valvula-1', { power: { on: false } });
//   client.close();
//
// Se o Gateway estiver fora do ar, cada chamada tenta reconectar uma vez e, falhando, rejeita com
// GatewayUnavailableError; quando o Gateway volta, a próxima chamada já funciona sem reiniciar o cliente.

const net = require('node:net');
const { EventEmitter } = require('node:events');
const { encodeFrame, FrameDecoder } = require('./framing');
const { encodeRequest, decodeResponse } = require('./protocol');

const DEFAULT_HOST = 'localhost';
const DEFAULT_PORT = 7000;

/** O Gateway respondeu com ErrorResponse. `code` é o nome do enum, ex.: "ERROR_NOT_FOUND". */
class GatewayError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'GatewayError';
    this.code = code;
  }
}

/** Não foi possível falar com o Gateway (não conectou, caiu, ou não respondeu a tempo). */
class GatewayUnavailableError extends Error {
  constructor(message) {
    super(message);
    this.name = 'GatewayUnavailableError';
  }
}

class GatewayClient extends EventEmitter {
  constructor({ host = DEFAULT_HOST, port = DEFAULT_PORT, connectTimeoutMs = 3000, requestTimeoutMs = 10000 } = {}) {
    super();
    this.host = host;
    this.port = port;
    this.connectTimeoutMs = connectTimeoutMs;
    this.requestTimeoutMs = requestTimeoutMs;
    this.socket = null;
    this.connecting = null;
    this.nextRequestId = 1;
    this.pending = new Map(); // requestId → { resolve, reject, timer }
    this.closedByUser = false;
  }

  get address() {
    return `${this.host}:${this.port}`;
  }

  get connected() {
    return this.socket !== null && !this.socket.destroyed;
  }

  /** Conecta se ainda não estiver conectado; chamadas simultâneas compartilham a mesma tentativa. */
  connect() {
    if (this.connected) return Promise.resolve();
    if (!this.connecting) {
      this.closedByUser = false;
      this.connecting = this.openSocket().finally(() => {
        this.connecting = null;
      });
    }
    return this.connecting;
  }

  openSocket() {
    return new Promise((resolve, reject) => {
      const socket = net.createConnection({ host: this.host, port: this.port });
      const decoder = new FrameDecoder();
      const fail = (error) => {
        socket.destroy();
        reject(new GatewayUnavailableError(`Gateway indisponível em ${this.address} (${error.message || error.code})`));
      };
      socket.setTimeout(this.connectTimeoutMs, () => fail(new Error('tempo esgotado ao conectar')));
      socket.once('error', fail);
      socket.once('connect', () => {
        socket.removeListener('error', fail);
        socket.setTimeout(0);
        socket.setNoDelay(true);
        this.socket = socket;
        socket.on('data', (chunk) => this.onData(socket, decoder, chunk));
        socket.on('error', () => {}); // o 'close' que vem em seguida trata a queda
        socket.on('close', () => this.onClose(socket, decoder));
        this.emit('connected');
        resolve();
      });
    });
  }

  onData(socket, decoder, chunk) {
    let frames;
    try {
      frames = decoder.push(chunk);
    } catch (error) {
      socket.destroy(); // framing corrompido: o fluxo não é mais confiável
      return;
    }
    for (const frame of frames) {
      let response;
      try {
        response = decodeResponse(frame);
      } catch (error) {
        continue; // resposta ilegível: o pedido correspondente vai expirar no timeout
      }
      const entry = this.pending.get(response.requestId);
      if (!entry) continue;
      this.pending.delete(response.requestId);
      clearTimeout(entry.timer);
      if (response.response === 'error') {
        entry.reject(new GatewayError(response.error.code, response.error.message));
      } else {
        entry.resolve(response);
      }
    }
  }

  onClose(socket, decoder) {
    if (this.socket === socket) this.socket = null;
    const reason = decoder.pendingBytes > 0 ? 'conexão perdida no meio de uma resposta' : 'conexão com o Gateway encerrada';
    for (const [id, entry] of this.pending) {
      clearTimeout(entry.timer);
      entry.reject(new GatewayUnavailableError(`${reason} (${this.address})`));
      this.pending.delete(id);
    }
    if (!this.closedByUser) this.emit('disconnected');
  }

  /** Envia um ClientRequest (sem requestId) e devolve a resposta decodificada; rejeita em erro. */
  async request(body) {
    await this.connect();
    const requestId = this.nextRequestId++;
    const bytes = encodeRequest({ ...body, requestId });
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        reject(new GatewayUnavailableError(`Gateway não respondeu em ${this.requestTimeoutMs} ms`));
      }, this.requestTimeoutMs);
      this.pending.set(requestId, { resolve, reject, timer });
      this.socket.write(encodeFrame(bytes), (error) => {
        if (!error) return;
        this.pending.delete(requestId);
        clearTimeout(timer);
        reject(new GatewayUnavailableError(`falha ao enviar para o Gateway: ${error.message}`));
      });
    });
  }

  async listSources() {
    return (await this.request({ listSources: {} })).listSources.sources;
  }

  /** `command` é um dos: { power: {on} }, { interval: {intervalS} }, { threshold: {threshold} },
   *  { parameter: {name, value} }, { failure: {mode, durationS} }. Devolve o CommandResponse. */
  async sendCommand(sourceId, command) {
    assertFiniteNumbers(command);
    return (await this.request({ command: { sourceId, ...command } })).command;
  }

  /** { queryType, sourceId?, sourceType?, metric?, windowS? } → QueryResponse. */
  async query(params) {
    return (await this.request({ query: params })).query;
  }

  /** { sourceId, fromMs?, toMs?, metric? } → HistoryResponse. */
  async history(params) {
    return (await this.request({ history: params })).history;
  }

  close() {
    this.closedByUser = true;
    if (this.socket) this.socket.destroy();
  }
}

// protobufjs converte tipos errados em silêncio (ex.: 'abc' → null); aqui falhamos alto.
function assertFiniteNumbers(value, trail = 'comando') {
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new TypeError(`${trail}: número inválido (${value})`);
  } else if (value && typeof value === 'object') {
    for (const [key, inner] of Object.entries(value)) assertFiniteNumbers(inner, `${trail}.${key}`);
  }
}

module.exports = { GatewayClient, GatewayError, GatewayUnavailableError, DEFAULT_HOST, DEFAULT_PORT };
