'use strict';
// Gateway falso para os testes do Node: servidor TCP que fala o protocolo real (framing + protobuf).
// Fica fora de test/ porque `node --test` executa todo .js dentro de test/ como se fosse um teste.
//
//   const gateway = await startFakeGateway((request) => ({ listSources: { sources: [] } }));
//   const client = new GatewayClient({ port: gateway.port });
//
// O handler recebe o pedido decodificado e devolve o corpo da resposta (sem requestId, que é
// preenchido aqui). Devolver `null` deixa o pedido sem resposta; `ctx.socket` permite derrubar a conexão.

const net = require('node:net');
const { encodeFrame, FrameDecoder } = require('../framing');
const { decodeRequest, encodeResponse } = require('../protocol');

function startFakeGateway(handler) {
  const sockets = new Set();
  const requests = [];
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => {});
    const decoder = new FrameDecoder();
    socket.on('data', (chunk) => {
      for (const frame of decoder.push(chunk)) {
        const request = decodeRequest(frame);
        requests.push(request);
        const body = handler(request, { socket });
        if (body) socket.write(encodeFrame(encodeResponse({ requestId: request.requestId, ...body })));
      }
    });
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      resolve({
        port: server.address().port,
        requests,
        dropConnections: () => sockets.forEach((s) => s.destroy()),
        close: () =>
          new Promise((done) => {
            sockets.forEach((s) => s.destroy());
            server.close(done);
          }),
      });
    });
  });
}

module.exports = { startFakeGateway };
