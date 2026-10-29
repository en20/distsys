'use strict';
// Carrega proto/client.proto com protobufjs e converte entre objetos JS e bytes.
// Os campos ficam em camelCase (source_id → sourceId) e os enums viram strings ("SOURCE_ONLINE").

const path = require('node:path');
const protobuf = require('protobufjs');

const PROTO_PATH = path.join(__dirname, '..', 'proto', 'client.proto');
const root = protobuf.loadSync(PROTO_PATH);

const ClientRequest = root.lookupType('distsys.client.ClientRequest');
const GatewayResponse = root.lookupType('distsys.client.GatewayResponse');

const TO_OBJECT_OPTIONS = { longs: Number, enums: String, defaults: true, oneofs: true, arrays: true, objects: true };

/** Objeto JS → bytes de ClientRequest. Levanta Error se o objeto não bate com o contrato. */
function encodeRequest(object) {
  const message = ClientRequest.fromObject(object);
  const problem = ClientRequest.verify(message);
  if (problem) throw new Error(`pedido inválido: ${problem}`);
  return ClientRequest.encode(message).finish();
}

/** Bytes de GatewayResponse → objeto JS; `response` diz qual campo do oneof veio. */
function decodeResponse(bytes) {
  return GatewayResponse.toObject(GatewayResponse.decode(bytes), TO_OBJECT_OPTIONS);
}

// Os dois abaixo servem ao Gateway falso dos testes (e a quem quiser simular o outro lado).
function decodeRequest(bytes) {
  return ClientRequest.toObject(ClientRequest.decode(bytes), TO_OBJECT_OPTIONS);
}

function encodeResponse(object) {
  const message = GatewayResponse.fromObject(object);
  const problem = GatewayResponse.verify(message);
  if (problem) throw new Error(`resposta inválida: ${problem}`);
  return GatewayResponse.encode(message).finish();
}

module.exports = { PROTO_PATH, root, encodeRequest, decodeResponse, decodeRequest, encodeResponse };
