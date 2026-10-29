'use strict';
// Cliente usado por tests/test_node_python_interop.py: fala com o servidor TCP Python de verdade
// e imprime um JSON com o que observou. Uso: node interop-client.js <host> <port>

const { GatewayClient, GatewayError } = require('../gateway-client');

async function main() {
  const [host, port] = process.argv.slice(2);
  const client = new GatewayClient({ host, port: Number(port), requestTimeoutMs: 20000 });
  const result = {};

  // Várias seguidas, sem esperar as respostas.
  const lists = await Promise.all(Array.from({ length: 25 }, () => client.listSources()));
  result.pipelinedCounts = lists.map((sources) => sources.length);
  result.sourceIds = lists[0].map((s) => s.sourceId);
  result.firstSource = lists[0][0];

  // Mensagem grande nos dois sentidos: 3 MB no pedido; o Gateway devolve um erro que repete o id.
  const hugeId = 'x'.repeat(3_000_000);
  try {
    await client.sendCommand(hugeId, { power: { on: true } });
  } catch (error) {
    if (!(error instanceof GatewayError)) throw error;
    result.bigErrorCode = error.code;
    result.bigErrorLength = error.message.length;
  }

  // A conexão continua utilizável depois da mensagem grande.
  result.afterBig = (await client.listSources()).length;
  client.close();
  process.stdout.write(JSON.stringify(result));
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error}\n`);
  process.exit(1);
});
