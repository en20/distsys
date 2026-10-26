#!/usr/bin/env node
'use strict';
// Cliente de linha de comando do Gateway (#17): TCP + protobuf.
//   node cli/index.js [--host localhost] [--port 7000]

const { parseArgs } = require('node:util');
const { GatewayClient, DEFAULT_HOST, DEFAULT_PORT } = require('../gateway-client');
const { createPrompter, InputClosed } = require('./prompt');
const { runMenu } = require('./menu');

function parseCliArgs(argv) {
  const { values } = parseArgs({
    args: argv,
    options: {
      host: { type: 'string', default: process.env.DISTSYS_GATEWAY_HOST ?? DEFAULT_HOST },
      port: { type: 'string', default: process.env.DISTSYS_GATEWAY_PORT ?? String(DEFAULT_PORT) },
      help: { type: 'boolean', short: 'h', default: false },
    },
  });
  const port = Number(values.port);
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error(`--port inválida: "${values.port}"`);
  return { host: values.host, port, help: values.help };
}

async function main() {
  let args;
  try {
    args = parseCliArgs(process.argv.slice(2));
  } catch (error) {
    console.error(`${error.message}\nUso: node cli/index.js [--host localhost] [--port 7000]`);
    process.exit(2);
  }
  if (args.help) {
    console.log('Uso: node cli/index.js [--host localhost] [--port 7000]');
    return;
  }

  const client = new GatewayClient({ host: args.host, port: args.port });
  client.on('disconnected', () => console.log('\n⚠ Conexão com o Gateway perdida; será reaberta na próxima ação.'));
  const prompter = createPrompter();
  const ctx = {
    client,
    ask: prompter.ask,
    print: (text) => console.log(text),
    color: process.stdout.isTTY && !process.env.NO_COLOR,
  };

  try {
    await client.connect();
    console.log(`Conectado ao Gateway em ${client.address}`);
  } catch (error) {
    console.log(`⚠ ${error.message}\n  O menu abre mesmo assim; cada ação tenta reconectar.`);
  }

  try {
    await runMenu(ctx);
  } catch (error) {
    if (!(error instanceof InputClosed)) throw error;
  } finally {
    prompter.close();
    client.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
