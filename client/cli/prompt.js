'use strict';
// Leitura de linhas do usuário. Usa o iterador assíncrono do readline para funcionar igual
// no terminal e com stdin redirecionado (echo "1\n0" | node cli/index.js), inclusive no fim da entrada.

const readline = require('node:readline');

/** stdin acabou (Ctrl+D ou pipe terminado): o menu encerra sem erro. */
class InputClosed extends Error {}

function createPrompter(input = process.stdin, output = process.stdout) {
  const rl = readline.createInterface({ input, terminal: false });
  const lines = rl[Symbol.asyncIterator]();
  return {
    async ask(question) {
      output.write(question);
      const { value, done } = await lines.next();
      if (done) {
        output.write('\n');
        throw new InputClosed();
      }
      return value.trim();
    },
    close: () => rl.close(),
  };
}

module.exports = { createPrompter, InputClosed };
