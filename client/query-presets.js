'use strict';
// Consultas agregadas oferecidas pelo CLI e pelo dashboard. Os nomes precisam bater com os que o motor
// de consultas do Gateway (#14) aceita; se o kickoff (#2) definir outros, é só ajustar esta lista.
// O CLI também aceita digitar qualquer nome, e o Gateway recusa os desconhecidos com ERROR_INVALID_REQUEST.

const QUERY_PRESETS = [
  { name: 'media', label: 'Média de uma grandeza' },
  { name: 'maximo', label: 'Valor máximo' },
  { name: 'minimo', label: 'Valor mínimo' },
  { name: 'contagem_eventos', label: 'Quantidade de eventos' },
];

module.exports = { QUERY_PRESETS };
