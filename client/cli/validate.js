'use strict';
// Validação das entradas digitadas no CLI, antes de qualquer envio ao Gateway.

/** Erro de uso (entrada inválida): mostrado ao usuário sem stack trace. */
class UserError extends Error {}

/** Aceita "5", "2.5" e "2,5"; recusa vazio, texto e Infinity/NaN. */
function parseNumber(text, { label, positive = false } = {}) {
  const normalized = String(text).trim().replace(',', '.');
  const value = normalized === '' ? NaN : Number(normalized);
  if (!Number.isFinite(value)) throw new UserError(`${label}: "${text}" não é um número válido`);
  if (positive && value <= 0) throw new UserError(`${label}: deve ser maior que zero`);
  return value;
}

function parseNonNegativeInteger(text, { label }) {
  const value = parseNumber(text, { label });
  if (!Number.isInteger(value) || value < 0) throw new UserError(`${label}: deve ser um inteiro maior ou igual a zero`);
  return value;
}

/** Escolhe a fonte por id exato ou pelo número da listagem (1, 2, ...). */
function findSource(sources, input) {
  const text = String(input).trim();
  if (text === '') throw new UserError('nenhuma fonte informada');
  const byId = sources.find((s) => s.sourceId === text);
  if (byId) return byId;
  if (/^\d+$/.test(text)) {
    const byIndex = sources[Number(text) - 1];
    if (byIndex) return byIndex;
  }
  throw new UserError(`fonte "${text}" não encontrada`);
}

function requireControllable(source) {
  if (!source.controllable) throw new UserError(`a fonte "${source.sourceId}" não é controlável (não aceita comandos)`);
  return source;
}

module.exports = { UserError, parseNumber, parseNonNegativeInteger, findSource, requireControllable };
