'use strict';
// Formatação da saída do CLI (tabelas e mensagens legíveis). Funções puras: fáceis de testar.

const { GatewayError, GatewayUnavailableError } = require('../gateway-client');
const { UserError } = require('./validate');

const STATE_LABEL = {
  SOURCE_ONLINE: 'ONLINE',
  SOURCE_OFFLINE: 'OFFLINE',
  SOURCE_POWERED_OFF: 'DESLIGADA',
  SOURCE_STATE_UNSPECIFIED: '?',
};

const ERROR_LABEL = {
  ERROR_INVALID_REQUEST: 'pedido inválido',
  ERROR_NOT_FOUND: 'não encontrado',
  ERROR_NOT_CONTROLLABLE: 'fonte não controlável',
  ERROR_SOURCE_UNAVAILABLE: 'fonte indisponível',
  ERROR_UNSUPPORTED: 'não suportado pelo Gateway',
  ERROR_INTERNAL: 'erro interno do Gateway',
  ERROR_UNSPECIFIED: 'erro',
};

const FAILURE_LABEL = {
  FAILURE_NONE: '',
  FAILURE_SILENT: 'falha: silenciosa',
  FAILURE_OUT_OF_RANGE: 'falha: fora da faixa',
  FAILURE_CRASH: 'falha: queda',
};

const ANSI = { ONLINE: '\x1b[32m', OFFLINE: '\x1b[31m', DESLIGADA: '\x1b[33m', RESET: '\x1b[0m' };

function stateLabel(source) {
  return STATE_LABEL[source.state] ?? source.state;
}

/**
 * Tabela de texto alinhada. `rows` é uma lista de listas de strings.
 * `decorate(texto, coluna, linha)` pode envolver a célula já alinhada (ex.: cor ANSI), sem afetar a largura.
 */
function renderTable(headers, rows, decorate = (text) => text) {
  const widths = headers.map((h, i) => Math.max(h.length, ...rows.map((r) => String(r[i]).length)));
  const line = (cells, rowIndex) =>
    cells
      .map((c, i) => decorate(String(c).padEnd(widths[i]), i, rowIndex))
      .join('  ')
      .trimEnd();
  return [line(headers, -1), widths.map((w) => '─'.repeat(w)).join('  '), ...rows.map((r, i) => line(r, i))].join('\n');
}

function formatNumber(value) {
  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(3)));
}

function formatLastValue(source) {
  if (!source.lastValues || source.lastValues.length === 0) return '-';
  const text = source.lastValues.map((m) => `${m.metric}=${formatNumber(m.value)}${m.unit ? ` ${m.unit}` : ''}`).join(', ');
  return source.lastReadingIsEvent ? `${text} (evento)` : text;
}

function formatSourceRow(source) {
  const failure = FAILURE_LABEL[source.failure] ?? '';
  return [
    source.sourceId,
    source.type,
    stateLabel(source),
    source.poweredOn ? 'sim' : 'não',
    source.controllable ? 'sim' : 'não',
    formatLastValue(source),
    source.sendIntervalS ? `${formatNumber(source.sendIntervalS)} s` : '-',
    'threshold' in source ? formatNumber(source.threshold) : '-',
    failure,
  ];
}

const STATE_COLUMN = 3;
const SOURCE_HEADERS = ['#', 'ID', 'TIPO', 'ESTADO', 'LIGADA', 'CONTROLÁVEL', 'ÚLTIMO VALOR', 'INTERVALO', 'LIMIAR', 'OBS'];

function renderSources(sources, options = {}) {
  if (sources.length === 0) return 'Nenhuma fonte conhecida pelo Gateway ainda (aguarde a descoberta).';
  const rows = sources.map((s, i) => [String(i + 1), ...formatSourceRow(s)]);
  // A cor entra depois do alinhamento: códigos ANSI não podem contar na largura das colunas.
  const paintState = (text, column, rowIndex) => {
    const color = rowIndex >= 0 && column === STATE_COLUMN ? ANSI[rows[rowIndex][column]] : undefined;
    return color ? `${color}${text}${ANSI.RESET}` : text;
  };
  return renderTable(SOURCE_HEADERS, rows, options.color ? paintState : undefined);
}

/** Parâmetros do domínio de uma fonte, para mostrar depois de um comando. */
function renderSourceDetail(source) {
  const lines = [
    `  ${source.sourceId} (${source.type}) — ${stateLabel(source)}, ${source.poweredOn ? 'ligada' : 'desligada'}`,
    `  intervalo: ${source.sendIntervalS ? `${formatNumber(source.sendIntervalS)} s` : '-'}   limiar: ${'threshold' in source ? formatNumber(source.threshold) : '-'}`,
  ];
  const parameters = Object.entries(source.parameters ?? {});
  if (parameters.length) lines.push(`  parâmetros: ${parameters.map(([k, v]) => `${k}=${formatNumber(v)}`).join(', ')}`);
  if (FAILURE_LABEL[source.failure]) lines.push(`  ${FAILURE_LABEL[source.failure]}`);
  return lines.join('\n');
}

function renderCommandResult(result) {
  return `✔ ${result.message}\n${renderSourceDetail(result.source)}`;
}

function renderQueryResult(result) {
  const lines = [`Consulta "${result.queryType}": ${formatNumber(result.value)}  (${result.samples} leituras)`];
  if (result.details.length) {
    lines.push(
      renderTable(
        ['FONTE', 'TIPO', 'GRANDEZA', 'VALOR', 'LEITURAS'],
        result.details.map((d) => [d.sourceId, d.type, d.metric || '-', formatNumber(d.value), String(d.samples)]),
      ),
    );
  }
  return lines.join('\n');
}

function renderHistory(history, { limit = 20 } = {}) {
  if (history.points.length === 0) return `Sem leituras de "${history.sourceId}" nesse intervalo.`;
  const shown = history.points.slice(-limit);
  const rows = shown.map((p) => [
    new Date(p.timestampMs).toLocaleString('pt-BR'),
    p.measurements.map((m) => `${m.metric}=${formatNumber(m.value)}${m.unit ? ` ${m.unit}` : ''}`).join(', '),
    p.isEvent ? 'evento' : '',
  ]);
  const header = `Histórico de "${history.sourceId}": ${history.points.length} leituras` + (shown.length < history.points.length ? ` (últimas ${shown.length})` : '');
  return `${header}\n${renderTable(['QUANDO', 'VALORES', ''], rows)}`;
}

/** Texto para o usuário a partir de qualquer erro que o menu possa receber. */
function describeError(error) {
  if (error instanceof UserError) return `✖ ${error.message}`;
  if (error instanceof GatewayError) return `✖ ${ERROR_LABEL[error.code] ?? 'erro'}: ${truncate(error.message)}`;
  if (error instanceof GatewayUnavailableError) return `✖ ${error.message}\n  A conexão será reaberta automaticamente na próxima ação.`;
  if (error instanceof TypeError) return `✖ ${error.message}`;
  return `✖ erro inesperado: ${error.message}`;
}

function truncate(text, max = 300) {
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

module.exports = {
  renderTable,
  renderSources,
  renderSourceDetail,
  renderCommandResult,
  renderQueryResult,
  renderHistory,
  describeError,
  formatLastValue,
  stateLabel,
};
