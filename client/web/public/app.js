'use strict';
// Dashboard: polling de /api/sources, gráficos (Chart.js), comandos e consultas.
// Todo texto vindo do Gateway entra no DOM via textContent (ids e nomes de fontes não são confiáveis).

const POLL_MS = 3000;
const HISTORY_WINDOW_MS = 30 * 60 * 1000;
const STATE_LABEL = { SOURCE_ONLINE: 'ONLINE', SOURCE_OFFLINE: 'OFFLINE', SOURCE_POWERED_OFF: 'DESLIGADA' };
const FAILURE_LABEL = { FAILURE_SILENT: 'falha: silenciosa', FAILURE_OUT_OF_RANGE: 'falha: fora da faixa', FAILURE_CRASH: 'falha: queda' };

const store = new SeriesStore();
const state = {
  sources: [],
  selectedId: null,
  previousStates: new Map(),
  historyChecked: new Set(), // fontes cujo histórico já foi pedido (ou que o Gateway não suporta)
  chart: null,
  chartKey: '',
  commandBusy: false,
};

const $ = (id) => document.getElementById(id);

function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (key === 'class') node.className = value;
    else if (key === 'dataset') Object.assign(node.dataset, value);
    else node[key] = value;
  }
  for (const child of children) node.append(child);
  return node;
}

async function api(path, options) {
  let response;
  try {
    response = await fetch(path, options);
  } catch (error) {
    throw Object.assign(new Error('dashboard fora do ar'), { code: 'DASHBOARD_UNAVAILABLE' });
  }
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const { code = 'ERROR', message = `HTTP ${response.status}` } = body.error ?? {};
    throw Object.assign(new Error(message), { code, status: response.status });
  }
  return body;
}

const stateOf = (source) => STATE_LABEL[source.state] ?? '?';
const fmt = (value) => (Number.isInteger(value) ? String(value) : String(Number(value.toFixed(3))));
const timeOf = (ms) => new Date(ms).toLocaleTimeString('pt-BR');

function lastValueText(source) {
  if (!source.lastValues.length) return '-';
  const text = source.lastValues.map((m) => `${m.metric}=${fmt(m.value)}${m.unit ? ` ${m.unit}` : ''}`).join(', ');
  return source.lastReadingIsEvent ? `${text} ⚠ evento` : text;
}

// ── polling ──────────────────────────────────────────────────────────────────

async function poll() {
  try {
    const { sources } = await api('/api/sources');
    state.sources = sources;
    setConnection(true);
    ingest(sources);
    renderSources();
    $('updated').textContent = `atualizado às ${timeOf(Date.now())}`;
    refreshSelection();
  } catch (error) {
    setConnection(false, error.message);
  }
}

function setConnection(ok, message = '') {
  const badge = $('connection');
  badge.className = `badge ${ok ? 'ok' : 'bad'}`;
  badge.textContent = ok ? 'Gateway conectado' : `Gateway indisponível${message ? `: ${message}` : ''}`;
}

/** Alimenta séries e log de eventos a partir de uma listagem nova. */
function ingest(sources) {
  for (const source of sources) {
    const added = store.addReading(source.sourceId, source.lastReadingMs, source.lastValues, source.lastReadingIsEvent);
    if (added && source.lastReadingIsEvent) {
      logEvent('event', `${source.sourceId}: evento imediato — ${lastValueText(source).replace(' ⚠ evento', '')}`, source.lastReadingMs);
    }
    const label = stateOf(source);
    const previous = state.previousStates.get(source.sourceId);
    if (previous && previous !== label) logEvent(label === 'OFFLINE' ? 'offline' : 'info', `${source.sourceId}: ${previous} → ${label}`);
    state.previousStates.set(source.sourceId, label);
  }
}

function logEvent(kind, text, atMs = Date.now()) {
  const list = $('events');
  list.prepend(el('li', { class: kind }, el('time', { textContent: timeOf(atMs) }), text));
  while (list.children.length > 100) list.lastChild.remove();
  $('events-empty').hidden = true;
}

// ── tabela de fontes ─────────────────────────────────────────────────────────

function renderSources() {
  const body = document.querySelector('#sources tbody');
  body.replaceChildren(
    ...state.sources.map((source) => {
      const label = stateOf(source);
      const classes = [source.sourceId === state.selectedId ? 'selected' : '', label === 'OFFLINE' ? 'offline' : '', source.lastReadingIsEvent ? 'event' : ''];
      const row = el('tr', { class: classes.filter(Boolean).join(' '), tabIndex: 0 });
      const select = () => selectSource(source.sourceId);
      row.addEventListener('click', select);
      row.addEventListener('keydown', (event) => (event.key === 'Enter' || event.key === ' ') && (event.preventDefault(), select()));
      const failure = FAILURE_LABEL[source.failure];
      row.append(
        el('td', { textContent: source.sourceId }),
        el('td', { textContent: source.type }),
        el('td', {}, el('span', { class: `badge ${label === 'ONLINE' ? 'ok' : label === 'OFFLINE' ? 'bad' : ''}`, textContent: label })),
        el('td', { textContent: source.poweredOn ? 'sim' : 'não' }),
        el('td', { textContent: source.controllable ? 'sim' : 'não' }),
        el('td', { textContent: lastValueText(source) }),
        el('td', { textContent: source.sendIntervalS ? `${fmt(source.sendIntervalS)} s${failure ? ` · ${failure}` : ''}` : '-' }),
        el('td', { textContent: 'threshold' in source ? fmt(source.threshold) : '-' }),
      );
      return row;
    }),
  );
  $('sources-empty').hidden = state.sources.length > 0;
}

// ── seleção, gráfico e comandos ──────────────────────────────────────────────

function selectedSource() {
  return state.sources.find((s) => s.sourceId === state.selectedId) ?? null;
}

async function selectSource(sourceId) {
  state.selectedId = sourceId;
  $('command-result').textContent = '';
  renderSources();
  renderCommands();
  await loadHistory(sourceId);
  refreshSelection();
}

/** Tenta o histórico do Gateway (#16) uma vez por fonte; sem ele, o gráfico usa as leituras acumuladas. */
async function loadHistory(sourceId) {
  if (state.historyChecked.has(sourceId)) return;
  state.historyChecked.add(sourceId);
  try {
    const { points } = await api(`/api/history/${encodeURIComponent(sourceId)}?fromMs=${Date.now() - HISTORY_WINDOW_MS}`);
    store.mergeHistory(sourceId, points);
  } catch (error) {
    if (error.status !== 501) state.historyChecked.delete(sourceId); // falha passageira: tenta de novo depois
  }
}

function refreshSelection() {
  const source = selectedSource();
  $('chart-title').textContent = source ? source.sourceId : 'selecione uma fonte';
  updateMetricSelect(source);
  drawChart(source);
  if (state.selectedId && !source) {
    state.selectedId = null;
    renderCommands();
  }
}

function updateMetricSelect(source) {
  const select = $('metric');
  const metrics = source ? store.metrics(source.sourceId) : [];
  const current = select.value;
  if (metrics.join('|') !== [...select.options].map((o) => o.value).join('|')) {
    select.replaceChildren(...metrics.map((m) => el('option', { value: m, textContent: m })));
    select.value = metrics.includes(current) ? current : (metrics[0] ?? '');
  }
}

function drawChart(source) {
  const metric = $('metric').value;
  const note = $('chart-note');
  if (!source || !metric) {
    destroyChart();
    note.textContent = source ? 'Aguardando a primeira leitura desta fonte…' : '';
    return;
  }
  const points = store.points(source.sourceId, metric);
  const unit = store.unit(source.sourceId, metric);
  const key = `${source.sourceId}|${metric}`;
  note.textContent = `${points.length} leituras · pontos vermelhos são eventos imediatos`;
  if (!state.chart || state.chartKey !== key) {
    destroyChart();
    state.chartKey = key;
    state.chart = new Chart($('chart'), {
      type: 'line',
      data: { datasets: [{ label: unit ? `${metric} (${unit})` : metric, data: points, borderColor: '#2563eb', backgroundColor: '#2563eb', tension: 0.2, pointRadius: eventRadius, pointBackgroundColor: eventColor }] },
      options: {
        animation: false,
        maintainAspectRatio: false,
        parsing: false,
        scales: {
          x: { type: 'linear', ticks: { callback: (value) => timeOf(value), maxTicksLimit: 6 } },
        },
        plugins: { tooltip: { callbacks: { title: (items) => timeOf(items[0].parsed.x) } } },
      },
    });
    return;
  }
  state.chart.data.datasets[0].data = points;
  state.chart.update('none');
}

const eventRadius = (context) => (context.raw?.event ? 6 : 3);
const eventColor = (context) => (context.raw?.event ? '#dc2626' : '#2563eb');

function destroyChart() {
  if (state.chart) state.chart.destroy();
  state.chart = null;
  state.chartKey = '';
}

function renderCommands() {
  const source = selectedSource();
  const panel = $('commands');
  const ok = Boolean(source?.controllable);
  panel.hidden = !ok;
  $('command-title').textContent = !source ? 'selecione uma fonte controlável' : ok ? source.sourceId : `${source.sourceId} não é controlável`;
  if (!ok) return;
  $('param-names').replaceChildren(...Object.keys(source.parameters).map((name) => el('option', { value: name })));
  if (source.sendIntervalS) panel.querySelector('[name=intervalS]').value = source.sendIntervalS;
  if ('threshold' in source) panel.querySelector('[name=threshold]').value = source.threshold;
}

async function sendCommand(command) {
  const source = selectedSource();
  const result = $('command-result');
  if (!source || state.commandBusy) return;
  state.commandBusy = true;
  setCommandsDisabled(true);
  try {
    const response = await api(`/api/sources/${encodeURIComponent(source.sourceId)}/command`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(command),
    });
    result.className = 'ok';
    result.textContent = `✔ ${response.message}`;
    await poll();
  } catch (error) {
    result.className = 'bad';
    result.textContent = `✖ ${error.message}`;
  } finally {
    state.commandBusy = false;
    setCommandsDisabled(false);
  }
}

function setCommandsDisabled(disabled) {
  for (const control of $('commands').querySelectorAll('button')) control.disabled = disabled;
}

function wireCommands() {
  for (const button of $('commands').querySelectorAll('button[data-power]')) {
    button.addEventListener('click', () => sendCommand({ power: { on: button.dataset.power === 'true' } }));
  }
  const builders = {
    interval: (f) => ({ interval: { intervalS: Number(f.intervalS.value) } }),
    threshold: (f) => ({ threshold: { threshold: Number(f.threshold.value) } }),
    parameter: (f) => ({ parameter: { name: f.name.value, value: Number(f.value.value) } }),
    failure: (f) => ({ failure: { mode: f.mode.value, durationS: Number(f.durationS.value || 0) } }),
  };
  for (const form of $('commands').querySelectorAll('form[data-form]')) {
    form.addEventListener('submit', (event) => {
      event.preventDefault();
      sendCommand(builders[form.dataset.form](form.elements));
    });
  }
}

// ── consultas agregadas ──────────────────────────────────────────────────────

async function loadConfig() {
  try {
    const { queries } = await api('/api/config');
    $('query-type').replaceChildren(...queries.map((q) => el('option', { value: q.name, textContent: `${q.label} (${q.name})` })));
  } catch (error) {
    // o painel de consultas fica vazio; o aviso de conexão já está no cabeçalho
  }
}

function wireQuery() {
  $('query-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    const params = new URLSearchParams();
    for (const [key, value] of new FormData(event.target)) if (String(value).trim() !== '') params.set(key, String(value).trim());
    const output = $('query-result');
    try {
      const result = await api(`/api/query?${params}`);
      output.replaceChildren(
        el('div', { class: 'summary', textContent: `${result.queryType}: ${fmt(result.value)} (${result.samples} leituras)` }),
        detailsTable(result.details),
      );
    } catch (error) {
      output.replaceChildren(el('div', { class: 'error', textContent: `✖ ${error.message}` }));
    }
  });
}

function detailsTable(details) {
  if (!details.length) return el('span');
  const head = el('tr', {}, ...['Fonte', 'Tipo', 'Grandeza', 'Valor', 'Leituras'].map((h) => el('th', { textContent: h })));
  const rows = details.map((d) => el('tr', {}, ...[d.sourceId, d.type, d.metric || '-', fmt(d.value), String(d.samples)].map((t) => el('td', { textContent: t }))));
  return el('table', {}, el('thead', {}, head), el('tbody', {}, ...rows));
}

// ── início ───────────────────────────────────────────────────────────────────

$('metric').addEventListener('change', () => drawChart(selectedSource()));
wireCommands();
wireQuery();
loadConfig();
poll();
setInterval(poll, POLL_MS);
