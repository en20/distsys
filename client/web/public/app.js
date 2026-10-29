'use strict';
// Dashboard "Fazenda Conectada": polling de /api/sources, cartões por tipo de fonte, gráfico (Chart.js),
// comandos e consultas. Todo texto vindo do Gateway entra no DOM via textContent (ids e nomes de fontes
// não são confiáveis); innerHTML só recebe os ícones constantes deste arquivo.

const POLL_MS = 3000;
const QUERY_REFRESH_MS = 15000;
const HISTORY_WINDOW_MS = 30 * 60 * 1000;
const SPARK_POINTS = 40;

const ICONS = {
  valve: '<svg viewBox="0 0 24 24"><path d="M12 3s6 6.5 6 11a6 6 0 0 1-12 0c0-4.5 6-11 6-11z"/><path d="M9.5 14.5a2.5 2.5 0 0 0 2.5 2.5"/></svg>',
  tank: '<svg viewBox="0 0 24 24"><rect x="5" y="3" width="14" height="18" rx="2"/><path d="M5 13c2.3-1.4 4.7 1.4 7 0s4.7 1.4 7 0"/></svg>',
  soil: '<svg viewBox="0 0 24 24"><path d="M12 20v-8"/><path d="M12 12c0-4 3-6 7-6 0 4-3 6-7 6z"/><path d="M12 14c0-3-2.5-5-6-5 0 3 2.5 5 6 5z"/><path d="M4 20h16"/></svg>',
  weather: '<svg viewBox="0 0 24 24"><circle cx="9" cy="9" r="3.5"/><path d="M9 2.5V4M2.5 9H4M4.4 4.4l1 1M13.6 4.4l-1 1"/><path d="M8 19h9a3.5 3.5 0 0 0 0-7 5 5 0 0 0-9.5 1.5A2.8 2.8 0 0 0 8 19z"/></svg>',
  sensor: '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M5 12a7 7 0 0 1 14 0M2 12a10 10 0 0 1 20 0"/></svg>',
};

// Tipos do domínio (agricultura de precisão, issue #2); qualquer outro tipo cai em "Outras fontes".
const TYPES = {
  IRRIGATION_VALVE: { label: 'Válvula de irrigação', group: 'irrigacao', icon: 'valve', tone: 'water', primary: 'vazao' },
  RESERVOIR_LEVEL: { label: 'Nível do reservatório', group: 'reservatorio', icon: 'tank', tone: 'water', primary: 'nivel' },
  SOIL_MOISTURE: { label: 'Umidade do solo', group: 'talhoes', icon: 'soil', tone: 'soil', primary: 'umidade_solo' },
  WEATHER_STATION: { label: 'Estação meteorológica', group: 'clima', icon: 'weather', tone: 'weather', primary: 'temperatura' },
};
const OTHER_TYPE = { label: '', group: 'outras', icon: 'sensor', tone: '', primary: null };
const GROUPS = [
  { key: 'irrigacao', title: 'Irrigação' },
  { key: 'reservatorio', title: 'Reservatório' },
  { key: 'talhoes', title: 'Talhões · umidade do solo' },
  { key: 'clima', title: 'Clima' },
  { key: 'outras', title: 'Outras fontes' },
];
const METRIC_LABEL = {
  vazao: 'Vazão', volume: 'Volume', nivel: 'Nível', umidade_solo: 'Umidade do solo',
  temperatura: 'Temperatura', umidade_ar: 'Umidade do ar', chuva: 'Chuva', vento: 'Vento',
};
const PARAM_LABEL = { vazao_alvo_lpm: { label: 'Vazão alvo', unit: 'L/min', max: 100 } };
const STATE = {
  SOURCE_ONLINE: { label: 'ONLINE', chip: 'ok' },
  SOURCE_OFFLINE: { label: 'OFFLINE', chip: 'bad' },
  SOURCE_POWERED_OFF: { label: 'DESLIGADA', chip: 'off' },
};
const FAILURE_LABEL = {
  FAILURE_SILENT: 'falha silenciosa',
  FAILURE_OUT_OF_RANGE: 'valores fora da faixa',
  FAILURE_CRASH: 'queda do processo',
  FAILURE_NONE: 'normalizar',
};

// Consultas do domínio (requisito 5d), com os nomes do motor de consultas do Gateway (gateway/queries.py).
const QUICK_QUERIES = [
  { key: 'seco', title: 'Talhão mais seco', desc: 'menor umidade média do solo', queryType: 'menor_media', metric: 'umidade_solo', unit: '%', rank: 'asc' },
  { key: 'agua', title: 'Água irrigada', desc: 'soma do volume das válvulas', queryType: 'soma', metric: 'volume', unit: 'L', rank: 'desc' },
  { key: 'alarmes', title: 'Alarmes', desc: 'eventos imediatos (limiar do reservatório)', queryType: 'contagem_eventos', metric: '', unit: '', rank: 'events' },
  { key: 'temp', title: 'Temperatura média', desc: 'estação meteorológica', queryType: 'media', metric: 'temperatura', unit: '°C', rank: null },
];

const store = new SeriesStore();
const state = {
  sources: [],
  firstPoll: true,
  selectedId: null,
  metric: null,
  previousStates: new Map(),
  historyChecked: new Set(), // fontes cujo histórico já foi pedido (ou que o Gateway não suporta)
  chart: null,
  chartKey: '',
  busy: false,
};

const $ = (id) => document.getElementById(id);

function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (key === 'class') node.className = value;
    else if (key === 'dataset') Object.assign(node.dataset, value);
    else if (key === 'style') node.style.cssText = value;
    else node[key] = value;
  }
  for (const child of children) if (child !== null && child !== undefined) node.append(child);
  return node;
}

function icon(name, tone = '') {
  const span = el('span', { class: `type-icon ${tone}` });
  span.innerHTML = ICONS[name] ?? ICONS.sensor; // constante, nunca dado do Gateway
  return span;
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

// ── formatação ───────────────────────────────────────────────────────────────

const typeOf = (source) => TYPES[source.type] ?? { ...OTHER_TYPE, label: source.type };
const stateOf = (source) => STATE[source.state] ?? { label: '?', chip: '' };
const isOffline = (source) => source.state === 'SOURCE_OFFLINE';
const timeOf = (ms) => new Date(ms).toLocaleTimeString('pt-BR');
const metricLabel = (metric) => METRIC_LABEL[metric] ?? metric;

function fmt(value, digits = 1) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return value.toLocaleString('pt-BR', { maximumFractionDigits: Math.abs(value) < 10 ? Math.max(digits, 2) : digits });
}

function valueOf(source, metric) {
  return source.lastValues.find((m) => m.metric === metric) ?? null;
}

function ago(ms) {
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 60) return `há ${s} s`;
  if (s < 3600) return `há ${Math.round(s / 60)} min`;
  return `há ${Math.round(s / 3600)} h`;
}

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

// ── polling ──────────────────────────────────────────────────────────────────

async function poll() {
  try {
    const { sources } = await api('/api/sources');
    state.sources = sources;
    setConnection(true);
    const first = state.firstPoll;
    ingest(sources);
    if (first) state.selectedId = initialSelection(sources);
    render();
    if (first) fillControls(selectedSource());
    $('updated').textContent = `atualizado às ${timeOf(Date.now())}`;
    for (const source of sources) loadHistory(source.sourceId);
  } catch (error) {
    setConnection(false, error.message);
  }
}

function setConnection(ok, message = '') {
  const pill = $('connection');
  pill.className = `pill ${ok ? 'ok' : 'bad'}`;
  pill.querySelector('.pill-text').textContent = ok ? 'Gateway conectado' : 'Gateway indisponível';
  pill.title = message;
}

/** Alimenta séries e linha do tempo a partir de uma listagem nova. */
function ingest(sources) {
  const seen = new Set();
  for (const source of sources) {
    seen.add(source.sourceId);
    const added = store.addReading(source.sourceId, source.lastReadingMs, source.lastValues, source.lastReadingIsEvent);
    if (added && source.lastReadingIsEvent && !state.firstPoll) logEvent('event', eventText(source), source.lastReadingMs);

    const label = stateOf(source).label;
    const previous = state.previousStates.get(source.sourceId);
    if (previous === undefined && !state.firstPoll) logEvent('online', `${source.sourceId} descoberta (${typeOf(source).label})`);
    if (previous && previous !== label) {
      logEvent(label === 'OFFLINE' ? 'offline' : label === 'ONLINE' ? 'online' : 'info', `${source.sourceId}: ${previous} → ${label}`);
    }
    state.previousStates.set(source.sourceId, label);
  }
  for (const id of state.previousStates.keys()) if (!seen.has(id)) state.previousStates.delete(id);
  state.firstPoll = false;
}

function eventText(source) {
  const primary = typeOf(source).primary;
  const reading = primary ? valueOf(source, primary) : source.lastValues[0];
  if (!reading) return `${source.sourceId}: evento imediato`;
  const value = `${metricLabel(reading.metric)} ${fmt(reading.value)} ${reading.unit}`.trim();
  if ('threshold' in source) {
    const below = reading.value < source.threshold;
    return `${source.sourceId}: ${value} — ${below ? 'abaixo do limiar' : 'voltou ao normal (limiar'} ${fmt(source.threshold)}${below ? '' : ')'}`;
  }
  return `${source.sourceId}: evento imediato — ${value}`;
}

function logEvent(kind, text, atMs = Date.now()) {
  const list = $('events');
  list.prepend(el('li', { class: kind }, el('time', { textContent: timeOf(atMs) }), el('span', { class: 'mark' }), el('span', { textContent: text })));
  while (list.children.length > 150) list.lastChild.remove();
  $('events-empty').hidden = true;
}

/** Histórico do Gateway (#16) uma vez por fonte; sem ele, as séries usam só as leituras do polling. */
async function loadHistory(sourceId) {
  if (state.historyChecked.has(sourceId)) return;
  state.historyChecked.add(sourceId);
  try {
    const { points } = await api(`/api/history/${encodeURIComponent(sourceId)}?fromMs=${Date.now() - HISTORY_WINDOW_MS}`);
    store.mergeHistory(sourceId, points);
    render();
  } catch (error) {
    if (error.status !== 501) state.historyChecked.delete(sourceId); // falha passageira: tenta de novo depois
  }
}

function render() {
  renderKpis();
  renderGroups();
  renderDetail();
}

// ── indicadores ──────────────────────────────────────────────────────────────

function setKpi(id, value, sub, alert = false) {
  const card = $(id);
  card.querySelector('.kpi-value').textContent = value;
  card.querySelector('.kpi-sub').textContent = sub;
  card.classList.toggle('alert-on', alert);
}

function renderKpis() {
  const all = state.sources;
  const live = all.filter((s) => !isOffline(s));
  const count = (st) => all.filter((s) => s.state === st).length;
  const offline = count('SOURCE_OFFLINE');
  const off = count('SOURCE_POWERED_OFF');
  setKpi('kpi-online', all.length ? `${count('SOURCE_ONLINE')} / ${all.length}` : '—',
    all.length ? [offline && `${offline} offline`, off && `${off} desligada${off > 1 ? 's' : ''}`].filter(Boolean).join(' · ') || 'todas respondendo' : 'aguardando descoberta',
    offline > 0);

  const soils = live.map((s) => ({ s, m: valueOf(s, 'umidade_solo') })).filter((x) => x.m);
  if (soils.length) {
    const avg = soils.reduce((sum, x) => sum + x.m.value, 0) / soils.length;
    const driest = soils.reduce((a, b) => (b.m.value < a.m.value ? b : a));
    setKpi('kpi-soil', `${fmt(avg)} %`, `mais seco: ${driest.s.sourceId} (${fmt(driest.m.value)} %)`);
  } else setKpi('kpi-soil', '—', 'sem sensores de solo');

  const tank = all.find((s) => s.type === 'RESERVOIR_LEVEL' && valueOf(s, 'nivel'));
  if (tank) {
    const level = valueOf(tank, 'nivel').value;
    const low = 'threshold' in tank && level < tank.threshold;
    setKpi('kpi-reservoir', `${fmt(level)} %`, 'threshold' in tank ? `limiar ${fmt(tank.threshold)} %${low ? ' · ABAIXO' : ''}` : tank.sourceId, low || isOffline(tank));
  } else setKpi('kpi-reservoir', '—', 'sem reservatório');

  const valves = all.filter((s) => s.type === 'IRRIGATION_VALVE');
  if (valves.length) {
    const open = valves.filter((s) => s.poweredOn && !isOffline(s));
    const flow = open.reduce((sum, s) => sum + (valueOf(s, 'vazao')?.value ?? 0), 0);
    setKpi('kpi-flow', `${fmt(flow)} L/min`, `${open.length} de ${valves.length} válvulas abertas`);
  } else setKpi('kpi-flow', '—', 'sem válvulas');

  const station = live.find((s) => s.type === 'WEATHER_STATION');
  if (station) {
    const part = (metric, unit) => (valueOf(station, metric) ? `${fmt(valueOf(station, metric).value)} ${unit}` : null);
    const rain = valueOf(station, 'chuva')?.value ?? 0;
    setKpi('kpi-weather', part('temperatura', '°C') ?? '—',
      [rain > 0 ? `chovendo ${fmt(rain)} mm/h` : 'sem chuva', part('umidade_ar', '% ar'), part('vento', 'km/h')].filter(Boolean).join(' · '));
  } else setKpi('kpi-weather', '—', 'sem estação');
}

// ── cartões ──────────────────────────────────────────────────────────────────

const cardNodes = new Map(); // sourceId → <button>; reaproveitados para um clique não cair num nó recém-trocado

/** Monta os grupos só quando a composição muda; no resto do tempo atualiza cada cartão no lugar. */
function renderGroups() {
  const container = $('groups');
  const byGroup = new Map(GROUPS.map((g) => [g.key, []]));
  for (const source of [...state.sources].sort((a, b) => a.sourceId.localeCompare(b.sourceId, 'pt-BR', { numeric: true }))) {
    byGroup.get(typeOf(source).group).push(source);
  }
  for (const source of state.sources) fillCard(cardNode(source.sourceId), source);
  const layout = GROUPS.map((g) => `${g.key}:${byGroup.get(g.key).map((s) => s.sourceId).join(',')}`).join('|');
  if (container.dataset.layout !== layout) {
    container.dataset.layout = layout;
    container.replaceChildren(
      ...GROUPS.filter((g) => byGroup.get(g.key).length).map((g) =>
        el('div', { class: 'group', style: `--n:${byGroup.get(g.key).length}` },
          el('div', { class: 'group-title' }, g.title, el('span', { class: 'count', textContent: String(byGroup.get(g.key).length) })),
          el('div', { class: 'cards' }, ...byGroup.get(g.key).map((s) => cardNode(s.sourceId))))),
    );
    for (const id of cardNodes.keys()) if (!state.sources.some((s) => s.sourceId === id)) cardNodes.delete(id);
  }
  $('empty').hidden = state.sources.length > 0;
}

function cardNode(sourceId) {
  let node = cardNodes.get(sourceId);
  if (!node) {
    node = el('button', { type: 'button', class: 'card', dataset: { sourceId } });
    node.addEventListener('click', () => selectSource(sourceId));
    cardNodes.set(sourceId, node);
  }
  return node;
}

function fillCard(node, source) {
  const type = typeOf(source);
  const st = stateOf(source);
  const classes = ['card', source.sourceId === state.selectedId && 'selected', isOffline(source) && 'offline', source.lastReadingIsEvent && 'event'];
  node.className = classes.filter(Boolean).join(' ');
  node.setAttribute('aria-pressed', String(source.sourceId === state.selectedId));
  node.replaceChildren();

  const primary = type.primary ? valueOf(source, type.primary) : source.lastValues[0];
  node.append(
    el('div', { class: 'card-head' },
      icon(type.icon, type.tone),
      el('div', {}, el('div', { class: 'name', textContent: source.sourceId }), el('div', { class: 'type', textContent: type.label })),
      el('span', { class: `chip ${st.chip}`, textContent: st.label })),
    el('div', { class: 'readout' },
      el('span', { class: 'big', textContent: primary ? fmt(primary.value) : '—' }),
      el('span', { class: 'unit', textContent: primary ? `${primary.unit} · ${metricLabel(primary.metric).toLowerCase()}` : 'aguardando leitura' })),
  );

  const level = cardGauge(source);
  if (level) node.append(level);
  const secondary = source.lastValues.filter((m) => m !== primary);
  if (source.type === 'IRRIGATION_VALVE' && 'vazao_alvo_lpm' in source.parameters) {
    node.append(el('div', { class: 'secondary' },
      el('span', {}, 'alvo ', el('b', { textContent: `${fmt(source.parameters.vazao_alvo_lpm)} L/min` })),
      ...secondary.map((m) => el('span', {}, `${metricLabel(m.metric).toLowerCase()} `, el('b', { textContent: `${fmt(m.value)} ${m.unit}` })))));
  } else if (secondary.length) {
    node.append(el('div', { class: 'secondary' },
      ...secondary.map((m) => el('span', {}, `${metricLabel(m.metric).toLowerCase()} `, el('b', { textContent: `${fmt(m.value)} ${m.unit}` })))));
  }

  if (primary) node.append(sparkline(store.points(source.sourceId, primary.metric).slice(-SPARK_POINTS), type.tone));

  const tags = [];
  if (source.controllable) tags.push(el('span', { class: 'tag', textContent: 'controlável' }));
  if (source.sendIntervalS) tags.push(el('span', { class: 'tag', textContent: `a cada ${fmt(source.sendIntervalS)} s` }));
  if (FAILURE_LABEL[source.failure] && source.failure !== 'FAILURE_NONE') tags.push(el('span', { class: 'tag danger', textContent: FAILURE_LABEL[source.failure] }));
  if (source.lastReadingIsEvent) tags.push(el('span', { class: 'tag danger', textContent: 'alarme' }));
  if (isOffline(source)) tags.push(el('span', { class: 'tag warn', textContent: `sem sinal ${ago(source.lastSeenMs)}` }));
  node.append(el('div', { class: 'tags' }, ...tags));
}

/** Barra de nível para solo (seco → úmido) e reservatório (com a marca do limiar). */
function cardGauge(source) {
  if (source.type === 'SOIL_MOISTURE') {
    const m = valueOf(source, 'umidade_solo');
    if (!m) return null;
    const fill = m.value < 25 ? 'var(--danger)' : m.value < 35 ? 'var(--warn)' : m.value > 60 ? 'var(--water)' : 'var(--brand)';
    return el('div', { class: 'bar', style: `--fill:${fill}`, title: 'seco ← → úmido' }, el('span', { style: `width:${clampPct(m.value)}%` }));
  }
  if (source.type === 'RESERVOIR_LEVEL') {
    const m = valueOf(source, 'nivel');
    if (!m) return null;
    const bar = el('div', { class: 'bar', style: '--fill:var(--water)' }, el('span', { style: `width:${clampPct(m.value)}%` }));
    if ('threshold' in source) bar.append(el('i', { style: `left:${clampPct(source.threshold)}%`, title: `limiar ${fmt(source.threshold)} %` }));
    return bar;
  }
  return null;
}

const clampPct = (v) => Math.max(0, Math.min(100, v));

function sparkline(points, tone) {
  const NS = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(NS, 'svg');
  svg.setAttribute('viewBox', '0 0 200 36');
  svg.setAttribute('preserveAspectRatio', 'none');
  svg.setAttribute('class', 'spark');
  svg.setAttribute('aria-hidden', 'true');
  if (tone === 'water') svg.style.setProperty('--spark', 'var(--water)');
  if (tone === 'soil') svg.style.setProperty('--spark', 'var(--warn)');
  if (tone === 'weather') svg.style.setProperty('--spark', '#8a6cc4');
  if (points.length < 2) return svg;

  const ys = points.map((p) => p.y);
  let min = Math.min(...ys);
  let max = Math.max(...ys);
  if (max - min < 1e-9) { min -= 1; max += 1; }
  // Espaçamento por índice: buracos no tempo (fonte parada, execução anterior) não espremem a curva.
  const xy = points.map((p, i) => [(i / (points.length - 1)) * 200, 33 - ((p.y - min) / (max - min)) * 30]);
  const line = xy.map(([x, y], i) => `${i ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
  const area = document.createElementNS(NS, 'path');
  area.setAttribute('class', 'area');
  area.setAttribute('d', `${line} L200,36 L0,36 Z`);
  const path = document.createElementNS(NS, 'path');
  path.setAttribute('class', 'line');
  path.setAttribute('d', line);
  path.setAttribute('vector-effect', 'non-scaling-stroke');
  svg.append(area, path);
  points.forEach((p, i) => {
    if (!p.event) return;
    const dot = document.createElementNS(NS, 'circle');
    dot.setAttribute('cx', xy[i][0]);
    dot.setAttribute('cy', xy[i][1]);
    dot.setAttribute('r', '3');
    svg.append(dot);
  });
  return svg;
}

// ── detalhe: gráfico e comandos ──────────────────────────────────────────────

/** Fonte aberta ao carregar: a do link (#fonte=<id>) ou a primeira na ordem dos grupos. */
function initialSelection(sources) {
  const wanted = new URLSearchParams(location.hash.slice(1)).get('fonte');
  if (wanted && sources.some((s) => s.sourceId === wanted)) return wanted;
  const order = (s) => GROUPS.findIndex((g) => g.key === typeOf(s).group);
  const sorted = [...sources].sort((a, b) => order(a) - order(b) || a.sourceId.localeCompare(b.sourceId, 'pt-BR', { numeric: true }));
  return sorted[0]?.sourceId ?? null;
}

const selectedSource = () => state.sources.find((s) => s.sourceId === state.selectedId) ?? null;

function selectSource(sourceId) {
  if (state.selectedId === sourceId) return;
  state.selectedId = sourceId;
  state.metric = null;
  history.replaceState(null, '', `#fonte=${encodeURIComponent(sourceId)}`);
  destroyChart();
  renderGroups();
  renderDetail();
  fillControls(selectedSource());
  if (window.matchMedia('(max-width: 1180px)').matches) $('detail').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function renderDetail() {
  const source = selectedSource();
  if (state.selectedId && !source) state.selectedId = null; // a fonte sumiu do registro
  $('detail-empty').hidden = Boolean(source);
  $('detail-body').hidden = !source;
  if (!source) return destroyChart();

  const type = typeOf(source);
  const st = stateOf(source);
  $('detail-icon').replaceWith(Object.assign(icon(type.icon, type.tone), { id: 'detail-icon' }));
  $('detail-title').textContent = source.sourceId;
  $('detail-sub').textContent = [type.label, source.controllable ? `gRPC ${source.ip}:${source.grpcPort}` : `${source.ip} · sensor contínuo`, source.sendIntervalS && `envia a cada ${fmt(source.sendIntervalS)} s`].filter(Boolean).join(' · ');
  $('detail-state').className = `chip ${st.chip}`;
  $('detail-state').textContent = st.label;

  const alert = $('detail-alert');
  const failure = source.failure && source.failure !== 'FAILURE_NONE' ? FAILURE_LABEL[source.failure] : null;
  const notes = [];
  if (isOffline(source)) notes.push(`Sem sinal ${ago(source.lastSeenMs)}: o Gateway marcou a fonte como OFFLINE.`);
  if (failure) notes.push(`Falha simulada ativa: ${failure}${source.failureUntilMs > 0 ? ` até ${timeOf(source.failureUntilMs)}` : ''}.`);
  if (source.state === 'SOURCE_POWERED_OFF') notes.push('Desligada por comando: não envia leituras até ser religada.');
  alert.hidden = notes.length === 0;
  alert.className = `alert ${isOffline(source) || failure ? '' : 'warn'}`;
  alert.textContent = notes.join(' ');

  renderMetricTabs(source);
  drawChart(source);

  $('controls').hidden = !source.controllable;
  $('readonly-note').hidden = source.controllable;
  if (source.controllable) updateLiveControls(source);
}

function orderedMetrics(source) {
  const metrics = store.metrics(source.sourceId);
  const primary = typeOf(source).primary;
  return metrics.includes(primary) ? [primary, ...metrics.filter((m) => m !== primary)] : metrics;
}

function renderMetricTabs(source) {
  const metrics = orderedMetrics(source);
  if (!metrics.includes(state.metric)) state.metric = metrics[0] ?? null;
  const tabs = $('metric-tabs');
  if (tabs.dataset.key === `${source.sourceId}|${metrics.join(',')}|${state.metric}`) return;
  tabs.dataset.key = `${source.sourceId}|${metrics.join(',')}|${state.metric}`;
  tabs.replaceChildren(...metrics.map((metric) => {
    const button = el('button', { type: 'button', textContent: metricLabel(metric) });
    button.setAttribute('role', 'tab');
    button.setAttribute('aria-selected', String(metric === state.metric));
    button.addEventListener('click', () => { state.metric = metric; renderDetail(); });
    return button;
  }));
}

function drawChart(source) {
  const metric = state.metric;
  const note = $('chart-note');
  if (!metric) {
    destroyChart();
    note.textContent = 'Aguardando a primeira leitura desta fonte…';
    return;
  }
  const points = store.points(source.sourceId, metric);
  const unit = store.unit(source.sourceId, metric);
  const tone = typeOf(source).tone;
  const color = cssVar(tone === 'water' ? '--water' : tone === 'soil' ? '--warn' : '--brand');
  const showThreshold = 'threshold' in source && metric === typeOf(source).primary && points.length > 0;
  const datasets = [{
    label: unit ? `${metricLabel(metric)} (${unit})` : metricLabel(metric),
    data: withGaps(points, source.sendIntervalS), borderColor: color, backgroundColor: `${color}22`, fill: true, tension: 0.3, borderWidth: 2,
    pointRadius: (ctx) => (ctx.raw?.event ? 5 : 0), pointHoverRadius: 5,
    pointBackgroundColor: (ctx) => (ctx.raw?.event ? cssVar('--danger') : color),
  }];
  if (showThreshold) {
    datasets.push({
      label: 'limiar', data: [{ x: points[0].x, y: source.threshold }, { x: points[points.length - 1].x, y: source.threshold }],
      borderColor: cssVar('--danger'), borderDash: [6, 4], borderWidth: 1.5, pointRadius: 0, fill: false,
    });
  }
  const events = points.filter((p) => p.event).length;
  note.textContent = `${points.length} leituras nos últimos ${HISTORY_WINDOW_MS / 60000} min ou desde que a página abriu${events ? ` · ${events} alarme(s) em vermelho` : ''}`;

  const key = `${source.sourceId}|${metric}|${showThreshold}|${document.documentElement.dataset.theme ?? ''}`;
  if (state.chart && state.chartKey === key) {
    state.chart.data.datasets.forEach((ds, i) => { ds.data = datasets[i].data; });
    state.chart.update('none');
    return;
  }
  destroyChart();
  state.chartKey = key;
  const grid = cssVar('--border');
  const ticks = cssVar('--muted');
  state.chart = new Chart($('chart'), {
    type: 'line',
    data: { datasets },
    options: {
      animation: false,
      maintainAspectRatio: false,
      parsing: false,
      interaction: { mode: 'nearest', axis: 'x', intersect: false },
      scales: {
        x: { type: 'linear', grid: { color: grid }, ticks: { color: ticks, callback: (value) => new Date(value).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' }), maxTicksLimit: 6 } },
        y: { grid: { color: grid }, ticks: { color: ticks } },
      },
      plugins: {
        legend: { display: showThreshold, labels: { color: ticks, boxWidth: 12 } },
        tooltip: { callbacks: { title: (items) => timeOf(items[0].parsed.x) } },
      },
    },
  });
}

/** Quebra a linha onde faltam leituras (fonte parada, offline ou execução anterior) em vez de ligar os pontos. */
function withGaps(points, intervalS) {
  const maxGapMs = Math.max(3 * (intervalS || 5), 15) * 1000;
  const out = [];
  points.forEach((p, i) => {
    if (i && p.x - points[i - 1].x > maxGapMs) out.push({ x: (p.x + points[i - 1].x) / 2, y: null });
    out.push(p);
  });
  return out;
}

function destroyChart() {
  if (state.chart) state.chart.destroy();
  state.chart = null;
  state.chartKey = '';
}

/** Estado que muda sozinho (energia, intervalo ativo) — atualizado a cada polling sem mexer no que o usuário digita. */
function updateLiveControls(source) {
  const power = $('power');
  power.setAttribute('aria-checked', String(source.poweredOn));
  $('power-hint').textContent = source.poweredOn ? 'ligada — enviando leituras' : 'desligada — não envia leituras';
  for (const chip of $('interval-presets').querySelectorAll('[data-interval]')) {
    chip.classList.toggle('active', Number(chip.dataset.interval) === source.sendIntervalS);
  }
  $('threshold-control').hidden = !('threshold' in source);
}

/** Preenche os campos com o estado atual; só na seleção e depois de um comando aplicado. */
function fillControls(source) {
  if (!source?.controllable) return;
  $('f-interval').value = source.sendIntervalS || '';
  if ('threshold' in source) {
    $('f-threshold').value = source.threshold;
    $('f-threshold-range').value = source.threshold;
    $('threshold-out').textContent = `${fmt(source.threshold)}`;
  }
  const params = $('parameters');
  const key = `${source.sourceId}|${Object.keys(source.parameters).join(',')}`;
  if (params.dataset.key !== key) {
    params.dataset.key = key;
    params.replaceChildren(...Object.entries(source.parameters).map(([name, value]) => parameterControl(name, value)));
    return;
  }
  for (const form of params.querySelectorAll('form[data-param]')) form.setValue(source.parameters[form.dataset.param]);
}

function parameterControl(name, value) {
  const meta = PARAM_LABEL[name] ?? { label: name, unit: '', max: Math.max(100, value * 2) };
  const out = el('output', { textContent: `${fmt(value)} ${meta.unit}`.trim() });
  const number = el('input', { type: 'number', step: 'any', value: String(value), required: true });
  number.setAttribute('aria-label', meta.label);
  const range = el('input', { type: 'range', min: '0', max: String(meta.max), step: '1', value: String(value) });
  range.setAttribute('aria-label', `${meta.label} (deslizante)`);
  range.addEventListener('input', () => { number.value = range.value; out.textContent = `${range.value} ${meta.unit}`.trim(); });
  number.addEventListener('input', () => { range.value = number.value; });
  const form = el('form', { class: 'control', dataset: { param: name } },
    el('span', { class: 'control-label' }, `${meta.label} `, out),
    el('div', { class: 'inline' }, range, number, el('button', { class: 'button', textContent: 'Aplicar' })));
  form.setValue = (v) => { number.value = String(v); range.value = String(v); out.textContent = `${fmt(v)} ${meta.unit}`.trim(); };
  form.addEventListener('submit', (event) => {
    event.preventDefault();
    sendCommand({ parameter: { name, value: Number(number.value) } }, `${meta.label} → ${number.value} ${meta.unit}`.trim());
  });
  return form;
}

async function sendCommand(command, description) {
  const source = selectedSource();
  if (!source || state.busy) return;
  state.busy = true;
  setControlsDisabled(true);
  try {
    const response = await api(`/api/sources/${encodeURIComponent(source.sourceId)}/command`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(command),
    });
    toast(`✔ ${source.sourceId}: ${description}`);
    logEvent('command', `${source.sourceId}: ${description}`);
    await poll();
    fillControls(selectedSource() ?? response.source);
  } catch (error) {
    toast(`✖ ${source.sourceId}: ${error.message}`, true);
    logEvent('offline', `${source.sourceId}: comando recusado — ${error.message}`);
    await poll();
  } finally {
    state.busy = false;
    setControlsDisabled(false);
  }
}

function setControlsDisabled(disabled) {
  for (const control of $('controls').querySelectorAll('button')) control.disabled = disabled;
}

function wireControls() {
  $('power').addEventListener('click', () => {
    const on = $('power').getAttribute('aria-checked') !== 'true';
    sendCommand({ power: { on } }, on ? 'ligada' : 'desligada');
  });
  for (const chip of $('interval-presets').querySelectorAll('[data-interval]')) {
    chip.addEventListener('click', () => sendCommand({ interval: { intervalS: Number(chip.dataset.interval) } }, `intervalo → ${chip.dataset.interval} s`));
  }
  document.querySelector('[data-form=interval]').addEventListener('submit', (event) => {
    event.preventDefault();
    const seconds = Number($('f-interval').value);
    sendCommand({ interval: { intervalS: seconds } }, `intervalo → ${seconds} s`);
  });
  $('f-threshold-range').addEventListener('input', () => {
    $('f-threshold').value = $('f-threshold-range').value;
    $('threshold-out').textContent = $('f-threshold-range').value;
  });
  $('f-threshold').addEventListener('input', () => { $('f-threshold-range').value = $('f-threshold').value; });
  $('threshold-control').addEventListener('submit', (event) => {
    event.preventDefault();
    const value = Number($('f-threshold').value);
    sendCommand({ threshold: { threshold: value } }, `limiar → ${value}`);
  });
  const failureForm = document.querySelector('[data-form=failure]');
  failureForm.addEventListener('submit', (event) => event.preventDefault());
  for (const button of failureForm.querySelectorAll('[data-failure]')) {
    button.addEventListener('click', () => {
      const mode = button.dataset.failure;
      if (mode === 'FAILURE_CRASH' && !window.confirm('Derrubar encerra o processo da fonte. Para voltar, será preciso iniciá-la de novo. Continuar?')) return;
      const durationS = mode === 'FAILURE_NONE' || mode === 'FAILURE_CRASH' ? 0 : Math.max(0, Math.round(Number(failureForm.elements.durationS.value) || 0));
      sendCommand({ failure: { mode, durationS } }, `${FAILURE_LABEL[mode]}${durationS ? ` por ${durationS} s` : ''}`);
    });
  }
}

// ── consultas ────────────────────────────────────────────────────────────────

function renderQueryCards() {
  $('quick-queries').replaceChildren(...QUICK_QUERIES.map((q) => {
    const button = el('button', { type: 'button', class: 'button ghost', textContent: 'Atualizar' });
    button.addEventListener('click', () => runQuickQuery(q));
    return el('article', { class: 'query-card', id: `q-${q.key}` },
      el('div', { class: 'q-title' }, el('span', { textContent: q.title }), el('code', { textContent: q.queryType })),
      el('div', { class: 'q-desc', textContent: q.desc }),
      el('div', { class: 'q-value', textContent: '…' }),
      el('div', { class: 'q-detail' }),
      el('div', { class: 'q-rank' }),
      button);
  }));
}

async function runQuickQuery(q) {
  const card = $(`q-${q.key}`);
  const params = new URLSearchParams({ queryType: q.queryType, windowS: $('query-window').value });
  if (q.metric) params.set('metric', q.metric);
  const [value, detail, rank] = ['.q-value', '.q-detail', '.q-rank'].map((s) => card.querySelector(s));
  try {
    const result = await api(`/api/query?${params}`);
    rank.replaceChildren();
    if (!result.samples) {
      value.textContent = '—';
      detail.textContent = 'sem leituras na janela';
      return;
    }
    if (q.rank === 'events') {
      value.textContent = String(Math.round(result.value));
      const hits = result.details.filter((d) => d.value > 0);
      detail.textContent = hits.length ? `em ${hits.length} fonte(s) · ${result.samples} leituras` : `nenhum alarme · ${result.samples} leituras`;
      rank.replaceChildren(...rankRows(hits, '', 'var(--danger)'));
      return;
    }
    value.textContent = `${fmt(result.value)} ${q.unit}`.trim();
    const top = result.details[0];
    detail.textContent = q.rank === 'asc' && top ? `${top.sourceId} · ${result.samples} leituras` : `${result.samples} leituras`;
    if (q.rank) rank.replaceChildren(...rankRows(result.details, q.unit, q.rank === 'asc' ? 'var(--warn)' : 'var(--water)'));
  } catch (error) {
    value.textContent = '—';
    detail.textContent = error.message;
    rank.replaceChildren();
  }
}

function rankRows(details, unit, color) {
  const max = Math.max(...details.map((d) => d.value), 1e-9);
  return details.slice(0, 4).map((d) => el('div', { class: 'q-rank-row' },
    el('span', { textContent: d.sourceId, title: d.sourceId }),
    el('div', { class: 'bar', style: `--fill:${color}` }, el('span', { style: `width:${clampPct((d.value / max) * 100)}%` })),
    el('span', { textContent: `${fmt(d.value)} ${unit}`.trim() })));
}

function runQuickQueries() {
  for (const q of QUICK_QUERIES) runQuickQuery(q);
}

async function loadConfig() {
  try {
    const { queries } = await api('/api/config');
    $('query-type').replaceChildren(...queries.map((q) => el('option', { value: q.name, textContent: `${q.label} (${q.name})` })));
    // As consultas do domínio também podem ser escolhidas aqui.
    for (const q of QUICK_QUERIES) {
      if (!queries.some((p) => p.name === q.queryType)) $('query-type').append(el('option', { value: q.queryType, textContent: `${q.title} (${q.queryType})` }));
    }
  } catch (error) {
    // o painel fica vazio; o aviso de conexão já está no cabeçalho
  }
}

function wireQuery() {
  $('query-window').addEventListener('change', runQuickQueries);
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

// ── avisos e tema ────────────────────────────────────────────────────────────

function toast(text, bad = false) {
  const node = el('div', { class: `toast ${bad ? 'bad' : ''}`, textContent: text });
  $('toasts').append(node);
  setTimeout(() => node.remove(), bad ? 7000 : 4000);
}

function wireTheme() {
  const root = document.documentElement;
  try {
    const saved = localStorage.getItem('theme');
    if (saved === 'light' || saved === 'dark') root.dataset.theme = saved;
  } catch (error) { /* sem armazenamento: segue o sistema */ }
  $('theme-toggle').addEventListener('click', () => {
    const dark = root.dataset.theme ? root.dataset.theme === 'dark' : window.matchMedia('(prefers-color-scheme: dark)').matches;
    root.dataset.theme = dark ? 'light' : 'dark';
    try { localStorage.setItem('theme', root.dataset.theme); } catch (error) { /* ignora */ }
    destroyChart();
    renderDetail();
  });
}

// ── início ───────────────────────────────────────────────────────────────────

wireTheme();
wireControls();
wireQuery();
renderQueryCards();
loadConfig();
poll().then(runQuickQueries);
setInterval(poll, POLL_MS);
setInterval(runQuickQueries, QUERY_REFRESH_MS);
