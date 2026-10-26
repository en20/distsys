'use strict';
// Menu interativo do CLI (#17). Recebe `ask`/`print` por injeção, então os testes o conduzem com
// respostas roteirizadas contra um Gateway falso.

const { GatewayUnavailableError } = require('../gateway-client');
const { QUERY_PRESETS } = require('../query-presets');
const { InputClosed } = require('./prompt');
const { renderSources, renderCommandResult, renderQueryResult, renderHistory, describeError } = require('./format');
const { UserError, parseNumber, parseNonNegativeInteger, findSource, requireControllable } = require('./validate');

const FAILURE_CHOICES = [
  { key: '1', mode: 'FAILURE_SILENT', label: 'silenciosa (para de enviar leituras)', timed: true },
  { key: '2', mode: 'FAILURE_OUT_OF_RANGE', label: 'valores fora da faixa', timed: true },
  { key: '3', mode: 'FAILURE_CRASH', label: 'queda do processo', timed: false },
  { key: '0', mode: 'FAILURE_NONE', label: 'remover falha', timed: false },
];

const MENU = `
──────── Cliente do Gateway ────────
 1) Listar fontes e estados
 2) Ligar / desligar fonte
 3) Alterar frequência de envio
 4) Ajustar limiar de alerta
 5) Ajustar parâmetro da fonte
 6) Simular falha
 7) Consultas agregadas
 8) Histórico de uma fonte (extra)
 0) Sair`;

const READ_ATTEMPTS = 3;

async function runMenu(ctx) {
  const { ask, print } = ctx;
  for (;;) {
    print(MENU);
    const choice = (await ask('Opção: ')).toLowerCase();
    if (choice === '0' || choice === 'q' || choice === 'sair') return;
    const action = ACTIONS[choice];
    if (!action) {
      print('✖ opção inválida');
      continue;
    }
    try {
      await action(ctx);
    } catch (error) {
      if (error instanceof InputClosed) throw error;
      print(describeError(error));
    }
  }
}

// ── leitura com nova tentativa ───────────────────────────────────────────────

/**
 * Consultas de leitura são seguras de repetir: se o Gateway não responde, tenta de novo
 * (cada tentativa reabre a conexão). Comandos não passam por aqui para não serem aplicados duas vezes.
 */
async function readWithRetry(ctx, fn) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (!(error instanceof GatewayUnavailableError) || attempt >= READ_ATTEMPTS) throw error;
      ctx.print(`⚠ ${error.message}\n  tentando reconectar (${attempt}/${READ_ATTEMPTS - 1})...`);
      await new Promise((resolve) => setTimeout(resolve, ctx.retryDelayMs ?? 1000));
    }
  }
}

const fetchSources = (ctx) => readWithRetry(ctx, () => ctx.client.listSources());

// ── seleção de fonte ─────────────────────────────────────────────────────────

async function chooseSource(ctx, { controllable = false } = {}) {
  const sources = await fetchSources(ctx);
  if (sources.length === 0) throw new UserError('nenhuma fonte conhecida pelo Gateway ainda');
  ctx.print(renderSources(sources, { color: ctx.color }));
  const source = findSource(sources, await ctx.ask('Fonte (id ou #): '));
  return controllable ? requireControllable(source) : source;
}

async function runCommand(ctx, source, command) {
  ctx.print(renderCommandResult(await ctx.client.sendCommand(source.sourceId, command)));
}

// ── ações do menu ────────────────────────────────────────────────────────────

const ACTIONS = {
  '1': async (ctx) => {
    ctx.print(renderSources(await fetchSources(ctx), { color: ctx.color }));
  },

  '2': async (ctx) => {
    const source = await chooseSource(ctx, { controllable: true });
    const answer = (await ctx.ask(`Ligar ou desligar "${source.sourceId}"? (l/d): `)).toLowerCase();
    if (['l', 'ligar', 'on', '1'].includes(answer)) return runCommand(ctx, source, { power: { on: true } });
    if (['d', 'desligar', 'off', '0'].includes(answer)) return runCommand(ctx, source, { power: { on: false } });
    throw new UserError('responda "l" para ligar ou "d" para desligar');
  },

  '3': async (ctx) => {
    const source = await chooseSource(ctx, { controllable: true });
    const intervalS = parseNumber(await ctx.ask('Novo intervalo de envio em segundos (ex.: 5): '), {
      label: 'intervalo',
      positive: true,
    });
    return runCommand(ctx, source, { interval: { intervalS } });
  },

  '4': async (ctx) => {
    const source = await chooseSource(ctx, { controllable: true });
    const threshold = parseNumber(await ctx.ask('Novo limiar de alerta: '), { label: 'limiar' });
    return runCommand(ctx, source, { threshold: { threshold } });
  },

  '5': async (ctx) => {
    const source = await chooseSource(ctx, { controllable: true });
    const known = Object.keys(source.parameters);
    if (known.length) ctx.print(`Parâmetros atuais: ${known.map((k) => `${k}=${source.parameters[k]}`).join(', ')}`);
    const name = await ctx.ask('Nome do parâmetro (ex.: vazao, intensidade, setpoint): ');
    if (name === '') throw new UserError('nome do parâmetro não informado');
    const value = parseNumber(await ctx.ask(`Valor de "${name}": `), { label: name });
    return runCommand(ctx, source, { parameter: { name, value } });
  },

  '6': async (ctx) => {
    const source = await chooseSource(ctx, { controllable: true });
    ctx.print(FAILURE_CHOICES.map((c) => ` ${c.key}) ${c.label}`).join('\n'));
    const key = await ctx.ask('Tipo de falha: ');
    const choice = FAILURE_CHOICES.find((c) => c.key === key);
    if (!choice) throw new UserError('tipo de falha inválido');
    let durationS = 0;
    if (choice.timed) {
      const text = await ctx.ask('Duração em segundos (0 = até novo comando): ');
      durationS = parseNonNegativeInteger(text === '' ? '0' : text, { label: 'duração' });
    }
    return runCommand(ctx, source, { failure: { mode: choice.mode, durationS } });
  },

  '7': async (ctx) => {
    ctx.print(QUERY_PRESETS.map((q, i) => ` ${i + 1}) ${q.label} [${q.name}]`).join('\n') + `\n ${QUERY_PRESETS.length + 1}) Outra (digitar o nome)`);
    const answer = await ctx.ask('Consulta: ');
    const preset = QUERY_PRESETS[Number(answer) - 1];
    let queryType = preset?.name;
    if (!queryType && Number(answer) === QUERY_PRESETS.length + 1) queryType = await ctx.ask('Nome da consulta: ');
    if (!queryType) throw new UserError('consulta inválida');
    const sourceId = await ctx.ask('Filtrar por id da fonte (Enter = todas): ');
    const sourceType = sourceId === '' ? await ctx.ask('Filtrar por tipo de fonte (Enter = todos): ') : '';
    const metric = await ctx.ask('Filtrar por grandeza (Enter = todas): ');
    const windowText = await ctx.ask('Janela em segundos (Enter = todo o histórico): ');
    const windowS = windowText === '' ? 0 : parseNonNegativeInteger(windowText, { label: 'janela' });
    ctx.print(renderQueryResult(await readWithRetry(ctx, () => ctx.client.query({ queryType, sourceId, sourceType, metric, windowS }))));
  },

  '8': async (ctx) => {
    const source = await chooseSource(ctx);
    const text = await ctx.ask('Últimos quantos minutos? (Enter = tudo): ');
    const fromMs = text === '' ? 0 : Date.now() - parseNumber(text, { label: 'minutos', positive: true }) * 60_000;
    const history = await readWithRetry(ctx, () => ctx.client.history({ sourceId: source.sourceId, fromMs: Math.round(fromMs) }));
    ctx.print(renderHistory(history));
  },
};

module.exports = { runMenu, FAILURE_CHOICES };
