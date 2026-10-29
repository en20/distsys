'use strict';
// Séries temporais acumuladas no navegador. Alimentadas pelo polling de /api/sources (última leitura
// de cada fonte) e, quando o Gateway tem histórico (#16), por /api/history. Sem DOM: testável no Node.

(function (root) {
  class SeriesStore {
    constructor(maxPointsPerSeries = 300) {
      this.maxPoints = maxPointsPerSeries;
      this.bySource = new Map(); // sourceId → Map(metric → { unit, points: Map(x → {x, y, event}) })
    }

    /** Registra uma leitura; leituras repetidas (mesmo timestamp) são ignoradas. Devolve true se era nova. */
    addReading(sourceId, timestampMs, measurements, isEvent = false) {
      if (!timestampMs) return false; // fonte que ainda não enviou nada
      let added = false;
      for (const m of measurements) added = this.addPoint(sourceId, m.metric, m.unit, timestampMs, m.value, isEvent) || added;
      return added;
    }

    /** Mescla pontos de HistoryResponse ({ timestampMs, measurements, isEvent }). */
    mergeHistory(sourceId, points) {
      for (const p of points) this.addReading(sourceId, p.timestampMs, p.measurements, p.isEvent);
    }

    addPoint(sourceId, metric, unit, x, y, event) {
      if (!Number.isFinite(x) || !Number.isFinite(y)) return false;
      let metrics = this.bySource.get(sourceId);
      if (!metrics) this.bySource.set(sourceId, (metrics = new Map()));
      let series = metrics.get(metric);
      if (!series) metrics.set(metric, (series = { unit: unit || '', points: new Map() }));
      if (series.points.has(x)) return false;
      series.points.set(x, { x, y, event: Boolean(event) });
      if (series.points.size > this.maxPoints) series.points.delete(Math.min(...series.points.keys()));
      return true;
    }

    metrics(sourceId) {
      return [...(this.bySource.get(sourceId)?.keys() ?? [])].sort();
    }

    unit(sourceId, metric) {
      return this.bySource.get(sourceId)?.get(metric)?.unit ?? '';
    }

    /** Pontos de uma grandeza ordenados no tempo: [{ x: epochMs, y: valor, event: bool }]. */
    points(sourceId, metric) {
      const series = this.bySource.get(sourceId)?.get(metric);
      return series ? [...series.points.values()].sort((a, b) => a.x - b.x) : [];
    }
  }

  if (typeof module !== 'undefined' && module.exports) module.exports = { SeriesStore };
  else root.SeriesStore = SeriesStore;
})(typeof window !== 'undefined' ? window : globalThis);
