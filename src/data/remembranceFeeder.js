/**
 * Remembrance feeder — the agent that listens to the globe and lets the
 * pipeline remember it. Subscribes to the ONE seam every layer already
 * flows through (contextStore.js) and turns the live world picture into
 * durable, coherence-scored records in the Remembrance field:
 *
 *   gev:layer:<layerId>    one AGGREGATE snapshot record per layer per
 *                          cycle (upsert by stable id): entity count and
 *                          compact numeric series (lat/lon + every
 *                          numeric property across the layer's entities).
 *   gev:<entityId>         one BIRTH record per durable event, stored the
 *                          first time an id appears (quakes, fires,
 *                          launches — event-shaped layers), never
 *                          re-stored per poll.
 *   gev:selected           what the operator looked at (upserted on each
 *                          gev:entity-selected, human-paced).
 *
 * THE FLOOD RULE — the ledger's own memory of data ingestion writes this
 * wiring: 19,800 per-entity self-readings once entered the field in
 * eleven hours (chain blocks #331–#354) and taught the instrument
 * nothing. So: aggregates per layer, never per entity; births once,
 * never re-stored; a global governor (MAX_STORES_PER_MIN) that drops
 * work instead of queueing it; and NO client-side field contributions —
 * the server scores every record with the one instrument as it enters
 * (a coherency originates from the Void compressor, nowhere else).
 */

import { getContextStore } from './contextStore.js';
import { remembranceConfigured, storeRecord } from './remembranceBridge.js';

export const FEEDER_INTERVAL_MS = 60_000;
export const MAX_STORES_PER_MIN = 30;
export const SERIES_CAP = 256;              // per numeric field per layer snapshot
export const SELECTION_THROTTLE_MS = 5_000;
/** Layers whose entities are EVENTS (born once, worth remembering each). */
export const EVENT_LAYER_PATTERN = /quake|earthquake|fire|launch|volcano/i;

function nowMinute(now) { return Math.floor(now / 60_000); }

/** Collect the numeric essentials of one context record. */
function numericFields(record) {
  const out = {};
  if (Number.isFinite(record.latitude)) out.latitude = record.latitude;
  if (Number.isFinite(record.longitude)) out.longitude = record.longitude;
  const props = record.properties;
  if (props && typeof props === 'object') {
    for (const [key, value] of Object.entries(props)) {
      const n = typeof value === 'number' ? value : Number(value);
      if (Number.isFinite(n)) out[key] = n;
    }
  }
  return out;
}

/** Fold a layer's records into { count, series: { field: number[] } }. */
export function aggregateLayer(records) {
  const series = {};
  for (const record of records) {
    for (const [field, value] of Object.entries(numericFields(record))) {
      const list = series[field] || (series[field] = []);
      if (list.length < SERIES_CAP) list.push(value);
    }
  }
  return { count: records.length, series };
}

export function createRemembranceFeeder({
  store = null,
  storeRecordImpl = storeRecord,
  configuredImpl = remembranceConfigured,
  intervalMs = FEEDER_INTERVAL_MS,
  now = Date.now,
} = {}) {
  const seenEventIds = new Set();
  let timer = null;
  let selectionListener = null;
  let lastSelectionAt = 0;
  let budgetMinute = -1;
  let budgetUsed = 0;
  const stats = { cycles: 0, stored: 0, dropped: 0, births: 0, selections: 0 };

  function withinBudget() {
    const minute = nowMinute(now());
    if (minute !== budgetMinute) { budgetMinute = minute; budgetUsed = 0; }
    if (budgetUsed >= MAX_STORES_PER_MIN) { stats.dropped += 1; return false; }
    budgetUsed += 1;
    return true;
  }

  async function put(rec) {
    if (!withinBudget()) return null;
    const r = await storeRecordImpl(rec);
    if (r && r.ok) stats.stored += 1;
    return r;
  }

  function contextEntities() {
    const s = store || getContextStore();
    return [...s.entities.values()];
  }

  async function flush() {
    stats.cycles += 1;
    const byLayer = new Map();
    for (const record of contextEntities()) {
      const layerId = record.layerId || 'unknown';
      const list = byLayer.get(layerId) || [];
      list.push(record);
      byLayer.set(layerId, list);
    }
    const at = new Date(now()).toISOString();
    for (const [layerId, records] of byLayer) {
      const snapshot = aggregateLayer(records);
      await put({
        id: `gev:layer:${layerId}`,
        name: `gev:layer:${layerId}`,
        content: JSON.stringify({ layer: layerId, at, ...snapshot }),
        tags: ['gev', 'gev-layer', `layer:${layerId}`],
      });
      if (EVENT_LAYER_PATTERN.test(layerId)) {
        for (const record of records) {
          if (!record.id || seenEventIds.has(record.id)) continue;
          seenEventIds.add(record.id);
          stats.births += 1;
          await put({
            id: `gev:${record.id}`,
            name: record.label || `gev:${record.id}`,
            content: JSON.stringify({
              layer: layerId, at,
              label: record.label ?? null,
              source: record.source ?? null,
              latitude: record.latitude ?? null,
              longitude: record.longitude ?? null,
              properties: record.properties ?? null,
            }),
            tags: ['gev', 'gev-event', `layer:${layerId}`],
          });
        }
      }
    }
    return stats;
  }

  async function onSelection(event) {
    const t = now();
    if (t - lastSelectionAt < SELECTION_THROTTLE_MS) return;
    lastSelectionAt = t;
    const record = event?.detail;
    if (!record?.id) return;
    stats.selections += 1;
    await put({
      id: 'gev:selected',
      name: 'gev:selected',
      content: JSON.stringify({
        at: new Date(t).toISOString(),
        id: record.id,
        layer: record.layerId ?? null,
        label: record.label ?? null,
        latitude: record.latitude ?? null,
        longitude: record.longitude ?? null,
      }),
      tags: ['gev', 'gev-selection'],
    });
  }

  async function start() {
    const configured = await configuredImpl();
    if (!configured) return false;     // no field wired — sleep, zero noise
    if (timer) return true;
    timer = setInterval(() => { flush().catch(() => { /* best-effort */ }); }, intervalMs);
    if (typeof window !== 'undefined') {
      selectionListener = (event) => { onSelection(event).catch(() => { /* best-effort */ }); };
      window.addEventListener('gev:entity-selected', selectionListener);
    }
    return true;
  }

  function stop() {
    if (timer) { clearInterval(timer); timer = null; }
    if (selectionListener && typeof window !== 'undefined') {
      window.removeEventListener('gev:entity-selected', selectionListener);
      selectionListener = null;
    }
  }

  return { start, stop, flush, onSelection, stats };
}

let _feeder = null;

/** Bootstrap entry (main.js): start the feeder; dormant when no field is wired. */
export async function initRemembranceFeeder() {
  if (_feeder) return _feeder;
  _feeder = createRemembranceFeeder();
  try {
    const live = await _feeder.start();
    if (live) console.info('[remembrance] feeder live — the field remembers the globe');
  } catch { /* the globe never depends on the field */ }
  return _feeder;
}
