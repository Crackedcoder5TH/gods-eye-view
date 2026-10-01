#!/usr/bin/env node
/**
 * End-to-end proof of the GEV → Remembrance wire, against a LIVE field
 * server: starts the real proxy middleware on a local HTTP server, drives
 * the real feeder with fixture entities through it, verifies every record
 * lands in the field's store byte-identically, then DELETES the fixture
 * records — the store is left clean, fixture data never remains and is
 * never presented as a measurement.
 *
 * Needs REMEMBRANCE_FIELD_URL (+ token if the field requires one).
 * Exits nonzero on any failure. Run: node scripts/remembrance-e2e.mjs
 */
import http from 'node:http';
import { createRemembranceProxyMiddleware } from './remembranceProxy.mjs';
import { createRemembranceFeeder } from '../src/data/remembranceFeeder.js';

const FIXTURES = new Map([
  ['gevfix:quake:e2e1', { id: 'gevfix:quake:e2e1', layerId: 'earthquakes-e2e-fixture', label: 'FIXTURE M 5.0', source: 'fixture', latitude: 10.5, longitude: 20.25, properties: { magnitude: 5.0, depthKm: 12.5 } }],
  ['gevfix:quake:e2e2', { id: 'gevfix:quake:e2e2', layerId: 'earthquakes-e2e-fixture', label: 'FIXTURE M 4.4', source: 'fixture', latitude: 11.0, longitude: 21.0, properties: { magnitude: 4.4, depthKm: 3.75 } }],
  ['gevfix:ac:e2e1', { id: 'gevfix:ac:e2e1', layerId: 'flights-e2e-fixture', label: 'FIXTURE AC', latitude: 40.0, longitude: -100.0, properties: { altitude: 9000, speed: 210.5 } }],
  // A layer above the instrument's 8-point floor, so its snapshot carries
  // series the field reads AS DATA (a non-event layer: one aggregate, no births).
  ...Array.from({ length: 12 }, (_, i) => [`gevfix:buoy:e2e${i}`, {
    id: `gevfix:buoy:e2e${i}`, layerId: 'buoys-e2e-fixture', label: `FIXTURE BUOY ${i}`,
    latitude: 30 + i * 0.75, longitude: -80 - i * 0.5,
    properties: { waveHeightM: 1.2 + Math.sin(i / 2), waterTempC: 24 + i * 0.3 },
  }]),
]);

async function main() {
  const middleware = createRemembranceProxyMiddleware();
  const server = http.createServer((req, res) => middleware(req, res));
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;

  const post = async (name, args) => {
    const res = await fetch(base, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name, arguments: args }) });
    const json = await res.json();
    const text = json?.result?.content?.[0]?.text;
    return text ? JSON.parse(text) : json;
  };

  const probe = await (await fetch(base)).json();
  if (!probe.configured) {
    console.error('REMEMBRANCE_FIELD_URL is not configured — nothing to prove against');
    return 1;
  }

  const expected = [];
  // The field's own answer to each store — when every record reads back
  // absent, these say why (a flat counter needs the counter that explains it).
  const storeReplies = [];
  const scored = [];   // the coherency the field's instrument assigned on entry
  const feeder = createRemembranceFeeder({
    store: { entities: FIXTURES, selectedEntityId: null, selectedAt: null },
    storeRecordImpl: async (rec) => {
      expected.push(rec);
      const reply = await post('legacy', { action: 'store', ...rec });
      if (!(reply && reply.ok)) storeReplies.push({ id: rec.id, reply });
      else scored.push({
        id: rec.id,
        coherence: reply.coherence ?? null,
        data: (reply.dataReadings || []).map((d) => `${d.name} ${d.points}pts → ${d.coherency}`),
      });
      return reply;
    },
  });
  await feeder.flush();
  await feeder.onSelection({ detail: FIXTURES.get('gevfix:quake:e2e1') });

  let identical = 0;
  const mismatches = [];
  for (const rec of expected) {
    const got = await post('legacy', { action: 'get', id: rec.id });
    const back = got && got.ok ? got.legacy : null;
    if (back && back.content === rec.content) identical += 1;
    else mismatches.push({ id: rec.id, why: back ? 'content differs' : 'absent' });
  }

  let deleted = 0;
  for (const rec of expected) {
    const d = await post('legacy', { action: 'delete', id: rec.id });
    if (d && d.ok) deleted += d.deleted || 0;
  }
  const gone = [];
  for (const rec of expected) {
    const got = await post('legacy', { action: 'get', id: rec.id });
    if (!(got && got.ok && got.legacy)) gone.push(rec.id);
  }

  server.close();
  const summary = {
    stored: expected.length,
    identical,
    mismatches,
    deleted,
    verified_gone: gone.length,
    stats: feeder.stats,
    store_refusals: storeReplies.slice(0, 3),
    scored_on_entry: scored,
  };
  console.log(JSON.stringify(summary, null, 2));
  return (mismatches.length === 0 && gone.length === expected.length) ? 0 : 1;
}

main().then((code) => process.exit(code)).catch((e) => { console.error(String(e?.stack || e)); process.exit(1); });
