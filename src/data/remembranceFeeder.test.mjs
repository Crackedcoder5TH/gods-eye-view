import test from 'node:test';
import assert from 'node:assert/strict';

import {
  createRemembranceFeeder,
  aggregateLayer,
  MAX_STORES_PER_MIN,
  SELECTION_THROTTLE_MS,
} from './remembranceFeeder.js';

/** A fixture context store shaped exactly like contextStore.js's. */
function fixtureStore() {
  return {
    entities: new Map([
      ['quake:q1', { id: 'quake:q1', layerId: 'earthquakes', label: 'M 5.1', source: 'USGS', latitude: 35.2, longitude: -117.5, properties: { magnitude: 5.1, depthKm: 9.4 } }],
      ['quake:q2', { id: 'quake:q2', layerId: 'earthquakes', label: 'M 4.2', source: 'USGS', latitude: 36.0, longitude: -118.0, properties: { magnitude: 4.2, depthKm: 4.1 } }],
      ['ac:a1',    { id: 'ac:a1',    layerId: 'flights',     label: 'UAL12', latitude: 40.1, longitude: -100.2, properties: { altitude: 11000, speed: 250.5, heading: 92 } }],
      ['ac:a2',    { id: 'ac:a2',    layerId: 'flights',     label: 'DAL7',  latitude: 41.0, longitude: -99.8,  properties: { altitude: 10400, speed: 244.1, heading: 88 } }],
    ]),
    selectedEntityId: null,
    selectedAt: null,
  };
}

function collectingImpl(calls) {
  return async (rec) => { calls.push(rec); return { ok: true, id: rec.id }; };
}

test('aggregateLayer folds numeric fields into per-field series', () => {
  const records = [...fixtureStore().entities.values()].filter((r) => r.layerId === 'flights');
  const agg = aggregateLayer(records);
  assert.equal(agg.count, 2);
  assert.deepEqual(agg.series.altitude, [11000, 10400]);
  assert.deepEqual(agg.series.latitude, [40.1, 41.0]);
  assert.equal(agg.series.heading.length, 2);
});

test('flush stores one aggregate per layer and one birth per durable event', async () => {
  const calls = [];
  const feeder = createRemembranceFeeder({ store: fixtureStore(), storeRecordImpl: collectingImpl(calls) });
  await feeder.flush();
  const ids = calls.map((c) => c.id).sort();
  // 2 layer aggregates + 2 quake births; flights are NOT event-shaped, so
  // no per-aircraft records — the flood rule.
  assert.deepEqual(ids, ['gev:layer:earthquakes', 'gev:layer:flights', 'gev:quake:q1', 'gev:quake:q2']);
  const quakeLayer = calls.find((c) => c.id === 'gev:layer:earthquakes');
  const body = JSON.parse(quakeLayer.content);
  assert.equal(body.count, 2);
  assert.deepEqual(body.series.magnitude, [5.1, 4.2]);
});

test('a second flush re-stores aggregates but never re-stores a birth', async () => {
  const calls = [];
  const feeder = createRemembranceFeeder({ store: fixtureStore(), storeRecordImpl: collectingImpl(calls) });
  await feeder.flush();
  await feeder.flush();
  const births = calls.filter((c) => c.tags.includes('gev-event'));
  assert.equal(births.length, 2);          // once each, ever
  const aggregates = calls.filter((c) => c.tags.includes('gev-layer'));
  assert.equal(aggregates.length, 4);      // 2 layers × 2 cycles
});

test('the governor drops stores past MAX_STORES_PER_MIN instead of queueing', async () => {
  const calls = [];
  let clock = 1_000_000;
  const store = { entities: new Map(), selectedEntityId: null, selectedAt: null };
  for (let i = 0; i < MAX_STORES_PER_MIN + 20; i++) {
    store.entities.set(`quake:${i}`, { id: `quake:${i}`, layerId: 'earthquakes', latitude: i, longitude: i, properties: { magnitude: 3 } });
  }
  const feeder = createRemembranceFeeder({ store, storeRecordImpl: collectingImpl(calls), now: () => clock });
  await feeder.flush();
  assert.equal(calls.length, MAX_STORES_PER_MIN);
  assert.ok(feeder.stats.dropped >= 20);
  // the budget refills on the next minute
  clock += 61_000;
  await feeder.flush();
  assert.ok(calls.length > MAX_STORES_PER_MIN);
});

test('selections are recorded and throttled', async () => {
  const calls = [];
  let clock = 5_000_000;
  const feeder = createRemembranceFeeder({ store: fixtureStore(), storeRecordImpl: collectingImpl(calls), now: () => clock });
  const detail = { id: 'quake:q1', layerId: 'earthquakes', label: 'M 5.1', latitude: 35.2, longitude: -117.5 };
  await feeder.onSelection({ detail });
  clock += 1_000;                            // inside the throttle window
  await feeder.onSelection({ detail });
  clock += SELECTION_THROTTLE_MS;
  await feeder.onSelection({ detail });
  const selections = calls.filter((c) => c.id === 'gev:selected');
  assert.equal(selections.length, 2);
  assert.equal(feeder.stats.selections, 2);
});
