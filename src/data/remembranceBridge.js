/**
 * Remembrance bridge (client side) — God's Eye View's line to the
 * Remembrance field, through the dev server's `/api/remembrance` proxy
 * (scripts/remembranceProxy.mjs). The bearer token never reaches the
 * browser; the proxy holds it and pins the RPC envelope.
 *
 * Best-effort like every layer feed: null on any failure, never throws,
 * short timeout — a downed field never stalls the globe. Mirrors the
 * shape of the Valor Legacies bridge (the pipeline's proven client).
 */

const PROXY_PATH = '/api/remembrance';
const TIMEOUT_MS = 6000;

let _configured = null; // null = not probed yet

/** Is a field wired behind the proxy? Probed once, cached. */
export async function remembranceConfigured() {
  if (_configured !== null) return _configured;
  try {
    const res = await fetch(PROXY_PATH, { method: 'GET' });
    const body = res.ok ? await res.json() : null;
    _configured = Boolean(body?.configured);
  } catch {
    _configured = false;
  }
  return _configured;
}

/** Test seam: reset the probe cache. */
export function resetRemembranceProbe() {
  _configured = null;
}

/**
 * Call one allowlisted field tool through the proxy.
 * @returns {Promise<object|null>} The tool's parsed result, or null.
 */
export async function mcpTool(name, args = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(PROXY_PATH, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, arguments: args }),
      signal: controller.signal,
    });
    if (!res.ok) return null;
    const json = await res.json();
    if (json?.error) return null;
    const content = json?.result?.content?.[0]?.text;
    if (!content) return null;
    try { return JSON.parse(content); } catch { return null; }
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Upsert one durable record in the field's store. A stable `id` makes it
 * an upsert; the SERVER coherence-scores the content with the one
 * instrument as it enters — nothing here computes or asserts a coherency.
 */
export async function storeRecord({ id, name, content, tags, meta }) {
  return mcpTool('legacy', { action: 'store', id, name, content, tags, meta });
}

/** Read the live field state (coherence, integral, cascade). */
export async function readField() {
  return mcpTool('field_read', {});
}

/** Resonant retrieval over the stored records. */
export async function recall(query, k = 6) {
  const r = await mcpTool('recall', { query, k });
  return r && r.ok && Array.isArray(r.slices) ? r.slices : [];
}
