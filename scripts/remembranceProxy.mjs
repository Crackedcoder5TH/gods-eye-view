/**
 * Remembrance field proxy — the bridge between God's Eye View and the
 * Remembrance information pipeline (the field server's MCP surface).
 *
 * Same posture as every other secret-bearing provider in vite.config.js:
 * the field's bearer token lives server-side only, the destination is
 * fixed from env (never caller-supplied), the body is bounded, errors are
 * sanitized, and the upstream call is time-limited with a capped response.
 *
 * The browser POSTs { name, arguments } and this middleware wraps the one
 * permitted JSON-RPC method (tools/call) around it — the client never
 * speaks raw RPC and only allowlisted tools pass:
 *   legacy      — the durable record store (coherence-scored ON THE SERVER
 *                 by the one instrument as each record enters; this proxy
 *                 never carries a client-invented coherency — the standing
 *                 rule: a coherency originates from the Void compressor)
 *   field_read  — read the field state (dashboards, health)
 *   recall      — resonant retrieval over stored records
 * `field_contribute` is deliberately NOT allowlisted: a browser has no
 * business asserting a coherence number into the field.
 *
 * GET /api/remembrance → { configured } so the client feeder can sleep
 * quietly when no field is wired instead of erroring per poll.
 *
 * Env (.env, lazily read per request — loadEnv applies AFTER import):
 *   REMEMBRANCE_FIELD_URL    e.g. http://127.0.0.1:7787/mcp or the
 *                            Railway deployment's https URL
 *   REMEMBRANCE_FIELD_TOKEN  bearer for privileged actions (legacy store)
 *
 * Dependency-free on purpose: vite.config.js registers it, and the unit
 * tests + the end-to-end proof import it without vite installed.
 */

export const REMEMBRANCE_TOOL_ALLOWLIST = new Set(['legacy', 'field_read', 'recall']);
export const REMEMBRANCE_BODY_MAX_BYTES = 512 * 1024;
export const REMEMBRANCE_RESPONSE_MAX_BYTES = 1024 * 1024;
export const REMEMBRANCE_TIMEOUT_MS = 8000;

function fieldUrl() {
  const raw = (process.env.REMEMBRANCE_FIELD_URL || '').trim();
  if (!raw) return null;
  return raw.endsWith('/mcp') ? raw : raw.replace(/\/$/, '') + '/mcp';
}

function isLoopback(hostname) {
  return hostname === '127.0.0.1' || hostname === 'localhost' || hostname === '::1';
}

/** Read a request body up to a byte cap; null past the cap. */
function readBodyCapped(req, maxBytes) {
  return new Promise((resolve) => {
    const chunks = [];
    let size = 0;
    let overflow = false;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > maxBytes) { overflow = true; chunks.length = 0; return; }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(overflow ? null : Buffer.concat(chunks).toString('utf8')));
    req.on('error', () => resolve(null));
  });
}

export function createRemembranceProxyMiddleware({ fetchImpl = null } = {}) {
  const doFetch = fetchImpl || fetch;
  return async (req, res) => {
    const send = (status, obj) => {
      if (res.headersSent) return;
      res.writeHead(status, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(obj));
    };
    try {
      const url = fieldUrl();
      if (req.method === 'GET') { send(200, { configured: Boolean(url) }); return; }
      if (req.method !== 'POST') { send(405, { error: 'POST only' }); return; }
      if (!url) { send(503, { error: 'REMEMBRANCE_FIELD_URL is not configured' }); return; }
      let dest;
      try { dest = new URL(url); } catch { send(503, { error: 'REMEMBRANCE_FIELD_URL is not a valid URL' }); return; }
      if (dest.protocol !== 'https:' && !isLoopback(dest.hostname)) {
        send(503, { error: 'the field URL must be https or loopback' });
        return;
      }
      const raw = await readBodyCapped(req, REMEMBRANCE_BODY_MAX_BYTES);
      if (raw === null) { send(413, { error: 'body too large' }); return; }
      let body;
      try { body = JSON.parse(raw); } catch { send(400, { error: 'body must be JSON' }); return; }
      const name = typeof body?.name === 'string' ? body.name : '';
      if (!REMEMBRANCE_TOOL_ALLOWLIST.has(name)) {
        send(400, { error: 'tool not permitted' });
        return;
      }
      const args = (body.arguments && typeof body.arguments === 'object') ? body.arguments : {};
      const headers = { 'Content-Type': 'application/json' };
      const token = (process.env.REMEMBRANCE_FIELD_TOKEN || '').trim();
      if (token) headers.Authorization = `Bearer ${token}`;
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), REMEMBRANCE_TIMEOUT_MS);
      try {
        const upstream = await doFetch(dest.href, {
          method: 'POST',
          headers,
          redirect: 'manual',
          signal: controller.signal,
          body: JSON.stringify({
            jsonrpc: '2.0',
            id: 1,
            method: 'tools/call',
            params: { name, arguments: args },
          }),
        });
        if (upstream.status >= 300 && upstream.status < 400) {
          try { await upstream.body?.cancel?.(); } catch { /* no-op */ }
          send(502, { error: 'field redirects are refused' });
          return;
        }
        const text = await upstream.text();
        if (text.length > REMEMBRANCE_RESPONSE_MAX_BYTES) {
          send(502, { error: 'field response too large' });
          return;
        }
        if (res.headersSent) return;
        res.writeHead(upstream.ok ? 200 : 502, { 'Content-Type': 'application/json' });
        res.end(text);
      } finally {
        clearTimeout(timer);
      }
    } catch (err) {
      send(502, { error: `remembrance proxy error: ${err?.name === 'AbortError' ? 'field timeout' : 'upstream unavailable'}` });
    }
  };
}

/** The Vite plugin vite.config.js registers. */
export function remembranceProxy() {
  return {
    name: 'remembrance-proxy',
    configureServer(server) {
      server.middlewares.use('/api/remembrance', createRemembranceProxyMiddleware());
    },
  };
}
