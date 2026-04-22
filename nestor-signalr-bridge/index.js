'use strict';

// ─────────────────────────────────────────────────────────────
//  Nestor SignalR Bridge
//  Pont bidirectionnel : Home Assistant WS  <->  Azure SignalR
// ─────────────────────────────────────────────────────────────

const http = require('http');
const signalR = require('@microsoft/signalr');
const WebSocket = require('ws');
const fs = require('fs');

// ─── Configuration ──────────────────────────────────────────

const OPTIONS_PATH = '/data/options.json';
const HA_WS_URL = 'ws://supervisor/core/websocket';
const DIAGNOSTICS_PORT = 8099;
const DIAGNOSTICS_HISTORY_LIMIT = 200;

let options = {};
try {
  options = JSON.parse(fs.readFileSync(OPTIONS_PATH, 'utf8'));
} catch {
  // Running outside HA (dev mode) — fall back to env vars
}

const NEGOTIATE_URL =
  options.negotiate_url ||
  process.env.NEGOTIATE_URL ||
  'https://nestorsmarthome-poc-ekdmcxekdwegd9ex.a01.azurefd.net/api/Negotiate';

const LOG_LEVEL = options.log_level || process.env.LOG_LEVEL || 'info';
const SUBSCRIBE_EVENTS = options.subscribe_events || ['state_changed'];
const RECONNECT_INTERVAL = options.reconnect_interval_ms || 5000;
const SIGNALR_KEEPALIVE = options.signalr_keep_alive_interval_ms || 15000;
const SUPERVISOR_TOKEN = process.env.SUPERVISOR_TOKEN;

// ─── Logger ─────────────────────────────────────────────────

const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
const currentLevel = LEVELS[LOG_LEVEL] ?? 1;
const SENSITIVE_KEYS = ['access_token', 'accesstoken', 'authorization', 'token'];

function log(level, tag, msg, data) {
  if ((LEVELS[level] ?? 1) < currentLevel) return;
  const ts = new Date().toISOString();
  const prefix = `[${ts}] [${level.toUpperCase().padEnd(5)}] [${tag}]`;
  if (data !== undefined) {
    console.log(`${prefix} ${msg}`, typeof data === 'string' ? data : JSON.stringify(data));
  } else {
    console.log(`${prefix} ${msg}`);
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function nowIso() {
  return new Date().toISOString();
}

function isSensitiveKey(key) {
  const normalized = String(key || '').toLowerCase();
  return SENSITIVE_KEYS.some((entry) => normalized.includes(entry));
}

function truncateString(value, maxLength) {
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength)}… (${value.length} chars)`;
}

function sanitizeValue(value, depth = 0, key = '') {
  if (isSensitiveKey(key)) return '<redacted>';

  if (value === null || value === undefined) return value;
  if (typeof value === 'string') return truncateString(value, 1200);
  if (typeof value === 'number' || typeof value === 'boolean') return value;

  if (value instanceof Error) {
    return {
      message: value.message,
      stack: value.stack ? truncateString(value.stack, 2000) : undefined,
    };
  }

  if (depth >= 4) return '[max-depth]';

  if (Array.isArray(value)) {
    const sanitized = value.slice(0, 20).map((entry) => sanitizeValue(entry, depth + 1));
    if (value.length > 20) sanitized.push(`... ${value.length - 20} more items`);
    return sanitized;
  }

  if (typeof value === 'object') {
    const entries = Object.entries(value);
    const output = {};
    for (const [childKey, childValue] of entries.slice(0, 25)) {
      output[childKey] = sanitizeValue(childValue, depth + 1, childKey);
    }
    if (entries.length > 25) output.__truncated__ = `${entries.length - 25} more keys`;
    return output;
  }

  return String(value);
}

// ─── Diagnostics Store ──────────────────────────────────────

class DiagnosticsStore {
  constructor(historyLimit) {
    this.historyLimit = historyLimit;
    this.entries = [];
    this.sequence = 0;
    this.sseClients = new Set();
    this.startedAt = nowIso();
    this.lastUpdatedAt = this.startedAt;

    this.connections = {
      ha: {
        status: 'idle',
        authenticated: false,
        lastConnectedAt: null,
        lastDisconnectedAt: null,
        lastError: null,
        lastErrorAt: null,
      },
      signalr: {
        status: 'idle',
        hubUrl: null,
        lastNegotiatedAt: null,
        lastConnectedAt: null,
        lastDisconnectedAt: null,
        lastError: null,
        lastErrorAt: null,
      },
    };

    this.counters = {
      toAzure: 0,
      fromAzure: 0,
      errors: 0,
      uiClients: 0,
    };

    this.config = {
      negotiateUrl: NEGOTIATE_URL,
      haWsUrl: HA_WS_URL,
      subscribeEvents: SUBSCRIBE_EVENTS,
      diagnosticsPort: DIAGNOSTICS_PORT,
    };
  }

  snapshot() {
    return {
      startedAt: this.startedAt,
      lastUpdatedAt: this.lastUpdatedAt,
      config: {
        ...this.config,
      },
      counters: {
        ...this.counters,
      },
      connections: {
        ha: { ...this.connections.ha },
        signalr: { ...this.connections.signalr },
      },
      recentEntries: [...this.entries].reverse(),
    };
  }

  updateConnection(name, patch, broadcast = true) {
    this.connections[name] = {
      ...this.connections[name],
      ...sanitizeValue(patch),
    };
    this.lastUpdatedAt = nowIso();
    if (broadcast) this.broadcast();
  }

  recordFlow(direction, label, payload, meta) {
    const entry = {
      id: ++this.sequence,
      timestamp: nowIso(),
      direction,
      label,
      payload: sanitizeValue(payload),
      meta: sanitizeValue(meta),
    };

    this.entries.push(entry);
    if (this.entries.length > this.historyLimit) this.entries.shift();

    if (direction === 'to-azure') this.counters.toAzure += 1;
    if (direction === 'from-azure') this.counters.fromAzure += 1;
    if (direction === 'error') this.counters.errors += 1;

    this.lastUpdatedAt = entry.timestamp;
    this.broadcast();
  }

  recordSystem(label, payload) {
    this.recordFlow('system', label, payload);
  }

  noteError(channel, error, meta) {
    this.updateConnection(channel, {
      lastError: sanitizeValue(error),
      lastErrorAt: nowIso(),
    }, false);
    this.recordFlow('error', `${channel}.error`, error, meta);
  }

  clearEntries() {
    this.entries = [];
    this.lastUpdatedAt = nowIso();
    this.broadcast();
  }

  attachSseClient(res) {
    this.sseClients.add(res);
    this.counters.uiClients = this.sseClients.size;
    this.lastUpdatedAt = nowIso();
    this._writeSseEvent(res, 'snapshot', this.snapshot());
    this.broadcast();
  }

  detachSseClient(res) {
    this.sseClients.delete(res);
    this.counters.uiClients = this.sseClients.size;
    this.lastUpdatedAt = nowIso();
    this.broadcast();
  }

  broadcast() {
    if (this.sseClients.size === 0) return;
    const snapshot = this.snapshot();
    for (const client of this.sseClients) {
      this._writeSseEvent(client, 'snapshot', snapshot);
    }
  }

  _writeSseEvent(res, event, data) {
    try {
      res.write(`event: ${event}\n`);
      res.write(`data: ${JSON.stringify(data)}\n\n`);
    } catch {
      this.sseClients.delete(res);
      this.counters.uiClients = this.sseClients.size;
    }
  }
}

function renderDashboardHtml() {
  return `<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Nestor SignalR Bridge</title>
  <style>
    :root {
      --bg: #07141a;
      --panel: rgba(13, 31, 40, 0.92);
      --panel-strong: rgba(18, 41, 53, 0.98);
      --text: #f3f7f9;
      --muted: #91aab7;
      --accent: #ffbf47;
      --good: #2fd3a7;
      --warn: #ff9f43;
      --bad: #ff6b6b;
      --line: rgba(255, 255, 255, 0.08);
      --shadow: 0 24px 60px rgba(0, 0, 0, 0.38);
      --radius: 18px;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: "Trebuchet MS", "Segoe UI Variable", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(255, 191, 71, 0.18), transparent 34%),
        radial-gradient(circle at top right, rgba(47, 211, 167, 0.16), transparent 28%),
        linear-gradient(160deg, #041016 0%, #081b24 48%, #051017 100%);
    }

    .shell {
      width: min(1200px, calc(100vw - 24px));
      margin: 18px auto;
      display: grid;
      gap: 18px;
    }

    .hero,
    .card,
    .event {
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
    }

    .hero {
      padding: 24px;
      overflow: hidden;
      position: relative;
    }

    .hero::after {
      content: "";
      position: absolute;
      inset: auto -60px -80px auto;
      width: 220px;
      height: 220px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(255, 191, 71, 0.18), transparent 70%);
      pointer-events: none;
    }

    .hero-top {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: flex-start;
      justify-content: space-between;
    }

    h1 {
      margin: 0;
      font-size: clamp(28px, 4vw, 42px);
      line-height: 1;
      letter-spacing: 0.02em;
    }

    .subtitle {
      margin: 10px 0 0;
      color: var(--muted);
      max-width: 720px;
      line-height: 1.5;
    }

    .status-strip {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 18px;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      font-size: 13px;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      background: rgba(255, 255, 255, 0.06);
      border: 1px solid rgba(255, 255, 255, 0.08);
    }

    .badge::before {
      content: "";
      width: 9px;
      height: 9px;
      border-radius: 50%;
      background: #64748b;
      box-shadow: 0 0 14px currentColor;
    }

    .badge.connected::before { color: var(--good); background: var(--good); }
    .badge.negotiated::before,
    .badge.authorizing::before,
    .badge.connecting::before,
    .badge.reconnecting::before { color: var(--warn); background: var(--warn); }
    .badge.disconnected::before,
    .badge.auth_invalid::before,
    .badge.error::before,
    .badge.stopped::before { color: var(--bad); background: var(--bad); }

    .grid {
      display: grid;
      gap: 18px;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    }

    .card {
      padding: 18px;
    }

    .label {
      font-size: 12px;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 8px;
    }

    .value {
      font-size: 28px;
      font-weight: 700;
    }

    .meta {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
      margin-top: 10px;
      word-break: break-word;
    }

    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
    }

    .toolbar h2,
    .card h2 {
      margin: 0;
      font-size: 18px;
    }

    button {
      border: 0;
      border-radius: 999px;
      padding: 10px 16px;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      background: linear-gradient(135deg, #ffbf47, #ff8b42);
      color: #10212a;
    }

    .feed {
      display: grid;
      gap: 12px;
    }

    .event {
      padding: 16px;
      background: var(--panel-strong);
    }

    .event-head {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
    }

    .pill {
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .pill.to-azure { background: rgba(255, 191, 71, 0.18); color: #ffd470; }
    .pill.from-azure { background: rgba(47, 211, 167, 0.18); color: #74f5cf; }
    .pill.system { background: rgba(122, 160, 188, 0.18); color: #c0d2de; }
    .pill.error { background: rgba(255, 107, 107, 0.18); color: #ff9f9f; }

    .event-title {
      font-weight: 700;
      font-size: 16px;
    }

    .timestamp {
      color: var(--muted);
      font-size: 13px;
    }

    pre {
      margin: 0;
      padding: 14px;
      border-radius: 14px;
      overflow: auto;
      font-size: 12px;
      line-height: 1.45;
      border: 1px solid rgba(255, 255, 255, 0.05);
      background: rgba(3, 11, 16, 0.72);
      color: #dbe7ee;
    }

    .empty {
      border: 1px dashed rgba(255, 255, 255, 0.12);
      border-radius: var(--radius);
      padding: 24px;
      text-align: center;
      color: var(--muted);
      background: rgba(255, 255, 255, 0.03);
    }

    .hint {
      font-size: 13px;
      color: var(--muted);
    }

    @media (max-width: 720px) {
      .shell {
        width: min(100vw - 14px, 100%);
        margin: 8px auto 18px;
      }

      .hero,
      .card,
      .event {
        border-radius: 16px;
      }

      .value {
        font-size: 24px;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <div class="hero-top">
        <div>
          <h1>Nestor SignalR Bridge</h1>
          <p class="subtitle">Tableau de bord de diagnostic en temps réel pour vérifier l’état des connexions Home Assistant et Azure SignalR, et visualiser les messages qui sortent vers Azure comme ceux qui arrivent depuis Azure.</p>
        </div>
        <div class="badge connected" id="ui-stream-state">Flux UI en ligne</div>
      </div>
      <div class="status-strip">
        <div class="badge idle" id="ha-state-badge">HA: idle</div>
        <div class="badge idle" id="signalr-state-badge">SignalR: idle</div>
        <div class="badge" id="updated-badge">Dernière mise à jour: --</div>
      </div>
    </section>

    <section class="grid">
      <article class="card">
        <div class="label">Home Assistant</div>
        <div class="value" id="ha-status">idle</div>
        <div class="meta" id="ha-meta">En attente.</div>
      </article>
      <article class="card">
        <div class="label">Azure SignalR</div>
        <div class="value" id="signalr-status">idle</div>
        <div class="meta" id="signalr-meta">En attente.</div>
      </article>
      <article class="card">
        <div class="label">Vers Azure</div>
        <div class="value" id="to-azure-count">0</div>
        <div class="meta">Messages effectivement envoyés au hub SignalR.</div>
      </article>
      <article class="card">
        <div class="label">Depuis Azure</div>
        <div class="value" id="from-azure-count">0</div>
        <div class="meta">Commandes reçues depuis Azure SignalR.</div>
      </article>
      <article class="card">
        <div class="label">Erreurs</div>
        <div class="value" id="error-count">0</div>
        <div class="meta">Erreurs de socket, négociation ou envoi détectées.</div>
      </article>
      <article class="card">
        <div class="label">Clients UI</div>
        <div class="value" id="ui-clients-count">0</div>
        <div class="meta">Pages ouvertes sur ce tableau de bord.</div>
      </article>
    </section>

    <section class="card">
      <div class="toolbar">
        <div>
          <h2>Flux temps réel</h2>
          <div class="hint">Historique en mémoire des derniers échanges. Les secrets et tokens sont masqués.</div>
        </div>
        <button id="clear-feed" type="button">Effacer l’historique</button>
      </div>
    </section>

    <section class="feed" id="feed"></section>
  </main>

  <script>
    const feedEl = document.getElementById('feed');
    const uiStreamStateEl = document.getElementById('ui-stream-state');
    const haStateBadgeEl = document.getElementById('ha-state-badge');
    const signalrStateBadgeEl = document.getElementById('signalr-state-badge');
    const updatedBadgeEl = document.getElementById('updated-badge');

    function escapeHtml(value) {
      return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    }

    function formatDate(value) {
      if (!value) return '—';
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString();
    }

    function badgeClass(status) {
      return String(status || 'idle').replace(/[^a-z0-9_-]/gi, '_');
    }

    function pretty(value) {
      if (value === undefined || value === null) return '—';
      return escapeHtml(JSON.stringify(value, null, 2));
    }

    function renderConnectionSummary(snapshot) {
      const ha = snapshot.connections.ha;
      const signalr = snapshot.connections.signalr;

      document.getElementById('ha-status').textContent = ha.status || 'idle';
      document.getElementById('signalr-status').textContent = signalr.status || 'idle';
      document.getElementById('to-azure-count').textContent = snapshot.counters.toAzure;
      document.getElementById('from-azure-count').textContent = snapshot.counters.fromAzure;
      document.getElementById('error-count').textContent = snapshot.counters.errors;
      document.getElementById('ui-clients-count').textContent = snapshot.counters.uiClients;

      haStateBadgeEl.className = 'badge ' + badgeClass(ha.status);
      signalrStateBadgeEl.className = 'badge ' + badgeClass(signalr.status);
      haStateBadgeEl.textContent = 'HA: ' + (ha.status || 'idle');
      signalrStateBadgeEl.textContent = 'SignalR: ' + (signalr.status || 'idle');
      updatedBadgeEl.textContent = 'Dernière mise à jour: ' + formatDate(snapshot.lastUpdatedAt);

      document.getElementById('ha-meta').innerHTML = [
        'Authentifié: ' + (ha.authenticated ? 'oui' : 'non'),
        'Dernière connexion: ' + formatDate(ha.lastConnectedAt),
        'Dernière déconnexion: ' + formatDate(ha.lastDisconnectedAt),
        'Dernière erreur: ' + escapeHtml(ha.lastError ? JSON.stringify(ha.lastError) : '—')
      ].join('<br>');

      document.getElementById('signalr-meta').innerHTML = [
        'Hub: ' + escapeHtml(signalr.hubUrl || '—'),
        'Dernière négociation: ' + formatDate(signalr.lastNegotiatedAt),
        'Dernière connexion: ' + formatDate(signalr.lastConnectedAt),
        'Dernière déconnexion: ' + formatDate(signalr.lastDisconnectedAt),
        'Dernière erreur: ' + escapeHtml(signalr.lastError ? JSON.stringify(signalr.lastError) : '—')
      ].join('<br>');
    }

    function renderFeed(entries) {
      if (!entries || entries.length === 0) {
        feedEl.innerHTML = '<div class="empty">Aucun échange enregistré pour le moment. Lance l\'add-on et les événements s\'afficheront ici en direct.</div>';
        return;
      }

      feedEl.innerHTML = entries.map((entry) => {
        const meta = entry.meta && Object.keys(entry.meta).length > 0
          ? '<pre>' + pretty(entry.meta) + '</pre>'
          : '';
        return [
          '<article class="event">',
          '  <div class="event-head">',
          '    <div style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">',
          '      <span class="pill ' + escapeHtml(entry.direction) + '">' + escapeHtml(entry.direction) + '</span>',
          '      <div class="event-title">' + escapeHtml(entry.label) + '</div>',
          '    </div>',
          '    <div class="timestamp">' + escapeHtml(formatDate(entry.timestamp)) + '</div>',
          '  </div>',
          '  <pre>' + pretty(entry.payload) + '</pre>',
          meta,
          '</article>'
        ].join('');
      }).join('');
    }

    function render(snapshot) {
      renderConnectionSummary(snapshot);
      renderFeed(snapshot.recentEntries || []);
    }

    async function loadInitialSnapshot() {
      const response = await fetch('./api/status', { cache: 'no-store' });
      const snapshot = await response.json();
      render(snapshot);
    }

    function connectStream() {
      const stream = new EventSource('./api/stream');

      stream.addEventListener('snapshot', (event) => {
        uiStreamStateEl.className = 'badge connected';
        uiStreamStateEl.textContent = 'Flux UI en ligne';
        render(JSON.parse(event.data));
      });

      stream.onerror = () => {
        uiStreamStateEl.className = 'badge disconnected';
        uiStreamStateEl.textContent = 'Flux UI interrompu';
      };
    }

    document.getElementById('clear-feed').addEventListener('click', async () => {
      await fetch('./api/clear', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        }
      });
    });

    loadInitialSnapshot().catch(() => {
      uiStreamStateEl.className = 'badge disconnected';
      uiStreamStateEl.textContent = 'Lecture initiale impossible';
    });
    connectStream();
  </script>
</body>
</html>`;
}

// ─── Diagnostics Server ─────────────────────────────────────

class DiagnosticsServer {
  constructor(diagnostics, port) {
    this.diagnostics = diagnostics;
    this.port = port;
    this.server = null;
  }

  start() {
    this.server = http.createServer((req, res) => this._handleRequest(req, res));
    this.server.listen(this.port, '0.0.0.0', () => {
      log('info', 'UI', `Diagnostics UI listening on port ${this.port}`);
      this.diagnostics.recordSystem('ui.server.started', { port: this.port });
    });
  }

  async stop() {
    if (!this.server) return;

    for (const client of this.diagnostics.sseClients) {
      try {
        client.end();
      } catch { /* ignore */ }
    }

    await new Promise((resolve) => this.server.close(resolve));
  }

  _handleRequest(req, res) {
    const requestUrl = new URL(req.url || '/', 'http://127.0.0.1');

    if (req.method === 'GET' && (requestUrl.pathname === '/' || requestUrl.pathname === '/index.html')) {
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store',
      });
      res.end(renderDashboardHtml());
      return;
    }

    if (req.method === 'GET' && requestUrl.pathname === '/api/status') {
      res.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8',
        'Cache-Control': 'no-store',
      });
      res.end(JSON.stringify(this.diagnostics.snapshot()));
      return;
    }

    if (req.method === 'GET' && requestUrl.pathname === '/api/stream') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no',
      });
      res.write('retry: 2000\n\n');

      const heartbeat = setInterval(() => {
        try {
          res.write(': ping\n\n');
        } catch {
          clearInterval(heartbeat);
        }
      }, 15000);

      this.diagnostics.attachSseClient(res);

      req.on('close', () => {
        clearInterval(heartbeat);
        this.diagnostics.detachSseClient(res);
      });
      return;
    }

    if (req.method === 'POST' && requestUrl.pathname === '/api/clear') {
      this.diagnostics.clearEntries();
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.method === 'GET' && requestUrl.pathname === '/health') {
      res.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8',
      });
      res.end(JSON.stringify({ ok: true }));
      return;
    }

    res.writeHead(404, {
      'Content-Type': 'application/json; charset=utf-8',
    });
    res.end(JSON.stringify({ error: 'Not found' }));
  }
}

// ─── Home Assistant WebSocket Client ────────────────────────

class HAClient {
  constructor(diagnostics) {
    this.diagnostics = diagnostics;
    this.ws = null;
    this.msgId = 0;
    this.authenticated = false;
    this.pendingRequests = new Map();
    this.shouldRun = true;

    // Callbacks set by Bridge
    this.onEvent = null;
    this.onConnected = null;
  }

  // --- Public API ---

  async connect() {
    return new Promise((resolve, reject) => {
      log('info', 'HA-WS', `Connecting to ${HA_WS_URL} …`);
      this.diagnostics.updateConnection('ha', {
        status: 'connecting',
        authenticated: false,
        lastError: null,
      });

      this.ws = new WebSocket(HA_WS_URL);
      this.authenticated = false;
      this.msgId = 0;
      this.pendingRequests.clear();

      let settled = false;

      this.ws.on('open', () => {
        log('info', 'HA-WS', 'TCP connection established');
        this.diagnostics.updateConnection('ha', {
          status: 'authorizing',
        });
      });

      this.ws.on('message', (raw) => {
        let msg;
        try {
          msg = JSON.parse(raw.toString());
        } catch (err) {
          log('error', 'HA-WS', 'Failed to parse message', raw.toString().slice(0, 200));
          return;
        }
        this._onMessage(msg, (err) => {
          if (settled) return;
          settled = true;
          if (err) reject(err);
          else resolve();
        });
      });

      this.ws.on('close', (code, reason) => {
        const r = reason?.toString() || '';
        log('warn', 'HA-WS', `Connection closed (code=${code} reason=${r})`);
        this.authenticated = false;
        this.diagnostics.updateConnection('ha', {
          status: this.shouldRun ? 'disconnected' : 'stopped',
          authenticated: false,
          lastDisconnectedAt: nowIso(),
        });
        this._rejectAllPending('Connection closed');
        if (!settled) {
          settled = true;
          reject(new Error(`WS closed before auth (code=${code})`));
        }
        if (this.shouldRun) this._scheduleReconnect();
      });

      this.ws.on('error', (err) => {
        log('error', 'HA-WS', `Socket error: ${err.message}`);
        this.diagnostics.noteError('ha', err, { phase: 'socket' });
      });
    });
  }

  async subscribeEvents(eventType) {
    const payload = { type: 'subscribe_events' };
    if (eventType) payload.event_type = eventType;
    log('info', 'HA-WS', `Subscribing to "${eventType || '*'}" events`);
    return this._sendCommand(payload);
  }

  async callService(domain, service, serviceData, target) {
    log('info', 'HA-WS', `Calling service ${domain}.${service}`);
    const payload = {
      type: 'call_service',
      domain,
      service,
      service_data: serviceData || {},
    };
    if (target) payload.target = target;
    return this._sendCommand(payload);
  }

  async getStates() {
    log('info', 'HA-WS', 'Requesting all states');
    return this._sendCommand({ type: 'get_states' });
  }

  close() {
    this.shouldRun = false;
    this.diagnostics.updateConnection('ha', {
      status: 'stopped',
      authenticated: false,
    });
    this.ws?.close();
  }

  // --- Internal ---

  _onMessage(msg, authCb) {
    switch (msg.type) {
      case 'auth_required':
        log('info', 'HA-WS', `Auth required (HA v${msg.ha_version})`);
        this._sendRaw({ type: 'auth', access_token: SUPERVISOR_TOKEN });
        break;

      case 'auth_ok':
        log('info', 'HA-WS', `Authenticated (HA v${msg.ha_version})`);
        this.authenticated = true;
        this.diagnostics.updateConnection('ha', {
          status: 'connected',
          authenticated: true,
          lastConnectedAt: nowIso(),
          lastError: null,
        });
        this.diagnostics.recordSystem('ha.authenticated', { haVersion: msg.ha_version });
        authCb(null);
        break;

      case 'auth_invalid':
        log('error', 'HA-WS', `Authentication failed: ${msg.message}`);
        this.diagnostics.updateConnection('ha', {
          status: 'auth_invalid',
          authenticated: false,
        }, false);
        this.diagnostics.noteError('ha', new Error(msg.message), { phase: 'auth' });
        authCb(new Error(msg.message));
        break;

      case 'event':
        this._dispatchEvent(msg);
        break;

      case 'result':
        this._resolveCommand(msg);
        break;

      case 'pong':
        // heartbeat response — ignore
        break;

      default:
        log('debug', 'HA-WS', `Unhandled message type: ${msg.type}`);
    }
  }

  _dispatchEvent(msg) {
    log('debug', 'HA-WS', `Event received: ${msg.event?.event_type || 'unknown'}`);
    if (this.onEvent) {
      try {
        this.onEvent(msg);
      } catch (err) {
        log('error', 'HA-WS', `Event handler error: ${err.message}`);
      }
    }
  }

  _resolveCommand(msg) {
    const pending = this.pendingRequests.get(msg.id);
    if (!pending) return;
    this.pendingRequests.delete(msg.id);
    if (msg.success) {
      pending.resolve(msg.result);
    } else {
      log('warn', 'HA-WS', `Command ${msg.id} failed`, msg.error);
      pending.reject(new Error(msg.error?.message || 'Unknown error'));
    }
  }

  _sendCommand(payload) {
    const id = ++this.msgId;
    payload.id = id;
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        if (this.pendingRequests.has(id)) {
          this.pendingRequests.delete(id);
          reject(new Error(`Command ${id} timed out (${payload.type})`));
        }
      }, 30_000);

      this.pendingRequests.set(id, {
        resolve: (val) => {
          clearTimeout(timeout);
          resolve(val);
        },
        reject: (err) => {
          clearTimeout(timeout);
          reject(err);
        },
      });
      this._sendRaw(payload);
    });
  }

  _sendRaw(data) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
      log('debug', 'HA-WS', 'Sent', data.type || '');
    } else {
      log('warn', 'HA-WS', 'Cannot send — socket not open');
    }
  }

  _rejectAllPending(reason) {
    for (const [id, p] of this.pendingRequests) {
      p.reject(new Error(reason));
    }
    this.pendingRequests.clear();
  }

  async _scheduleReconnect() {
    let delay = RECONNECT_INTERVAL;
    const maxDelay = 60_000;

    while (this.shouldRun) {
      log('info', 'HA-WS', `Reconnecting in ${delay}ms …`);
      await sleep(delay);
      if (!this.shouldRun) return;

      try {
        await this.connect();
        log('info', 'HA-WS', 'Reconnected successfully');
        if (this.onConnected) await this.onConnected();
        return;
      } catch (err) {
        log('error', 'HA-WS', `Reconnect failed: ${err.message}`);
        delay = Math.min(delay * 2, maxDelay);
      }
    }
  }
}

// ─── Azure SignalR Client ───────────────────────────────────

class SignalRClient {
  constructor(negotiateUrl, diagnostics) {
    this.negotiateUrl = negotiateUrl;
    this.diagnostics = diagnostics;
    this.connection = null;
    this.shouldRun = true;

    // Callbacks set by Bridge
    this.onCommand = null;
    this.onReconnected = null;
  }

  // --- Public API ---

  async connect() {
    // Step 1: Negotiate to get fresh URL + JWT
    this.diagnostics.updateConnection('signalr', {
      status: 'connecting',
      lastError: null,
    });

    let negotiation;
    try {
      negotiation = await this._negotiate();
    } catch (err) {
      this.diagnostics.updateConnection('signalr', {
        status: 'disconnected',
      }, false);
      this.diagnostics.noteError('signalr', err, { phase: 'negotiate' });
      throw err;
    }

    const { url, accessToken } = negotiation;

    // Step 2: Dispose previous connection if any
    if (this.connection) {
      try {
        await this.connection.stop();
      } catch { /* ignore */ }
    }

    // Step 3: Build new connection with fresh token
    this.connection = new signalR.HubConnectionBuilder()
      .withUrl(url, {
        accessTokenFactory: () => accessToken,
        transport: signalR.HttpTransportType.WebSockets,
        skipNegotiation: true,
      })
      .withAutomaticReconnect({
        nextRetryDelayInMilliseconds: (ctx) => {
          // Exponential backoff: 1s, 2s, 4s, 8s … max 30s — for transient failures
          if (ctx.previousRetryCount < 8) {
            return Math.min(1000 * Math.pow(2, ctx.previousRetryCount), 30_000);
          }
          // After 8 retries, give up and let onclose trigger a full re-negotiate
          return null;
        },
      })
      .configureLogging(signalR.LogLevel.Warning)
      .build();

    this.connection.keepAliveIntervalInMilliseconds = SIGNALR_KEEPALIVE;

    // Step 4: Register hub method handlers
    this._registerHandlers();

    // Step 5: Lifecycle hooks
    this.connection.onreconnecting((err) => {
      log('warn', 'SIGNALR', `Reconnecting (transient)… ${err?.message || ''}`);
      this.diagnostics.updateConnection('signalr', {
        status: 'reconnecting',
      }, false);
      if (err) this.diagnostics.noteError('signalr', err, { phase: 'reconnecting' });
    });

    this.connection.onreconnected((connId) => {
      log('info', 'SIGNALR', `Reconnected (transient), connectionId=${connId}`);
      this.diagnostics.updateConnection('signalr', {
        status: 'connected',
        lastConnectedAt: nowIso(),
        lastError: null,
      });
      this.diagnostics.recordSystem('signalr.reconnected', { connectionId: connId });
      if (this.onReconnected) this.onReconnected();
    });

    this.connection.onclose(async (err) => {
      log('warn', 'SIGNALR', `Connection closed: ${err?.message || 'no error'}`);
      this.diagnostics.updateConnection('signalr', {
        status: this.shouldRun ? 'disconnected' : 'stopped',
        lastDisconnectedAt: nowIso(),
      }, false);
      if (err) this.diagnostics.noteError('signalr', err, { phase: 'close' });
      // Token likely expired or max retries exceeded — full re-negotiate
      if (this.shouldRun) await this._fullReconnect();
    });

    // Step 6: Start
    await this.connection.start();
    log('info', 'SIGNALR', `Connected to hub: ${url}`);
    this.diagnostics.updateConnection('signalr', {
      status: 'connected',
      lastConnectedAt: nowIso(),
      lastError: null,
    });
    this.diagnostics.recordSystem('signalr.connected', { hubUrl: url.split('?')[0] });
  }

  async send(method, ...args) {
    const payload = args.length <= 1 ? args[0] : args;

    if (this.connection?.state !== signalR.HubConnectionState.Connected) {
      log('warn', 'SIGNALR', `Cannot send "${method}" — not connected (state=${this.connection?.state})`);
      this.diagnostics.noteError('signalr', new Error(`Cannot send ${method} while disconnected`), {
        phase: 'send',
        method,
        state: this.connection?.state,
      });
      return false;
    }

    try {
      await this.connection.invoke(method, ...args);
      log('debug', 'SIGNALR', `Invoked "${method}"`);
      this.diagnostics.recordFlow('to-azure', method, payload, {
        transport: 'signalr.invoke',
      });
      return true;
    } catch (err) {
      log('error', 'SIGNALR', `Failed to invoke "${method}": ${err.message}`);
      this.diagnostics.noteError('signalr', err, {
        phase: 'send',
        method,
      });
      return false;
    }
  }

  async stop() {
    this.shouldRun = false;
    this.diagnostics.updateConnection('signalr', {
      status: 'stopped',
    });
    try {
      await this.connection?.stop();
    } catch { /* ignore */ }
  }

  // --- Internal ---

  async _negotiate() {
    log('info', 'SIGNALR', `Negotiating → ${this.negotiateUrl}`);
    const resp = await fetch(this.negotiateUrl);
    if (!resp.ok) {
      const body = await resp.text().catch(() => '');
      throw new Error(`Negotiate HTTP ${resp.status}: ${body.slice(0, 200)}`);
    }
    const data = await resp.json();
    if (!data.url || !data.accessToken) {
      throw new Error('Negotiate response missing "url" or "accessToken"');
    }
    log('info', 'SIGNALR', `Negotiate OK → hub=${data.url.split('?')[0]}`);
    this.diagnostics.updateConnection('signalr', {
      status: 'negotiated',
      hubUrl: data.url.split('?')[0],
      lastNegotiatedAt: nowIso(),
      lastError: null,
    });
    this.diagnostics.recordSystem('signalr.negotiated', {
      hubUrl: data.url.split('?')[0],
    });
    return data;
  }

  _registerHandlers() {
    // ── CallService ──────────────────────────────────────
    // Expected payload from SignalR:
    //   method: "CallService"
    //   args: { domain, service, service_data?, target? }
    this.connection.on('CallService', async (payload) => {
      log('info', 'SIGNALR', `⇐ CallService: ${payload?.domain}.${payload?.service}`);
      this.diagnostics.recordFlow('from-azure', 'CallService', payload);
      if (this.onCommand) {
        try {
          const result = await this.onCommand('call_service', payload);
          await this.send('CommandResult', {
            command: 'CallService',
            success: true,
            result,
            correlationId: payload?.correlationId,
          });
        } catch (err) {
          await this.send('CommandResult', {
            command: 'CallService',
            success: false,
            error: err.message,
            correlationId: payload?.correlationId,
          });
        }
      }
    });

    // ── GetStates ────────────────────────────────────────
    this.connection.on('GetStates', async (payload) => {
      log('info', 'SIGNALR', '⇐ GetStates');
      this.diagnostics.recordFlow('from-azure', 'GetStates', payload || {});
      if (this.onCommand) {
        try {
          const states = await this.onCommand('get_states', payload || {});
          await this.send('StatesResult', {
            success: true,
            states,
            correlationId: payload?.correlationId,
          });
        } catch (err) {
          await this.send('StatesResult', {
            success: false,
            error: err.message,
            correlationId: payload?.correlationId,
          });
        }
      }
    });

    // ── ExecuteCommand (generic) ─────────────────────────
    // Flexible catch-all for any HA WS command
    // Expected: { type: "...", ...other fields }
    this.connection.on('ExecuteCommand', async (payload) => {
      log('info', 'SIGNALR', '⇐ ExecuteCommand', payload?.type || '');
      this.diagnostics.recordFlow('from-azure', 'ExecuteCommand', payload || {});
      if (this.onCommand) {
        try {
          const result = await this.onCommand('execute', payload);
          await this.send('CommandResult', {
            command: 'ExecuteCommand',
            success: true,
            result,
            correlationId: payload?.correlationId,
          });
        } catch (err) {
          await this.send('CommandResult', {
            command: 'ExecuteCommand',
            success: false,
            error: err.message,
            correlationId: payload?.correlationId,
          });
        }
      }
    });
  }

  async _fullReconnect() {
    let delay = RECONNECT_INTERVAL;
    const maxDelay = 60_000;

    while (this.shouldRun) {
      log('info', 'SIGNALR', `Full re-negotiate in ${delay}ms …`);
      await sleep(delay);
      if (!this.shouldRun) return;

      try {
        await this.connect();
        if (this.onReconnected) this.onReconnected();
        return;
      } catch (err) {
        log('error', 'SIGNALR', `Re-negotiate failed: ${err.message}`);
        delay = Math.min(delay * 2, maxDelay);
      }
    }
  }
}

// ─── Bridge — Orchestrator ──────────────────────────────────

class Bridge {
  constructor(diagnostics) {
    this.diagnostics = diagnostics;
    this.ha = new HAClient(diagnostics);
    this.signalr = new SignalRClient(NEGOTIATE_URL, diagnostics);
  }

  async start() {
    this.diagnostics.recordSystem('bridge.starting', {
      negotiateUrl: NEGOTIATE_URL,
      haWsUrl: HA_WS_URL,
      subscribeEvents: SUBSCRIBE_EVENTS,
    });
    log('info', 'BRIDGE', '════════════════════════════════════════');
    log('info', 'BRIDGE', ' Nestor SignalR Bridge — Starting');
    log('info', 'BRIDGE', `  Negotiate : ${NEGOTIATE_URL}`);
    log('info', 'BRIDGE', `  HA WS     : ${HA_WS_URL}`);
    log('info', 'BRIDGE', `  Events    : ${SUBSCRIBE_EVENTS.join(', ')}`);
    log('info', 'BRIDGE', `  Log level : ${LOG_LEVEL}`);
    log('info', 'BRIDGE', '════════════════════════════════════════');

    if (!SUPERVISOR_TOKEN) {
      log('error', 'BRIDGE', 'SUPERVISOR_TOKEN is not set — are you running inside an HA add-on?');
      throw new Error('SUPERVISOR_TOKEN is not set');
    }

    // Wire HA -> SignalR
    this.ha.onEvent = (msg) => this._haEventToSignalR(msg);
    this.ha.onConnected = () => this._onHAReconnected();

    // Wire SignalR -> HA
    this.signalr.onCommand = (type, data) => this._signalrCommandToHA(type, data);
    this.signalr.onReconnected = () => this._onSignalRReconnected();

    // Connect HA first (local, fast), then SignalR (remote)
    log('info', 'BRIDGE', '── Step 1/3: Connecting to Home Assistant ──');
    await this.ha.connect();

    log('info', 'BRIDGE', '── Step 2/3: Subscribing to HA events ──');
    await this._subscribeAllEvents();

    log('info', 'BRIDGE', '── Step 3/3: Connecting to Azure SignalR ──');
    await this.signalr.connect();

    log('info', 'BRIDGE', '══ Bridge is LIVE ══');
    this.diagnostics.recordSystem('bridge.live', {
      diagnosticsUrl: `http://0.0.0.0:${DIAGNOSTICS_PORT}`,
    });
  }

  async stop() {
    log('info', 'BRIDGE', 'Shutting down …');
    this.ha.close();
    await this.signalr.stop();
    this.diagnostics.recordSystem('bridge.stopped', {});
    log('info', 'BRIDGE', 'Shutdown complete');
  }

  // --- HA -> SignalR (event forwarding) ---

  _haEventToSignalR(msg) {
    const eventType = msg.event?.event_type;
    const eventData = msg.event?.data;

    if (eventType === 'state_changed') {
      // Send a specialised message for state changes
      const { entity_id, new_state, old_state } = eventData || {};
      log('debug', 'BRIDGE', `HA→SR state_changed: ${entity_id}`);
      this.signalr.send('StateChanged', {
        entity_id,
        new_state,
        old_state,
        timestamp: msg.event?.time_fired,
      });
    } else {
      // Forward any other subscribed event generically
      log('debug', 'BRIDGE', `HA→SR event: ${eventType}`);
      this.signalr.send('EventReceived', {
        event_type: eventType,
        data: eventData,
        origin: msg.event?.origin,
        timestamp: msg.event?.time_fired,
      });
    }
  }

  // --- SignalR -> HA (command execution) ---

  async _signalrCommandToHA(type, data) {
    switch (type) {
      case 'call_service':
        return this.ha.callService(
          data.domain,
          data.service,
          data.service_data || data.data || {},
          data.target
        );

      case 'get_states':
        return this.ha.getStates();

      case 'execute': {
        // Generic HA WS command passthrough
        // The payload must contain a "type" field for the HA WS API
        if (!data?.type) throw new Error('ExecuteCommand payload missing "type" field');
        const { correlationId, ...wsPayload } = data;
        return this.ha._sendCommand(wsPayload);
      }

      default:
        throw new Error(`Unknown command type: ${type}`);
    }
  }

  // --- Reconnection helpers ---

  async _subscribeAllEvents() {
    for (const eventType of SUBSCRIBE_EVENTS) {
      try {
        await this.ha.subscribeEvents(eventType);
      } catch (err) {
        log('error', 'BRIDGE', `Failed to subscribe to "${eventType}": ${err.message}`);
        this.diagnostics.noteError('ha', err, {
          phase: 'subscribe_events',
          eventType,
        });
      }
    }
  }

  async _onHAReconnected() {
    log('info', 'BRIDGE', 'HA reconnected — re-subscribing to events');
    await this._subscribeAllEvents();
  }

  _onSignalRReconnected() {
    log('info', 'BRIDGE', 'SignalR reconnected — bridge remains active');
  }
}

// ─── Entry Point ────────────────────────────────────────────

async function main() {
  const diagnostics = new DiagnosticsStore(DIAGNOSTICS_HISTORY_LIMIT);
  const diagnosticsServer = new DiagnosticsServer(diagnostics, DIAGNOSTICS_PORT);
  let bridge = new Bridge(diagnostics);
  let stopping = false;

  diagnosticsServer.start();

  // Graceful shutdown
  const shutdown = async (signal) => {
    if (stopping) return;
    stopping = true;
    log('info', 'MAIN', `Received ${signal}, shutting down …`);
    await bridge.stop();
    await diagnosticsServer.stop();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  while (!stopping) {
    try {
      await bridge.start();
      return;
    } catch (err) {
      diagnostics.recordFlow('error', 'bridge.start_failed', err, {
        retryInMs: RECONNECT_INTERVAL,
      });
      log('error', 'MAIN', `Startup error: ${err.message}`);
      log('error', 'MAIN', err.stack);
      await bridge.stop();
      if (stopping) return;
      log('warn', 'MAIN', `Retrying startup in ${RECONNECT_INTERVAL}ms`);
      await sleep(RECONNECT_INTERVAL);
      bridge = new Bridge(diagnostics);
    }
  }
}

main().catch((err) => {
  log('error', 'MAIN', `Unhandled fatal error: ${err.message}`);
  log('error', 'MAIN', err.stack);
  process.exit(1);
});
