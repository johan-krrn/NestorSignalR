'use strict';

// ─────────────────────────────────────────────────────────────
//  Nestor SignalR Bridge
//  Pont bidirectionnel : Home Assistant WS  <->  Azure SignalR
// ─────────────────────────────────────────────────────────────

const signalR = require('@microsoft/signalr');
const WebSocket = require('ws');
const fs = require('fs');

// ─── Configuration ──────────────────────────────────────────

const OPTIONS_PATH = '/data/options.json';
const HA_WS_URL = 'ws://supervisor/core/websocket';

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

// ─── Home Assistant WebSocket Client ────────────────────────

class HAClient {
  constructor() {
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

      this.ws = new WebSocket(HA_WS_URL);
      this.authenticated = false;
      this.msgId = 0;
      this.pendingRequests.clear();

      let settled = false;

      this.ws.on('open', () => {
        log('info', 'HA-WS', 'TCP connection established');
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
        this._rejectAllPending('Connection closed');
        if (!settled) {
          settled = true;
          reject(new Error(`WS closed before auth (code=${code})`));
        }
        if (this.shouldRun) this._scheduleReconnect();
      });

      this.ws.on('error', (err) => {
        log('error', 'HA-WS', `Socket error: ${err.message}`);
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
        authCb(null);
        break;

      case 'auth_invalid':
        log('error', 'HA-WS', `Authentication failed: ${msg.message}`);
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
  constructor(negotiateUrl) {
    this.negotiateUrl = negotiateUrl;
    this.connection = null;
    this.shouldRun = true;

    // Callbacks set by Bridge
    this.onCommand = null;
    this.onReconnected = null;
  }

  // --- Public API ---

  async connect() {
    // Step 1: Negotiate to get fresh URL + JWT
    const { url, accessToken } = await this._negotiate();

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
    });

    this.connection.onreconnected((connId) => {
      log('info', 'SIGNALR', `Reconnected (transient), connectionId=${connId}`);
      if (this.onReconnected) this.onReconnected();
    });

    this.connection.onclose(async (err) => {
      log('warn', 'SIGNALR', `Connection closed: ${err?.message || 'no error'}`);
      // Token likely expired or max retries exceeded — full re-negotiate
      if (this.shouldRun) await this._fullReconnect();
    });

    // Step 6: Start
    await this.connection.start();
    log('info', 'SIGNALR', `Connected to hub: ${url}`);
  }

  async send(method, ...args) {
    if (this.connection?.state !== signalR.HubConnectionState.Connected) {
      log('warn', 'SIGNALR', `Cannot send "${method}" — not connected (state=${this.connection?.state})`);
      return;
    }
    try {
      await this.connection.invoke(method, ...args);
      log('debug', 'SIGNALR', `Invoked "${method}"`);
    } catch (err) {
      log('error', 'SIGNALR', `Failed to invoke "${method}": ${err.message}`);
    }
  }

  async stop() {
    this.shouldRun = false;
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
    return data;
  }

  _registerHandlers() {
    // ── CallService ──────────────────────────────────────
    // Expected payload from SignalR:
    //   method: "CallService"
    //   args: { domain, service, service_data?, target? }
    this.connection.on('CallService', async (payload) => {
      log('info', 'SIGNALR', `⇐ CallService: ${payload?.domain}.${payload?.service}`);
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
  constructor() {
    this.ha = new HAClient();
    this.signalr = new SignalRClient(NEGOTIATE_URL);
  }

  async start() {
    log('info', 'BRIDGE', '════════════════════════════════════════');
    log('info', 'BRIDGE', ' Nestor SignalR Bridge — Starting');
    log('info', 'BRIDGE', `  Negotiate : ${NEGOTIATE_URL}`);
    log('info', 'BRIDGE', `  HA WS     : ${HA_WS_URL}`);
    log('info', 'BRIDGE', `  Events    : ${SUBSCRIBE_EVENTS.join(', ')}`);
    log('info', 'BRIDGE', `  Log level : ${LOG_LEVEL}`);
    log('info', 'BRIDGE', '════════════════════════════════════════');

    if (!SUPERVISOR_TOKEN) {
      log('error', 'BRIDGE', 'SUPERVISOR_TOKEN is not set — are you running inside an HA add-on?');
      process.exit(1);
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
  }

  async stop() {
    log('info', 'BRIDGE', 'Shutting down …');
    this.ha.close();
    await this.signalr.stop();
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
  const bridge = new Bridge();

  // Graceful shutdown
  const shutdown = async (signal) => {
    log('info', 'MAIN', `Received ${signal}, shutting down …`);
    await bridge.stop();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  try {
    await bridge.start();
  } catch (err) {
    log('error', 'MAIN', `Fatal startup error: ${err.message}`);
    log('error', 'MAIN', err.stack);
    process.exit(1);
  }
}

main();
