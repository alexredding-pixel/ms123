/**
 * Maritime Sentinel - AIS WebSocket Proxy + Event Persistence
 *
 * What this does:
 *  1. Maintains a permanent AIS connection to aisstream.io (24/7, browser-independent)
 *  2. Forwards live messages to any connected browser clients
 *  3. Logs destination changes to voyage-events.json so history survives restarts
 *  4. GET /events     -> full event log as JSON
 *  5. GET /health     -> status check
 *  6. POST /subscribe -> update API key + tracked MMSIs from the dashboard
 */

const { WebSocket, WebSocketServer } = require('ws');
const http = require('http');
const fs   = require('fs');
const path = require('path');

const PROXY_PORT    = process.env.PORT || 8765;
const AISSTREAM_URL = 'wss://stream.aisstream.io/v0/stream';
const EVENTS_FILE   = path.join(__dirname, 'voyage-events.json');
const CONFIG_FILE   = path.join(__dirname, 'proxy-config.json');

let aisSocket      = null;
let reconnectTimer = null;
let apiKey         = '';
let trackedMmsis   = [];
let connectedCount = 0;
const lastDest     = {};

// ── EVENT LOG ─────────────────────────────────────────────────────────────────
function loadEvents() {
  try {
    if (fs.existsSync(EVENTS_FILE)) return JSON.parse(fs.readFileSync(EVENTS_FILE, 'utf8'));
  } catch (e) {}
  return [];
}

function logEvent(evt) {
  const events = loadEvents();
  const tenMinsAgo = Date.now() - 600000;
  const dup = events.some(e =>
    e.mmsi === evt.mmsi && e.destination === evt.destination &&
    new Date(e.timestamp).getTime() > tenMinsAgo
  );
  if (dup) return;
  events.push(evt);
  if (events.length > 1000) events.splice(0, events.length - 1000);
  try { fs.writeFileSync(EVENTS_FILE, JSON.stringify(events, null, 2)); } catch (e) {}
  console.log(`[events] ${evt.mmsi} -> ${evt.destination} ETA:${evt.eta || '?'}`);
}

// ── CONFIG ────────────────────────────────────────────────────────────────────
function loadConfig() {
  try {
    if (fs.existsSync(CONFIG_FILE)) {
      const c = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
      apiKey = c.apiKey || process.env.AIS_API_KEY || '';
      trackedMmsis = c.trackedMmsis || [];
      console.log(`[config] ${trackedMmsis.length} vessels, key: ${apiKey ? 'set' : 'missing'}`);
    } else {
      apiKey = process.env.AIS_API_KEY || '';
    }
  } catch (e) {}
}

function saveConfig() {
  try { fs.writeFileSync(CONFIG_FILE, JSON.stringify({ apiKey, trackedMmsis }, null, 2)); } catch (e) {}
}

// ── AIS MESSAGE INSPECTION ────────────────────────────────────────────────────
function inspectMessage(raw) {
  try {
    const msg = JSON.parse(raw);
    if (msg.MessageType !== 'ShipStaticData') return;
    const meta = msg.MetaData || {};
    const sd   = msg.Message?.ShipStaticData || {};
    const mmsi = String(meta.MMSI_String || meta.MMSI || '');
    const dest = (sd.Destination || '').trim().toUpperCase();
    if (!mmsi || !dest || dest === 'UNKNOWN' || dest === '0') return;
    if (lastDest[mmsi] === dest) return;
    lastDest[mmsi] = dest;

    let etaStr = null;
    if (sd.Eta) {
      const e = sd.Eta;
      if (e.Month && e.Day) {
        const yr = new Date().getFullYear();
        etaStr = `${yr}-${String(e.Month).padStart(2,'0')}-${String(e.Day).padStart(2,'0')} ${String(e.Hour||0).padStart(2,'0')}:${String(e.Minute||0).padStart(2,'0')} UTC`;
      }
    }

    logEvent({
      mmsi, destination: dest, eta: etaStr,
      lat: meta.latitude ?? null, lng: meta.longitude ?? null,
      timestamp: new Date().toISOString(),
    });
  } catch (e) {}
}

// ── AIS CONNECTION ────────────────────────────────────────────────────────────
function connectToAIS() {
  if (!apiKey || trackedMmsis.length === 0) {
    console.log('[ais] Waiting for API key and vessel list...');
    return;
  }
  if (aisSocket && (aisSocket.readyState === WebSocket.OPEN ||
                    aisSocket.readyState === WebSocket.CONNECTING)) return;

  console.log(`[ais] Connecting (${trackedMmsis.length} vessels)...`);
  aisSocket = new WebSocket(AISSTREAM_URL);

  aisSocket.on('open', () => {
    console.log('[ais] Connected');
    aisSocket.send(JSON.stringify({
      APIKey:          apiKey,
      BoundingBoxes:   [[[-90, -180], [90, 180]]],
      FiltersShipMMSI: trackedMmsis,
    }));
    const ping = setInterval(() => {
      if (aisSocket.readyState === WebSocket.OPEN) aisSocket.ping();
      else clearInterval(ping);
    }, 30000);
    aisSocket.on('close', () => clearInterval(ping));
  });

  aisSocket.on('message', (data, isBinary) => {
    const raw = isBinary ? data.toString('utf8') : data.toString();
    inspectMessage(raw);
    wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(raw); });
  });

  aisSocket.on('error', err => console.error('[ais] Error:', err.message));

  aisSocket.on('close', (code) => {
    console.log(`[ais] Closed (${code}) - reconnecting in 30s`);
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectToAIS, 30000);
  });
}

// ── HTTP SERVER ───────────────────────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  if (req.method === 'GET' && req.url === '/events') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(loadEvents()));
    return;
  }

  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      aisConnected: aisSocket?.readyState === WebSocket.OPEN,
      browsers: connectedCount,
      vessels: trackedMmsis.length,
      events: loadEvents().length,
      uptime: Math.round(process.uptime()) + 's',
    }));
    return;
  }

  if (req.method === 'POST' && req.url === '/subscribe') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      try {
        const { apiKey: k, mmsis } = JSON.parse(body);
        if (k) apiKey = k;
        if (Array.isArray(mmsis) && mmsis.length) trackedMmsis = mmsis;
        saveConfig();
        if (aisSocket) { aisSocket.close(); aisSocket = null; }
        clearTimeout(reconnectTimer);
        setTimeout(connectToAIS, 500);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, trackedMmsis }));
      } catch (e) {
        res.writeHead(400); res.end(JSON.stringify({ error: 'Bad JSON' }));
      }
    });
    return;
  }

  res.writeHead(404); res.end('Not found');
});

// ── WEBSOCKET SERVER ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', browserSocket => {
  connectedCount++;
  console.log(`[ws] Browser connected (${connectedCount} total)`);
  if (!aisSocket || aisSocket.readyState !== WebSocket.OPEN) connectToAIS();

  browserSocket.on('message', data => {
    // Sniff the subscription message for API key and MMSIs so proxy stays in sync
    try {
      const sub = JSON.parse(data.toString());
      if (sub.APIKey) {
        const changed = apiKey !== sub.APIKey ||
          JSON.stringify(trackedMmsis.sort()) !== JSON.stringify((sub.FiltersShipMMSI||[]).sort());
        apiKey = sub.APIKey;
        trackedMmsis = sub.FiltersShipMMSI || trackedMmsis;
        if (changed) saveConfig();
      }
    } catch (e) {}
    if (aisSocket?.readyState === WebSocket.OPEN) aisSocket.send(data.toString());
  });

  browserSocket.on('close', () => {
    connectedCount = Math.max(0, connectedCount - 1);
    console.log(`[ws] Browser disconnected (${connectedCount} total)`);
  });

  browserSocket.on('error', err => console.error('[ws] Error:', err.message));
});

// ── START ─────────────────────────────────────────────────────────────────────
loadConfig();
httpServer.listen(PROXY_PORT, () => {
  console.log(`\n=== Maritime Sentinel Proxy ===`);
  console.log(`    Port: ${PROXY_PORT}`);
  console.log(`    GET  /events    -> voyage event log`);
  console.log(`    GET  /health    -> status`);
  console.log(`    POST /subscribe -> update vessels`);
  console.log('');
  if (apiKey && trackedMmsis.length > 0) connectToAIS();
  else console.log('    Waiting for first browser connection...\n');
});
