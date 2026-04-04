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
 *  7. POST /parse-bl   -> extract page 1 of B/L PDF and parse via Claude API
 */

const { WebSocket, WebSocketServer } = require('ws');
const http   = require('http');
const https  = require('https');
const fs     = require('fs');
const path   = require('path');
const crypto = require('crypto');

// Constant-time string comparison — prevents timing attacks on the proxy secret
function safeEqual(a, b) {
  try {
    const ba = Buffer.from(String(a));
    const bb = Buffer.from(String(b));
    if (ba.length !== bb.length) return false;
    return crypto.timingSafeEqual(ba, bb);
  } catch { return false; }
}

// Normalise a raw AIS destination string to just the terminal port.
// AIS destination is free-text typed by officers — common formats include:
//   "SGSIN>GBFXT"       → "GBFXT"
//   "SG SIN > LKCMB"   → "LKCMB"
//   "CNSHA>GBFXT>DEHAM" → "DEHAM"  (multi-hop — take last)
//   "FELIXSTOWE"        → "FELIXSTOWE"  (plain — unchanged)
function normaliseAisDest(raw) {
  if (!raw) return '';
  const clean = raw.trim().toUpperCase();
  if (!clean.includes('>')) return clean;
  const parts = clean.split('>').map(p => p.trim()).filter(Boolean);
  return parts[parts.length - 1];
}

// UN/LOCODE → port name fragments — mirrors the dashboard LOCODE_MAP
const LOCODE_MAP = {
  'GBFXT': ['FELIXSTOWE'],
  'GBLGP': ['LONDON','GATEWAY'],
  'GBSOU': ['SOUTHAMPTON'],
  'GBLIV': ['LIVERPOOL'],
  'GBTIL': ['TILBURY'],
  'GBIMM': ['IMMINGHAM'],
  'GBHUL': ['HULL'],
  'DEHAM': ['HAMBURG'],
  'NLRTM': ['ROTTERDAM'],
  'BEANR': ['ANTWERP'],
  'DEBRV': ['BREMERHAVEN'],
  'FRLEH': ['HAVRE','LE HAVRE'],
  'ESBCN': ['BARCELONA'],
  'ESVLC': ['VALENCIA'],
  'ESALG': ['ALGECIRAS'],
  'GRPIR': ['PIRAEUS'],
  'ITGOA': ['GENOA'],
  'CNSHA': ['SHANGHAI'],
  'CNNBO': ['NINGBO'],
  'CNSHE': ['SHENZHEN'],
  'CNQIN': ['QINGDAO'],
  'CNTAO': ['QINGDAO'],
  'CNTSN': ['TIANJIN'],
  'HKHKG': ['HONG KONG','HONGKONG'],
  'SGSIN': ['SINGAPORE'],
  'MYPKG': ['KLANG','PORT KLANG'],
  'MYTPP': ['TANJUNG','PELEPAS'],
  'INNSA': ['NHAVA','SHEVA'],
  'INMUN': ['MUNDRA'],
  'INMAA': ['CHENNAI'],
  'AEDXB': ['DUBAI'],
  'AEJEA': ['JEBEL','ALI'],
  'LKCMB': ['COLOMBO'],
  'USNYC': ['NEW YORK'],
  'USLAX': ['LOS ANGELES'],
  'USLGB': ['LONG BEACH'],
  'USHOU': ['HOUSTON'],
  'USSAV': ['SAVANNAH'],
  'ZADUR': ['DURBAN'],
  'ZACPT': ['CAPE TOWN'],
  'KEOMB': ['MOMBASA'],
  'AUSYD': ['SYDNEY'],
  'AUMEL': ['MELBOURNE'],
  'KRPUS': ['BUSAN'],
  'MAPTM': ['TANGER'],
  'LKCMB': ['COLOMBO'],
  'USBOS': ['BOSTON'],
  'USORF': ['NORFOLK'],
  'PNCT' : ['NEWARK'],
};

// Returns true if aisDest (LOCODE or plain text) matches the user's discharge port string
function destIsFinal(aisDest, userDest) {
  if (!aisDest || !userDest) return false;
  const norm = normaliseAisDest(aisDest);
  // Split user dest into words BEFORE stripping so "London Gateway, UK" → ['LONDON','GATEWAY','UK']
  const userWords = userDest.toUpperCase()
    .replace(/[^A-Z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length >= 3);

  // 1. LOCODE lookup
  const locodeWords = LOCODE_MAP[norm];
  if (locodeWords) {
    return locodeWords.some(lw =>
      userWords.some(uw => uw.includes(lw.replace(/\s+/g,'').slice(0,5))
                        || lw.replace(/\s+/g,'').slice(0,5).includes(uw.slice(0,5)))
    );
  }

  // 2. Fuzzy plain-text match
  const nc = norm.replace(/[^A-Z0-9]/g,'');
  return userWords.some(w => {
    const wc = w.replace(/[^A-Z0-9]/g,'');
    return wc.length >= 4 && (nc.includes(wc.slice(0,5)) || wc.slice(0,5).includes(nc.slice(0,5)));
  });
}

// ── ANTHROPIC API (key stored as env var — never exposed to browser) ────────────
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY || '';

// Extract first page of a base64 PDF as base64
// Strategy: find the first page boundary using PDF structure markers,
// then return just that portion. Falls back to full PDF if parsing fails.
function extractFirstPage(base64Pdf) {
  try {
    const buf   = Buffer.from(base64Pdf, 'base64');
    const str   = buf.toString('binary');
    // Find second page object or EOF — whichever comes first
    // PDF pages are separated by /Page markers in the xref table
    // Simple heuristic: find the 2nd occurrence of 'endobj' after first 'Page'
    const pageIdx = str.indexOf('/Type /Page');
    if (pageIdx === -1) return base64Pdf; // Can't detect, send whole thing
    // Find second /Type /Page — that marks start of page 2
    const page2Idx = str.indexOf('/Type /Page', pageIdx + 10);
    if (page2Idx === -1) return base64Pdf; // Single page doc

    // Walk back to find the obj header for page 2
    const cutIdx = str.lastIndexOf('\nendobj', page2Idx);
    if (cutIdx === -1 || cutIdx < 1000) return base64Pdf;

    // Build a minimal valid PDF from just the first portion
    // Actually simpler: just send the first ~40KB which always covers page 1
    // Average BL page 1 is 15-25KB of raw PDF content
    const PAGE1_BYTE_LIMIT = 60 * 1024; // 60KB — comfortably covers page 1
    if (buf.length <= PAGE1_BYTE_LIMIT) return base64Pdf; // Already small
    return buf.slice(0, PAGE1_BYTE_LIMIT).toString('base64');
  } catch(e) {
    return base64Pdf; // Fall back to full PDF
  }
}

// ── SUPABASE CLIENT (lightweight — no SDK needed) ─────────────────────────────
const SUPABASE_URL = 'https://nkxvacdhwimhemnmcpxe.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
if (!SUPABASE_KEY) { console.error('[FATAL] SUPABASE_SERVICE_KEY env var not set — exiting'); process.exit(1); }

function supabase(method, table, body, params) {
  return new Promise((resolve) => {
    let url = SUPABASE_URL + '/rest/v1/' + table;
    if (params) url += '?' + params;
    const bodyStr = body ? JSON.stringify(body) : null;
    const req = https.request(url, {
      method,
      headers: {
        'apikey':        SUPABASE_KEY,
        'Authorization': 'Bearer ' + SUPABASE_KEY,
        'Content-Type':  'application/json',
        'Prefer':        method === 'POST' ? 'resolution=merge-duplicates' : '',
      },
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve({ ok: res.statusCode < 300, data: data ? JSON.parse(data) : null, status: res.statusCode }); }
        catch(e) { resolve({ ok: false, data: null }); }
      });
    });
    req.on('error', () => resolve({ ok: false, data: null }));
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

// Serve the dashboard HTML so it runs on the Railway domain (fixes CORS)
function serveDashboard(req, res) {
  const htmlPath = path.join(__dirname, 'maritime-sentinel.html');
  if (!fs.existsSync(htmlPath)) {
    res.writeHead(404); res.end('maritime-sentinel.html not found in deploy'); return;
  }
  const html = fs.readFileSync(htmlPath);
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
}

const PROXY_PORT    = process.env.PORT || 8765;
const AISSTREAM_URL = 'wss://stream.aisstream.io/v0/stream';
const CONFIG_FILE   = path.join(__dirname, 'proxy-config.json');

let aisSocket      = null;
let reconnectTimer = null;
let apiKey         = '';
let trackedMmsis   = [];
let connectedCount = 0;
const lastDest     = {};
const parseRateLimit = new Map(); // ip -> { count, resetAt } — rate limits /parse-bl

// Track last time each MMSI sent a message via aisstream
const lastAisMessage = {};

// Arrival detection state — mirrors dashboard logic server-side
// so ATAs are captured even when browser is closed
const lastNavStatus = {}; // mmsi -> 'In Transit' | 'In Port' | 'At Anchor'

const NAV_STATUS_MAP = {
  0: 'In Transit', 1: 'At Anchor', 2: 'In Transit', 3: 'In Transit',
  4: 'In Transit', 5: 'In Port',   6: 'In Transit', 7: 'In Transit',
  8: 'In Transit', 11: 'In Port',  12: 'In Port',   15: 'In Transit',
};

// ── EVENT LOG ─────────────────────────────────────────────────────────────────
// Last-known position store — Supabase (survives redeploys, syncs across devices)
async function logPosition(pos) {
  await supabase('POST', 'vessel_positions',
    { mmsi: pos.mmsi, data: pos, updated_at: new Date().toISOString() },
    'on_conflict=mmsi'
  );
}

async function loadPositions() {
  const res = await supabase('GET', 'vessel_positions', null, 'select=mmsi,data');
  if (!res.ok || !Array.isArray(res.data)) return {};
  const out = {};
  res.data.forEach(row => { out[row.mmsi] = row.data; });
  return out;
}

async function loadEvents() {
  const res = await supabase('GET', 'voyage_events', null,
    'select=mmsi,destination,eta,lat,lng,dest_changed,timestamp&order=timestamp.asc&limit=1000'
  );
  return (res.ok && Array.isArray(res.data)) ? res.data : [];
}

async function logEvent(evt) {
  // Dedup: skip if same mmsi+dest+eta logged in last 10 minutes
  const tenMinsAgo = new Date(Date.now() - 600000).toISOString();
  const check = await supabase('GET', 'voyage_events', null,
    'mmsi=eq.' + evt.mmsi +
    '&destination=eq.' + encodeURIComponent(evt.destination) +
    '&eta=eq.' + encodeURIComponent(evt.eta || '') +
    '&timestamp=gte.' + tenMinsAgo +
    '&limit=1'
  );
  if (check.ok && check.data && check.data.length > 0) return;

  await supabase('POST', 'voyage_events', {
    mmsi:         evt.mmsi,
    destination:  evt.destination,
    eta:          evt.eta || null,
    lat:          evt.lat  || null,
    lng:          evt.lng  || null,
    dest_changed: evt.destChanged !== false,
    timestamp:    evt.timestamp || new Date().toISOString(),
  });
  console.log('[events] ' + evt.mmsi + ' -> ' + evt.destination + ' ETA:' + (evt.eta || '?'));
}

// ── CONFIG ────────────────────────────────────────────────────────────────────
function loadConfig() {
  try {
    // AIS key from env var ONLY — never read from disk (keeps key off filesystem)
    apiKey = process.env.AIS_API_KEY || '';
    if (fs.existsSync(CONFIG_FILE)) {
      const c = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
      trackedMmsis = c.trackedMmsis || [];
      console.log('[config] ' + trackedMmsis.length + ' vessels, AIS key: ' + (apiKey ? 'set' : 'missing'));
    }
  } catch (e) {}
}

function saveConfig() {
  // Never write apiKey to disk — it lives in env var only
  try { fs.writeFileSync(CONFIG_FILE, JSON.stringify({ trackedMmsis }, null, 2)); } catch (e) {}
}

// Log an arrival event into voyage_events using a special ATA: prefix
// so the dashboard replay can distinguish arrivals from transit events.
async function logArrivalEvent(mmsi, destination, lat, lng, timestamp) {
  const dest   = destination.trim().toUpperCase();
  const ataKey = 'ATA:' + dest;

  // Dedup: skip if we already logged an arrival at this port in last 2 hours
  const twoHoursAgo = new Date(Date.now() - 7200000).toISOString();
  const check = await supabase('GET', 'voyage_events', null,
    'mmsi=eq.' + mmsi +
    '&destination=eq.' + encodeURIComponent(ataKey) +
    '&timestamp=gte.' + twoHoursAgo +
    '&limit=1'
  );
  if (check.ok && check.data && check.data.length > 0) return;

  await supabase('POST', 'voyage_events', {
    mmsi:         mmsi,
    destination:  ataKey,
    eta:          null,
    lat:          lat  || null,
    lng:          lng  || null,
    dest_changed: false,
    timestamp:    timestamp || new Date().toISOString(),
  });
  console.log('[arrival] ' + mmsi + ' arrived at ' + dest + ' @ ' + (timestamp || 'now'));

  // Fire arrival and delay notifications (async, fire-and-forget)
  getShipmentForMmsi(mmsi).then(s => {
    if (!s) return;
    const fin = destIsFinal(dest, s.dest || '');
    notifyArrival(mmsi, dest, timestamp ? new Date(timestamp).toLocaleString('en-GB') : 'now', fin);
  }).catch(() => {});
}

// Log a departure event — stored as ATD:PORTNAME so the dashboard can stamp
// an Actual Time of Departure on the port's timeline step.
async function logDepartureEvent(mmsi, destination, timestamp) {
  const dest   = destination.trim().toUpperCase();
  const atdKey = 'ATD:' + dest;

  // Dedup: skip if we already logged a departure from this port in last 2 hours
  const twoHoursAgo = new Date(Date.now() - 7200000).toISOString();
  const check = await supabase('GET', 'voyage_events', null,
    'mmsi=eq.' + mmsi +
    '&destination=eq.' + encodeURIComponent(atdKey) +
    '&timestamp=gte.' + twoHoursAgo +
    '&limit=1'
  );
  if (check.ok && check.data && check.data.length > 0) return;

  await supabase('POST', 'voyage_events', {
    mmsi:         mmsi,
    destination:  atdKey,
    eta:          null,
    lat:          null,
    lng:          null,
    dest_changed: false,
    timestamp:    timestamp || new Date().toISOString(),
  });
  console.log('[departure] ' + mmsi + ' departed ' + dest + ' @ ' + (timestamp || 'now'));

  // Fire departure notification
  const atdStr = timestamp ? new Date(timestamp).toLocaleString('en-GB') : 'now';
  notifyDeparture(mmsi, dest, atdStr).catch(() => {});
}

// ── AIS MESSAGE INSPECTION ────────────────────────────────────────────────────
async function inspectMessage(raw) {
  try {
    const msg  = JSON.parse(raw);
    const meta = msg.MetaData || {};
    const mmsi = String(meta.MMSI_String || meta.MMSI || '');
    if (!mmsi) return;

    lastAisMessage[mmsi] = Date.now();

    // ── Position Report: persist last known position + detect arrivals ──────────
    if (msg.MessageType === 'PositionReport') {
      const pr  = msg.Message?.PositionReport || {};
      const lat = meta.latitude  ?? pr.Latitude  ?? null;
      const lng = meta.longitude ?? pr.Longitude ?? null;
      const sog = pr.Sog  ?? null;
      const cog = pr.Cog  ?? null;
      const nav = pr.NavigationalStatus ?? null;

      if (lat && lng) {
        await logPosition({ mmsi, lat, lng, sog, cog, navStatus: nav,
                      timestamp: new Date().toISOString() });
      }

      // ── Arrival & departure detection ─────────────────────────────────────────
      if (nav != null) {
        const newStatus   = NAV_STATUS_MAP[nav] || 'In Transit';
        const prevStatus  = lastNavStatus[mmsi]  || 'In Transit';
        const nowInPort   = (newStatus === 'In Port' || newStatus === 'At Anchor')
                         && sog != null && sog < 0.5;
        const nowUnderway = newStatus === 'In Transit' && sog != null && sog >= 0.5;
        const wasUnderway = prevStatus === 'In Transit';
        const wasInPort   = prevStatus === 'In Port' || prevStatus === 'At Anchor';
        const aisTimestamp = meta.time_utc || meta.TimeUtc || new Date().toISOString();

        if (nowInPort && wasUnderway && lastDest[mmsi]) {
          await logArrivalEvent(mmsi, lastDest[mmsi], lat, lng, aisTimestamp);
        }
        if (nowUnderway && wasInPort && lastDest[mmsi]) {
          await logDepartureEvent(mmsi, lastDest[mmsi], aisTimestamp);
        }
        lastNavStatus[mmsi] = newStatus;
      }
      // ─────────────────────────────────────────────────────────────────────────
      return;
    }

    // ── ShipStaticData: log destination/ETA changes ───────────────────────────
    if (msg.MessageType === 'ShipStaticData') {
      const sd  = msg.Message?.ShipStaticData || {};
      const raw = (sd.Destination || '').trim().toUpperCase();
      if (!raw || raw === 'UNKNOWN' || raw === '0') return;

      // Normalise AIS destination — officers often type the full route as
      // "SGSIN>GBFXT" or "SG SIN > LKCMB". Extract the terminal port only.
      const dest = normaliseAisDest(raw);

      let etaStr = null;
      if (sd.Eta) {
        const e = sd.Eta;
        if (e.Month && e.Day) {
          const yr = new Date().getFullYear();
          etaStr = `${yr}-${String(e.Month).padStart(2,'0')}-${String(e.Day).padStart(2,'0')} ${String(e.Hour||0).padStart(2,'0')}:${String(e.Minute||0).padStart(2,'0')} UTC`;
        }
      }

      const destChanged = lastDest[mmsi] !== dest;
      const etaChanged  = etaStr && lastDest[mmsi + '_eta'] !== etaStr;
      if (!destChanged && !etaChanged) return;

      lastDest[mmsi] = dest;
      if (etaStr) {
        lastDest[mmsi + '_eta'] = etaStr;
        // Check for significant ETA drift and notify if threshold exceeded
        checkEtaDrift(mmsi, dest, etaStr).catch(() => {});
      }

      await logEvent({
        mmsi, destination: dest, eta: etaStr,
        lat: meta.latitude ?? null, lng: meta.longitude ?? null,
        timestamp: new Date().toISOString(),
        destChanged,
      });
    }
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
    // Track last message time for this MMSI (for fallback poller)
    try {
      const m = JSON.parse(raw);
      const mmsi = String(m?.MetaData?.MMSI_String || m?.MetaData?.MMSI || '');
      if (mmsi) lastAisMessage[mmsi] = Date.now();
    } catch(e) {}
    inspectMessage(raw); // async — fire and forget
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
  res.setHeader('Access-Control-Allow-Origin', 'https://ms123-production.up.railway.app');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-proxy-secret, Authorization');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // /config is authenticated via Supabase JWT — handled before proxy secret check
  if (req.method === 'GET' && req.url === '/config') {
    const authHeader = req.headers['authorization'] || '';
    if (!authHeader.startsWith('Bearer ')) {
      res.writeHead(401); res.end(JSON.stringify({ error: 'Unauthorized' })); return;
    }
    const sbToken = authHeader.replace('Bearer ', '');
    const vReq = https.request(SUPABASE_URL + '/auth/v1/user', {
      method: 'GET',
      headers: { 'apikey': process.env.SUPABASE_ANON_KEY || '', 'Authorization': 'Bearer ' + sbToken }
    }, (r) => {
      let d = ''; r.on('data', c => d += c);
      r.on('end', () => {
        if (r.statusCode === 200) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ proxySecret: process.env.PROXY_SECRET || '' }));
        } else {
          res.writeHead(401); res.end(JSON.stringify({ error: 'Invalid session' }));
        }
      });
    });
    vReq.on('error', () => { res.writeHead(500); res.end('{}'); });
    vReq.end();
    return;
  }

  // Serve favicon (no auth required)
  if (req.url === '/favicon.ico') {
    res.writeHead(200, { 'Content-Type': 'image/svg+xml' });
    res.end('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" rx="8" fill="#001736"/><text x="16" y="22" text-anchor="middle" font-size="18" fill="white">&#x2693;</text></svg>');
    return;
  }

  // All other endpoints require the shared proxy secret
  // Exempt: page load routes (no secret available yet), health, favicon
  const PROXY_SECRET = process.env.PROXY_SECRET || '';
  const exemptPaths  = ['/', '/index.html', '/health', '/favicon.ico'];
  if (PROXY_SECRET && !exemptPaths.includes(req.url) && !safeEqual(req.headers['x-proxy-secret'], PROXY_SECRET)) {
    res.writeHead(401, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Unauthorized' }));
    return;
  }

  // GET /notify-prefs — return current notification preferences
  if (req.method === 'GET' && req.url === '/notify-prefs') {
    loadNotifPrefs().then(prefs => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(prefs));
    }).catch(() => { res.writeHead(500); res.end('{}'); });
    return;
  }

  // POST /notify-prefs — save notification preferences
  if (req.method === 'POST' && req.url === '/notify-prefs') {
    let body = '';
    req.on('data', c => { body += c; if (body.length > 4096) req.destroy(); });
    req.on('end', async () => {
      try {
        const p = JSON.parse(body);
        // Upsert into notification_prefs — single row keyed by id='default'
        await supabase('POST', 'notification_prefs', {
          id:                         'default',
          notify_eta_change:          !!p.notify_eta_change,
          notify_arrival:             !!p.notify_arrival,
          notify_departure:           !!p.notify_departure,
          notify_delay:               !!p.notify_delay,
          notify_14day:               !!p.notify_14day,
          eta_change_threshold_days:  parseInt(p.eta_change_threshold_days) || 1,
        }, 'on_conflict=id');
        notifPrefs = null; // bust cache
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      } catch(e) {
        res.writeHead(400); res.end(JSON.stringify({ error: 'Bad JSON' }));
      }
    });
    return;
  }

  if (req.method === 'GET' && req.url === '/events') {
    loadEvents().then(events => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(events));
    }).catch(() => { res.writeHead(500); res.end('[]'); });
    return;
  }

  if (req.method === 'GET' && req.url === '/positions') {
    loadPositions().then(positions => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(positions));
    }).catch(() => { res.writeHead(500); res.end('{}'); });
    return;
  }

  // GET /vessel/:mmsi — fetch vessel position via allorigins CORS proxy
  // allorigins.win forwards the request from their server with proper headers
  if (req.method === 'GET' && req.url.startsWith('/vessel/')) {
    const mmsi = req.url.split('/vessel/')[1].split('?')[0].trim();
    if (!mmsi || !/^\d+$/.test(mmsi)) {
      res.writeHead(400); res.end('Invalid MMSI'); return;
    }
    const target = encodeURIComponent(`https://www.vesselfinder.com/api/pub/click/${mmsi}`);
    const proxyUrl = `https://api.allorigins.win/get?url=${target}`;
    const proxyReq = https.request(proxyUrl, { timeout: 10000,
      headers: { 'User-Agent': 'maritime-sentinel/1.0' }
    }, (proxyRes) => {
      let data = '';
      proxyRes.on('data', c => data += c);
      proxyRes.on('end', () => {
        try {
          // allorigins wraps response in { contents: "..." }
          const wrapper = JSON.parse(data);
          const inner   = JSON.parse(wrapper.contents);
          res.writeHead(200, {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': 'https://ms123-production.up.railway.app',
          });
          res.end(JSON.stringify(inner));
        } catch(e) {
          res.writeHead(502); res.end('Bad upstream response');
        }
      });
    });
    proxyReq.on('error', () => { res.writeHead(502); res.end('Upstream error'); });
    proxyReq.on('timeout', () => { proxyReq.destroy(); res.writeHead(504); res.end('Timeout'); });
    proxyReq.end();
    return;
  }

  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      aisConnected: aisSocket?.readyState === WebSocket.OPEN,
      browsers: connectedCount,
      vessels: trackedMmsis.length,
      uptime: Math.round(process.uptime()) + 's',
    }));
    return;
  }

  if (req.method === 'POST' && req.url === '/subscribe') {
    let body = '';
    req.on('data', c => { body += c; if (body.length > 100 * 1024) { req.destroy(); return; } });
    req.on('end', () => {
      try {
        const { apiKey: k, mmsis } = JSON.parse(body);
        if (k) apiKey = k;
        if (Array.isArray(mmsis) && mmsis.length) {
          // Validate every MMSI is a 7–9 digit number — same rule as bootstrap
          const validated = mmsis
            .map(m => String(m).trim())
            .filter(m => /^\d{7,9}$/.test(m));
          if (validated.length !== mmsis.length) {
            console.warn('[subscribe] Rejected ' + (mmsis.length - validated.length) + ' invalid MMSI(s)');
          }
          if (validated.length > 0) trackedMmsis = validated;
        }
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

  // ── POST /parse-bl — extract page 1 and send to Claude for parsing ─────────
  if (req.method === 'POST' && req.url === '/parse-bl') {
    // Rate limit: max 20 parses per IP per hour
    const ip = req.socket.remoteAddress || 'unknown';
    const now = Date.now();
    const rl  = parseRateLimit.get(ip) || { count: 0, resetAt: now + 3600000 };
    if (now > rl.resetAt) { rl.count = 0; rl.resetAt = now + 3600000; }
    if (rl.count >= 20) {
      res.writeHead(429, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Rate limit exceeded. Try again later.' }));
      return;
    }
    rl.count++;
    parseRateLimit.set(ip, rl);

    if (!ANTHROPIC_KEY) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'ANTHROPIC_API_KEY not configured on server' }));
      return;
    }
    let body = '';
    req.on('data', c => { body += c; if (body.length > 20 * 1024 * 1024) req.destroy(); });
    req.on('end', async () => {
      try {
        const { pdf } = JSON.parse(body); // base64 PDF from browser
        if (!pdf) { res.writeHead(400); res.end(JSON.stringify({ error: 'No PDF data' })); return; }

        // Trim to first page to save tokens
        const page1 = extractFirstPage(pdf);
        console.log('[parse-bl] PDF size: ' + Math.round(Buffer.from(pdf,'base64').length/1024) + 'KB → page1: ' + Math.round(Buffer.from(page1,'base64').length/1024) + 'KB');

        const prompt = `You are parsing a Bill of Lading. Extract ALL available fields and return ONLY valid JSON — no markdown, no explanation, no backticks.

Return this exact structure (use null for any field not found):
{
  "vessel": "vessel name in UPPERCASE",
  "carrier": "shipping line name",
  "mmsi": "9-digit MMSI if shown, else null",
  "bl_number": "bill of lading reference number",
  "container_type": "40' or 20' or LCL",
  "container_number": "container ID (e.g. MSCU1234567) if shown, else null",
  "origin": "port of loading, city and country code e.g. Yantian, CN",
  "dest": "port of discharge, city and country code e.g. Felixstowe, UK",
  "load_date": "YYYY-MM-DD estimated departure, null if not found",
  "eta_date": "YYYY-MM-DD ETA at discharge port, null if not found",
  "weight_kg": "gross weight as number only, null if not found",
  "pallets": "number of pallets or packages as integer, null if not found",
  "incoterms": "EXW/FCA/FOB/CFR/CIF/CPT/DAP if stated, else null",
  "hs_code": "HS/HTS tariff code for the goods if stated (digits only, e.g. 72041000), else null",
  "po_number": "customer purchase order or reference number — look for a 4-digit number starting with 6 or 7 in any reference, PO, customer ref, or shipper ref field. Return digits only, null if not found",
  "description": "brief cargo description"
}`;

        // Call Anthropic API server-side
        const anthropicRes = await new Promise((resolve, reject) => {
          const payload = JSON.stringify({
            model: 'claude-sonnet-4-20250514',
            max_tokens: 1000,
            messages: [{
              role: 'user',
              content: [
                { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: page1 } },
                { type: 'text', text: prompt }
              ]
            }]
          });
          const req2 = https.request('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
              'x-api-key':         ANTHROPIC_KEY,
              'anthropic-version': '2023-06-01',
              'Content-Type':      'application/json',
              'Content-Length':    Buffer.byteLength(payload),
            }
          }, (r) => {
            let d = '';
            r.on('data', c => d += c);
            r.on('end', () => resolve({ status: r.statusCode, body: d }));
          });
          req2.on('error', reject);
          req2.write(payload);
          req2.end();
        });

        if (anthropicRes.status !== 200) {
          console.error('[parse-bl] Anthropic error:', anthropicRes.body.slice(0, 200));
          res.writeHead(502, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Claude API error ' + anthropicRes.status }));
          return;
        }

        const apiData = JSON.parse(anthropicRes.body);
        const raw     = apiData.content && apiData.content[0] && apiData.content[0].text;
        const clean   = (raw || '').replace(/```json|```/g, '').trim();
        const parsed  = JSON.parse(clean);

        console.log('[parse-bl] Success — vessel:', parsed.vessel, 'bl:', parsed.bl_number);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, data: parsed }));

      } catch(e) {
        console.error('[parse-bl] Error:', e.message, e.stack);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Failed to parse document. Please try again.' }));
      }
    });
    return;
  }

  // Serve dashboard at root
  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    serveDashboard(req, res); return;
  }

  res.writeHead(404); res.end('Not found');
});

// ── WEBSOCKET SERVER ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

// Per-IP connection tracking — prevents a single client flooding the proxy
const wsConnsByIp = new Map();
const WS_MAX_PER_IP = 3;

wss.on('connection', (browserSocket, req) => {
  const ip = req.headers['x-forwarded-for']?.split(',')[0].trim()
           || req.socket.remoteAddress || 'unknown';

  // Enforce per-IP limit
  const ipCount = (wsConnsByIp.get(ip) || 0) + 1;
  if (ipCount > WS_MAX_PER_IP) {
    console.warn('[ws] Rejected connection from ' + ip + ' — limit of ' + WS_MAX_PER_IP + ' reached');
    browserSocket.close(1008, 'Too many connections from this IP');
    return;
  }
  wsConnsByIp.set(ip, ipCount);
  connectedCount++;
  console.log(`[ws] Browser connected from ${ip} (${connectedCount} total, ${ipCount} from this IP)`);
  if (!aisSocket || aisSocket.readyState !== WebSocket.OPEN) connectToAIS();

  browserSocket.on('message', data => {
    // Extract API key and MMSIs from the browser's subscription message.
    // The PROXY manages the aisstream subscription directly — we do NOT
    // forward the browser's subscription to aisstream, because the proxy's
    // persistent connection is the authoritative subscriber.
    // This prevents the browser sending a stale/incomplete MMSI list that
    // overwrites the proxy's up-to-date subscription.
    try {
      const sub = JSON.parse(data.toString());
      // Validate proxy secret in WebSocket messages
      const PROXY_SECRET = process.env.PROXY_SECRET || '';
      if (PROXY_SECRET && !safeEqual(sub._secret, PROXY_SECRET)) {
        console.log('[ws] Rejected message — invalid secret');
        return;
      }
      // Server-managed mode: proxy has AIS_API_KEY — ignore browser's APIKey
      // Browser-managed mode: no server key — accept APIKey from browser
      if (!process.env.AIS_API_KEY && sub.APIKey && sub.APIKey !== 'server') {
        if (sub.APIKey !== apiKey) {
          apiKey = sub.APIKey;
          console.log('[ws] Accepted APIKey from browser (no server-side key)');
        }
      }
      // Always accept MMSI updates from browser (new shipments added while browsing)
      if (sub.FiltersShipMMSI?.length) {
        const validated = sub.FiltersShipMMSI
          .map(m => String(m).trim())
          .filter(m => /^\d{7,9}$/.test(m));
        const sorted  = validated.slice().sort();
        const current = trackedMmsis.slice().sort();
        if (JSON.stringify(sorted) !== JSON.stringify(current)) {
          trackedMmsis = validated;
          saveConfig();
          console.log('[ws] Updated MMSIs from browser: ' + trackedMmsis.join(', '));
          // Reconnect AIS with new MMSI list
          if (apiKey && aisSocket) { aisSocket.close(); aisSocket = null; }
        }
      }
      // Ensure AIS is running
      if (apiKey && (!aisSocket || aisSocket.readyState !== WebSocket.OPEN)) {
        connectToAIS();
      }
    } catch (e) {}
    // Do NOT forward subscription to aisstream — proxy manages that directly.
    // Handle browser-pushed fallback position data
    try {
      const msg = JSON.parse(data.toString());
      if (msg._type === 'fallback_position') {
        handleBrowserFallback(msg);
      }
    } catch(e) {}
  });

  browserSocket.on('close', () => {
    connectedCount = Math.max(0, connectedCount - 1);
    const remaining = Math.max(0, (wsConnsByIp.get(ip) || 1) - 1);
    if (remaining === 0) wsConnsByIp.delete(ip);
    else wsConnsByIp.set(ip, remaining);
    console.log(`[ws] Browser disconnected (${connectedCount} total)`);
  });

  browserSocket.on('error', err => console.error('[ws] Error:', err.message));
});


// ── MMSI BOOTSTRAP FROM SUPABASE ──────────────────────────────────────────────
// Loads tracked MMSIs directly from shipments table so the proxy can start
// tracking immediately on boot, without waiting for a browser connection.
async function bootstrapMmsis() {
  try {
    const res = await supabase('GET', 'shipments', null, 'select=data');
    if (!res.ok || !Array.isArray(res.data)) return;
    const mmsis = res.data
      .map(row => row.data && row.data.mmsi)
      .filter(m => m && /^\d{7,9}$/.test(String(m)));
    const unique = [...new Set(mmsis.map(String))];
    if (unique.length > 0) {
      trackedMmsis = unique;
      saveConfig();
      console.log('[bootstrap] Loaded ' + unique.length + ' MMSIs from Supabase: ' + unique.join(', '));
    } else {
      console.log('[bootstrap] No MMSIs found in Supabase shipments');
    }
  } catch(e) {
    console.error('[bootstrap] Error loading MMSIs:', e.message);
  }
}

// Seed firstEtaSeen from voyage_events so ETA drift detection survives restarts.
// For each mmsi+dest pair, the earliest event with an ETA is the baseline.
async function seedFirstEtaSeen() {
  try {
    const res = await supabase('GET', 'voyage_events', null,
      'select=mmsi,destination,eta,timestamp&eta=not.is.null&order=timestamp.asc'
    );
    if (!res.ok || !Array.isArray(res.data)) return;
    let count = 0;
    for (const row of res.data) {
      if (!row.mmsi || !row.destination || !row.eta) continue;
      // Skip ATA/ATD entries
      if (row.destination.startsWith('ATA:') || row.destination.startsWith('ATD:')) continue;
      const key = String(row.mmsi) + '_' + row.destination;
      // Only set if not already present — earliest row wins (order=timestamp.asc)
      if (!firstEtaSeen[key]) {
        firstEtaSeen[key] = row.eta;
        count++;
      }
    }
    if (count > 0) console.log('[bootstrap] Seeded ' + count + ' ETA baselines from voyage_events');
  } catch(e) {
    console.error('[bootstrap] Error seeding ETA baselines:', e.message);
  }
}

// ── START ─────────────────────────────────────────────────────────────────────
loadConfig();
httpServer.listen(PROXY_PORT, async () => {
  console.log('\n=== Maritime Sentinel Proxy ===');
  console.log('    Port: ' + PROXY_PORT);
  console.log('    AIS_API_KEY:          ' + (process.env.AIS_API_KEY          ? 'SET' : 'NOT SET'));
  console.log('    SUPABASE_SERVICE_KEY: ' + (process.env.SUPABASE_SERVICE_KEY ? 'SET' : 'NOT SET'));
  console.log('    PROXY_SECRET:         ' + (process.env.PROXY_SECRET         ? 'SET' : 'NOT SET'));
  console.log('    ANTHROPIC_API_KEY:    ' + (process.env.ANTHROPIC_API_KEY    ? 'SET' : 'NOT SET'));
  console.log('    RESEND_API_KEY:       ' + (process.env.RESEND_API_KEY       ? 'SET' : 'NOT SET'));
  console.log('    NOTIFY_EMAIL:         ' + (process.env.NOTIFY_EMAIL         ? process.env.NOTIFY_EMAIL : 'NOT SET'));
  console.log('');

  // Always try to load MMSIs from Supabase on startup
  await bootstrapMmsis();

  // Restore ETA drift baselines so notifications survive restarts
  await seedFirstEtaSeen();

  if (apiKey) {
    if (trackedMmsis.length > 0) {
      console.log('[start] Auto-connecting to AIS with ' + trackedMmsis.length + ' vessels...');
      connectToAIS();
    } else {
      console.log('[start] AIS key set but no vessels to track yet — waiting for first browser connection');
    }
  } else {
    console.log('[start] No AIS_API_KEY set — waiting for browser to provide key');
  }
});

// Re-bootstrap MMSIs from Supabase every 5 minutes to pick up new shipments
// added while the proxy is running (without needing a browser connection)
setInterval(async () => {
  const prevCount = trackedMmsis.length;
  await bootstrapMmsis();
  if (trackedMmsis.length !== prevCount) {
    console.log('[interval] MMSI list changed — reconnecting AIS');
    if (aisSocket) { aisSocket.close(); aisSocket = null; }
    clearTimeout(reconnectTimer);
    if (apiKey) setTimeout(connectToAIS, 1000);
  }

  // Check 14-day proximity alerts against all active shipments
  try {
    const res = await supabase('GET', 'shipments', null, 'select=id,data');
    if (res.ok && Array.isArray(res.data)) {
      await check14DayAlerts(res.data);
    }
  } catch(e) { console.error('[14day] check error:', e.message); }
}, 5 * 60 * 1000);

// ── NOTIFICATION SYSTEM ───────────────────────────────────────────────────────
// Sends email via Resend API when tracked events occur.
// Preferences are loaded from Supabase notification_prefs table.
// Dedup via notifications_sent table — prevents repeat alerts within a window.

const RESEND_API_KEY  = process.env.RESEND_API_KEY  || '';
const NOTIFY_EMAIL    = process.env.NOTIFY_EMAIL    || '';
const NOTIFY_FROM     = process.env.NOTIFY_FROM     || 'Maritime Sentinel <onboarding@resend.dev>';

// In-memory prefs cache — refreshed every 10 minutes
let notifPrefs = null;
let notifPrefsLoadedAt = 0;

async function loadNotifPrefs() {
  // Return cached prefs if fresh
  if (notifPrefs && Date.now() - notifPrefsLoadedAt < 600000) return notifPrefs;
  try {
    const res = await supabase('GET', 'notification_prefs', null, 'limit=1');
    if (res.ok && res.data && res.data.length > 0) {
      notifPrefs = res.data[0];
      notifPrefsLoadedAt = Date.now();
      return notifPrefs;
    }
  } catch(e) {}
  // Defaults if table is empty or missing
  return {
    notify_eta_change:  true,
    notify_arrival:     true,
    notify_departure:   true,
    notify_delay:       true,
    notify_14day:       true,
    eta_change_threshold_days: 1,
  };
}

// Returns true if this event was already notified within windowHours
async function alreadyNotified(shipmentId, eventType, windowHours = 24) {
  const since = new Date(Date.now() - windowHours * 3600000).toISOString();
  try {
    const res = await supabase('GET', 'notifications_sent', null,
      'shipment_id=eq.' + encodeURIComponent(shipmentId) +
      '&event_type=eq.' + encodeURIComponent(eventType) +
      '&sent_at=gte.' + since +
      '&limit=1'
    );
    return res.ok && res.data && res.data.length > 0;
  } catch(e) { return false; }
}

async function markNotified(shipmentId, mmsi, eventType) {
  try {
    await supabase('POST', 'notifications_sent', {
      shipment_id: shipmentId,
      mmsi:        String(mmsi),
      event_type:  eventType,
      sent_at:     new Date().toISOString(),
    });
  } catch(e) {}
}

// Core send function — calls Resend REST API directly (no SDK needed)
async function sendEmail(subject, html) {
  if (!RESEND_API_KEY || !NOTIFY_EMAIL) {
    console.log('[notify] Skipping email — RESEND_API_KEY or NOTIFY_EMAIL not set');
    return false;
  }
  try {
    const body = JSON.stringify({ from: NOTIFY_FROM, to: [NOTIFY_EMAIL], subject, html });
    return new Promise((resolve) => {
      const req = https.request('https://api.resend.com/emails', {
        method: 'POST',
        headers: {
          'Authorization': 'Bearer ' + RESEND_API_KEY,
          'Content-Type':  'application/json',
          'Content-Length': Buffer.byteLength(body),
        },
      }, (res) => {
        let d = '';
        res.on('data', c => d += c);
        res.on('end', () => {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            console.log('[notify] Email sent: ' + subject);
            resolve(true);
          } else {
            console.error('[notify] Resend error ' + res.statusCode + ':', d.slice(0, 200));
            resolve(false);
          }
        });
      });
      req.on('error', e => { console.error('[notify] Email request error:', e.message); resolve(false); });
      req.write(body);
      req.end();
    });
  } catch(e) {
    console.error('[notify] sendEmail error:', e.message);
    return false;
  }
}

// HTML email template — clean, minimal, mobile-friendly
function emailHtml(title, rows, footnote) {
  const rowsHtml = rows.map(([label, value]) =>
    `<tr><td style="padding:8px 0;color:#64748b;font-size:13px;width:140px;vertical-align:top;">${label}</td>` +
    `<td style="padding:8px 0;color:#0f172a;font-size:13px;font-weight:600;">${value}</td></tr>`
  ).join('');
  return `<!DOCTYPE html><html><body style="margin:0;padding:0;background:#f8fafc;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<div style="max-width:520px;margin:40px auto;background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.1);">
  <div style="background:#001736;padding:24px 32px;">
    <div style="color:#93c5fd;font-size:11px;font-weight:700;letter-spacing:0.15em;text-transform:uppercase;margin-bottom:6px;">Maritime Sentinel</div>
    <div style="color:#ffffff;font-size:20px;font-weight:700;">${title}</div>
  </div>
  <div style="padding:28px 32px;">
    <table style="width:100%;border-collapse:collapse;">${rowsHtml}</table>
    ${footnote ? `<div style="margin-top:20px;padding-top:16px;border-top:1px solid #e2e8f0;color:#94a3b8;font-size:12px;">${footnote}</div>` : ''}
  </div>
  <div style="background:#f8fafc;padding:16px 32px;text-align:center;color:#94a3b8;font-size:11px;">
    Maritime Sentinel · <a href="https://ms123-production.up.railway.app" style="color:#3b82f6;text-decoration:none;">Open Dashboard</a>
  </div>
</div></body></html>`;
}

// Look up the shipment record for a given MMSI (from Supabase)
// Returns { id, vessel, origin, dest, arriveDate } or null
async function getShipmentForMmsi(mmsi) {
  try {
    const res = await supabase('GET', 'shipments', null,
      'select=id,data&order=data->>id.asc'
    );
    if (!res.ok || !Array.isArray(res.data)) return null;
    const row = res.data.find(r => r.data && String(r.data.mmsi) === String(mmsi)
                                           && !r.data.completedAt);
    return row ? { id: row.id, ...row.data } : null;
  } catch(e) { return null; }
}

// ── NOTIFICATION TRIGGERS ─────────────────────────────────────────────────────

async function notifyArrival(mmsi, portName, ataStr, isFinal) {
  const prefs = await loadNotifPrefs();
  if (!prefs.notify_arrival) return;
  const s = await getShipmentForMmsi(mmsi);
  if (!s) return;
  const eventType = isFinal ? 'arrival_final' : 'arrival_' + portName.slice(0,8).toUpperCase();
  if (await alreadyNotified(s.id, eventType, 12)) return;

  const title  = isFinal ? '🏁 Arrived at Final Destination' : '⚓ Vessel Arrived at Port';
  const ok = await sendEmail(
    (isFinal ? '[Final Arrival] ' : '[Port Arrival] ') + s.id + ' — ' + s.vessel,
    emailHtml(title, [
      ['Shipment',    s.id],
      ['Vessel',      s.vessel],
      ['Route',       s.origin + ' → ' + s.dest],
      ['Port',        portName],
      ['Arrived',     ataStr],
      isFinal ? ['Status', 'Final discharge port reached'] : ['Type', 'Intermediate port stop'],
    ], isFinal ? 'Your cargo has reached its final destination.' : 'Vessel is stopping at an intermediate port before continuing to ' + s.dest + '.')
  );
  if (ok) await markNotified(s.id, mmsi, eventType);
}

async function notifyDeparture(mmsi, portName, atdStr) {
  const prefs = await loadNotifPrefs();
  if (!prefs.notify_departure) return;
  const s = await getShipmentForMmsi(mmsi);
  if (!s) return;
  const eventType = 'departure_' + portName.slice(0,8).toUpperCase();
  if (await alreadyNotified(s.id, eventType, 12)) return;

  const ok = await sendEmail(
    '[Departed] ' + s.id + ' — ' + s.vessel + ' left ' + portName,
    emailHtml('🚢 Vessel Departed', [
      ['Shipment',   s.id],
      ['Vessel',     s.vessel],
      ['Route',      s.origin + ' → ' + s.dest],
      ['Departed',   portName],
      ['Time',       atdStr],
    ], 'Vessel is now underway. Next stop: ' + s.dest + '.')
  );
  if (ok) await markNotified(s.id, mmsi, eventType);
}

async function notifyDelay(mmsi, portName, ataStr, etaStr, delayDays) {
  const prefs = await loadNotifPrefs();
  if (!prefs.notify_delay) return;
  if (!delayDays || delayDays <= 0) return;
  const s = await getShipmentForMmsi(mmsi);
  if (!s) return;
  const eventType = 'delay_' + portName.slice(0,8).toUpperCase();
  if (await alreadyNotified(s.id, eventType, 24)) return;

  const ok = await sendEmail(
    '[Delay] ' + s.id + ' — ' + s.vessel + ' arrived ' + delayDays + 'd late at ' + portName,
    emailHtml('⚠️ Arrival Delay Detected', [
      ['Shipment',    s.id],
      ['Vessel',      s.vessel],
      ['Port',        portName],
      ['ETA was',     etaStr || '—'],
      ['Arrived',     ataStr],
      ['Delay',       '+' + delayDays + ' day' + (delayDays !== 1 ? 's' : '')],
    ], 'This delay may affect your final ETA to ' + s.dest + '. Check the dashboard for an updated estimate.')
  );
  if (ok) await markNotified(s.id, mmsi, eventType);
}

async function notifyEtaChange(mmsi, dest, prevEtaStr, newEtaStr, driftDays) {
  const prefs = await loadNotifPrefs();
  if (!prefs.notify_eta_change) return;
  const threshold = prefs.eta_change_threshold_days || 1;
  if (Math.abs(driftDays) < threshold) return;
  const s = await getShipmentForMmsi(mmsi);
  if (!s) return;
  // Dedup window: 6h — ETA can wobble repeatedly in a short period
  const eventType = 'eta_change_' + dest.slice(0,6);
  if (await alreadyNotified(s.id, eventType, 6)) return;

  const later   = driftDays > 0;
  const driftStr = (later ? '+' : '') + driftDays + ' day' + (Math.abs(driftDays) !== 1 ? 's' : '');
  const ok = await sendEmail(
    '[ETA Change] ' + s.id + ' — ' + s.vessel + ' ' + (later ? 'delayed' : 'ahead') + ' ' + driftStr,
    emailHtml(later ? '📅 ETA Pushed Back' : '📅 ETA Moved Forward', [
      ['Shipment',   s.id],
      ['Vessel',     s.vessel],
      ['Port',       dest],
      ['Previous ETA', prevEtaStr || '—'],
      ['New ETA',    newEtaStr],
      ['Change',     driftStr],
    ], later
      ? 'The vessel is running behind schedule. Your final ETA to ' + s.dest + ' may be affected.'
      : 'The vessel is running ahead of schedule.')
  );
  if (ok) await markNotified(s.id, mmsi, eventType);
}

async function notifyApproaching14Days(mmsi, s, etaDate) {
  const prefs = await loadNotifPrefs();
  if (!prefs.notify_14day) return;
  // Dedup: once per shipment per voyage (48h window is generous — re-check tomorrow)
  if (await alreadyNotified(s.id, '14day_warning', 48)) return;

  const daysAway = Math.round((etaDate - Date.now()) / 86400000);
  const ok = await sendEmail(
    '[14 Days] ' + s.id + ' — ' + s.vessel + ' arrives in ~' + daysAway + ' days',
    emailHtml('📦 Shipment Arriving Soon', [
      ['Shipment',     s.id],
      ['Vessel',       s.vessel],
      ['Destination',  s.dest],
      ['ETA',          etaDate.toLocaleDateString('en-GB', {day:'2-digit',month:'short',year:'numeric'})],
      ['Days away',    '~' + daysAway + ' days'],
    ], 'Time to prepare for receipt — arrange customs clearance, warehouse space, and delivery logistics.')
  );
  if (ok) await markNotified(s.id, mmsi, '14day_warning');
}

// ── 14-DAY PROXIMITY CHECK (runs on bootstrap interval) ──────────────────────
async function check14DayAlerts(shipmentRows) {
  const prefs = await loadNotifPrefs();
  if (!prefs.notify_14day || !RESEND_API_KEY || !NOTIFY_EMAIL) return;

  const now = Date.now();
  for (const row of shipmentRows) {
    const s = row.data;
    if (!s || s.completedAt || !s.mmsi) continue;

    // Determine best ETA to final destination
    // Priority: AIS ETA to final port > revisedEta > arriveDate
    let etaDate = null;

    // Try lastEtaByDest for the final destination key
    if (s.lastEtaByDest && s.dest) {
      const destKey = s.dest.toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,5);
      const entry = Object.entries(s.lastEtaByDest || {}).find(([k]) =>
        k.replace(/[^A-Z0-9]/g,'').slice(0,5) === destKey
      );
      if (entry) {
        const d = new Date(entry[1].replace('ETA: ',''));
        if (!isNaN(d)) etaDate = d;
      }
    }
    // Fall back to revisedEta or arriveDate
    if (!etaDate && s.revisedEta) {
      const d = new Date(s.revisedEta.replace(/(\d{2}) (\w{3}) (\d{4})/, '$2 $1 $3'));
      if (!isNaN(d)) etaDate = d;
    }
    if (!etaDate && s.arriveDate) {
      const d = new Date(s.arriveDate);
      if (!isNaN(d)) etaDate = d;
    }
    if (!etaDate) continue;

    const daysAway = (etaDate - now) / 86400000;
    // Trigger if between 13.5 and 14.5 days away (±12h window around the 14-day mark)
    if (daysAway >= 13.5 && daysAway <= 14.5) {
      await notifyApproaching14Days(s.mmsi, s, etaDate);
    }
  }
}

// ── ETA DRIFT TRACKING ────────────────────────────────────────────────────────
// Track first ETA seen per mmsi+dest to detect significant drift.
// Stored in memory (resets on redeploy — acceptable, drift builds up over days).
const firstEtaSeen = {}; // key: mmsi+'_'+dest → ISO string

async function checkEtaDrift(mmsi, dest, newEtaStr) {
  const key = mmsi + '_' + dest;
  if (!firstEtaSeen[key]) {
    firstEtaSeen[key] = newEtaStr;
    return; // First time seeing ETA for this dest — establish baseline
  }
  const prev = firstEtaSeen[key];
  if (prev === newEtaStr) return; // No change

  // Parse both dates
  const parseEta = str => {
    if (!str) return null;
    // Handles "YYYY-MM-DD HH:MM UTC" and "ETA: MM/DD HH:MM UTC"
    const iso = str.replace('ETA: ','').replace(' UTC','').trim();
    const mmdd = iso.match(/^(\d{2})\/(\d{2})\s+(\d{2}):(\d{2})$/);
    if (mmdd) {
      const yr = new Date().getFullYear();
      return new Date(yr + '-' + mmdd[1] + '-' + mmdd[2] + 'T' + mmdd[3] + ':' + mmdd[4] + ':00Z');
    }
    return new Date(iso);
  };

  const prevDate = parseEta(prev);
  const newDate  = parseEta(newEtaStr);
  if (!prevDate || !newDate || isNaN(prevDate) || isNaN(newDate)) return;

  const driftDays = Math.round((newDate - prevDate) / 86400000);
  if (driftDays === 0) return;

  // Update baseline to new ETA so we don't re-alert on the same drift
  firstEtaSeen[key] = newEtaStr;
  await notifyEtaChange(mmsi, dest, prev, newEtaStr, driftDays);
}
