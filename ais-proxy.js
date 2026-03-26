/**
 * Maritime Sentinel — AIS WebSocket Proxy
 * Bridges browser <-> aisstream.io (which blocks direct browser connections)
 *
 * Setup (one time):
 *   npm install ws
 *
 * Run:
 *   node ais-proxy.js
 *
 * Then open maritime-sentinel.html in your browser as normal.
 * The dashboard will connect to ws://localhost:8765 instead of aisstream directly.
 */

const { WebSocket, WebSocketServer } = require('ws');

const PROXY_PORT = process.env.PORT || 8765;
const AISSTREAM_URL = 'wss://stream.aisstream.io/v0/stream';

const wss = new WebSocketServer({ port: PROXY_PORT });

console.log(`\n🚢  Maritime Sentinel AIS Proxy`);
console.log(`    Listening on ws://localhost:${PROXY_PORT}`);
console.log(`    Forwarding to ${AISSTREAM_URL}\n`);

wss.on('connection', (browserSocket) => {
  console.log('[proxy] Browser connected');

  // Open upstream connection to aisstream
  const aisSocket = new WebSocket(AISSTREAM_URL);

  aisSocket.on('open', () => {
    console.log('[proxy] Connected to aisstream.io');
  });

  // Forward messages from aisstream to browser, always as UTF-8 string
  aisSocket.on('message', (data, isBinary) => {
    if (browserSocket.readyState === WebSocket.OPEN) {
      const str = isBinary ? data.toString('utf8') : data.toString();
      browserSocket.send(str);
    }
  });

  // Forward messages from browser → aisstream (subscription message)
  browserSocket.on('message', (data) => {
    console.log('[proxy] Sending subscription:', data.toString().slice(0, 120));
    if (aisSocket.readyState === WebSocket.OPEN) {
      aisSocket.send(data);
    } else {
      // Queue until aisstream is ready
      aisSocket.once('open', () => aisSocket.send(data));
    }
  });

  aisSocket.on('error', (err) => {
    console.error('[proxy] aisstream error:', err.message);
    browserSocket.close();
  });

  aisSocket.on('close', (code, reason) => {
    console.log(`[proxy] aisstream closed — code: ${code} reason: ${reason.toString() || 'none'}`);
    browserSocket.close();
  });

  browserSocket.on('close', () => {
    console.log('[proxy] Browser disconnected — closing upstream');
    aisSocket.close();
  });

  browserSocket.on('error', (err) => {
    console.error('[proxy] Browser socket error:', err.message);
    aisSocket.close();
  });
});
