import { createServer as createHttpServer } from 'node:http';
import { createServer as createHttpsServer } from 'node:https';
import { createHmac, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { WebSocket, WebSocketServer } from 'ws';

const PORT = Number(process.env.SIGNALING_PORT || 8001);
const DISCONNECT_GRACE_MS = Number(process.env.SIGNALING_DISCONNECT_GRACE_MS || 15_000);
const TURN_URLS = (process.env.TURN_URLS || '')
  .split(',')
  .map((url) => url.trim())
  .filter(Boolean);
const TURN_SHARED_SECRET = process.env.TURN_SHARED_SECRET || '';
const TURN_CREDENTIAL_TTL_SECONDS = Number(process.env.TURN_CREDENTIAL_TTL_SECONDS || 3600);
if (Boolean(TURN_URLS.length) !== Boolean(TURN_SHARED_SECRET)) {
  throw new Error('Set both TURN_URLS and TURN_SHARED_SECRET to enable TURN relay credentials.');
}
if (!Number.isSafeInteger(TURN_CREDENTIAL_TTL_SECONDS) || TURN_CREDENTIAL_TTL_SECONDS < 60) {
  throw new Error('TURN_CREDENTIAL_TTL_SECONDS must be an integer of at least 60.');
}
if (TURN_URLS.some((url) => !/^turns?:/i.test(url))) {
  throw new Error('TURN_URLS must contain comma-separated turn: or turns: URLs.');
}
if (!Number.isSafeInteger(DISCONNECT_GRACE_MS) || DISCONNECT_GRACE_MS < 0) {
  throw new Error('SIGNALING_DISCONNECT_GRACE_MS must be a non-negative integer.');
}
const HOSPITALS = new Map([
  ['H001', 'Hospital A'],
  ['H002', 'Hospital B'],
  ['H003', 'Hospital C'],
]);
const peers = new Map();
const tlsKeyPath = process.env.SIGNALING_TLS_KEY;
const tlsCertPath = process.env.SIGNALING_TLS_CERT;
if (Boolean(tlsKeyPath) !== Boolean(tlsCertPath)) {
  throw new Error('Set both SIGNALING_TLS_KEY and SIGNALING_TLS_CERT to enable secure WebSocket signaling.');
}
const requestHandler = (request, response) => {
  if (request.method === 'GET' && request.url === '/ice-config') {
    const iceServers = [
      { urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] },
    ];
    if (TURN_URLS.length) {
      const username = `${Math.floor(Date.now() / 1000) + TURN_CREDENTIAL_TTL_SECONDS}:${randomUUID()}`;
      const credential = createHmac('sha1', TURN_SHARED_SECRET)
        .update(username)
        .digest('base64');
      iceServers.push({ urls: TURN_URLS, username, credential });
    }
    response.writeHead(200, {
      'cache-control': 'no-store',
      'content-type': 'application/json',
    });
    response.end(JSON.stringify({ iceServers }));
    return;
  }
  if (request.url !== '/health') {
    response.writeHead(404).end();
    return;
  }
  response.writeHead(200, { 'content-type': 'application/json' });
  response.end(JSON.stringify({ status: 'ok', peers: peers.size }));
};
const server = tlsKeyPath
  ? createHttpsServer({
      key: readFileSync(tlsKeyPath),
      cert: readFileSync(tlsCertPath),
    }, requestHandler)
  : createHttpServer(requestHandler);
const webSocketServer = new WebSocketServer({ server, maxPayload: 64 * 1024 });

function send(socket, value) {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(value));
  }
}

function publicPeer(peer) {
  return {
    id: peer.id,
    role: peer.role,
    hospitalId: peer.hospitalId,
    targetHospitalId: peer.targetHospitalId,
    name: peer.name,
    connectedAt: peer.connectedAt,
  };
}

function broadcastPresence() {
  const roster = [...peers.values()].map(publicPeer);
  for (const peer of peers.values()) {
    send(peer.socket, { type: 'presence', peers: roster });
  }
}

function validRegistration(message) {
  if (message.role === 'hospital') {
    return HOSPITALS.has(message.hospitalId)
      && message.id === message.hospitalId
      && typeof message.name === 'string'
      && message.name.trim().length > 0
      && message.name.length <= 80;
  }
  return message.role === 'other'
    && /^OTHER-[0-9a-f-]{36}$/i.test(message.id)
    && typeof message.name === 'string'
    && message.name.trim().length > 0
    && message.name.length <= 80
    && HOSPITALS.has(message.targetHospitalId);
}

webSocketServer.on('connection', (socket) => {
  socket.isAlive = true;
  socket.peerId = null;
  socket.on('pong', () => {
    socket.isAlive = true;
  });

  socket.on('message', (data) => {
    let message;
    try {
      message = JSON.parse(data.toString());
    } catch {
      send(socket, { type: 'error', message: 'Signaling messages must be valid JSON.' });
      return;
    }

    if (!socket.peerId) {
      if (message.type !== 'register' || !validRegistration(message)) {
        send(socket, { type: 'error', message: 'Register a valid hospital or other-user identity first.' });
        socket.close(1008, 'Invalid registration');
        return;
      }
      const existing = peers.get(message.id);
      if (existing?.socket?.readyState === WebSocket.OPEN) {
        send(existing.socket, { type: 'replaced', message: 'This node connected from another browser session.' });
        existing.socket.close(4001, 'Node connected elsewhere');
      }
      if (existing?.disconnectTimer) clearTimeout(existing.disconnectTimer);
      const peer = existing || {
        id: message.id,
        role: message.role,
        hospitalId: message.role === 'hospital' ? message.hospitalId : null,
        targetHospitalId: message.role === 'other' ? message.targetHospitalId : null,
        name: message.name.trim(),
        connectedAt: new Date().toISOString(),
        socket,
      };
      Object.assign(peer, {
        role: message.role,
        hospitalId: message.role === 'hospital' ? message.hospitalId : null,
        targetHospitalId: message.role === 'other' ? message.targetHospitalId : null,
        name: message.name.trim(),
        connectedAt: new Date().toISOString(),
        socket,
        disconnectTimer: null,
      });
      peers.set(peer.id, peer);
      socket.peerId = peer.id;
      send(socket, { type: 'registered', peer: publicPeer(peer) });
      broadcastPresence();
      return;
    }

    if (message.type !== 'signal'
      || typeof message.to !== 'string'
      || !['offer', 'answer', 'candidate'].includes(message.signal?.type)) {
      send(socket, { type: 'error', message: 'Only SDP and ICE signaling messages may use this server.' });
      return;
    }
    const target = peers.get(message.to);
    if (!target) {
      send(socket, {
        type: 'signal-error',
        to: message.to,
        message: 'The requested peer is not connected to signaling.',
      });
      return;
    }
    send(target.socket, {
      type: 'signal',
      from: socket.peerId,
      signal: message.signal,
    });
  });

  socket.on('close', (code, reason) => {
    const peer = socket.peerId && peers.get(socket.peerId);
    if (peer?.socket === socket) {
      peer.socket = null;
      const explicitlyDisconnected = code === 1000 && reason.toString() === 'Node disconnected';
      if (explicitlyDisconnected || DISCONNECT_GRACE_MS === 0) {
        peers.delete(peer.id);
        broadcastPresence();
      } else {
        peer.disconnectTimer = setTimeout(() => {
          if (peers.get(peer.id) !== peer || peer.socket) return;
          peers.delete(peer.id);
          broadcastPresence();
        }, DISCONNECT_GRACE_MS);
      }
    }
  });

  socket.on('error', (error) => {
    console.error('WebSocket signaling error:', error.message);
  });
});

const heartbeat = setInterval(() => {
  for (const socket of webSocketServer.clients) {
    if (!socket.isAlive) {
      socket.terminate();
      continue;
    }
    socket.isAlive = false;
    socket.ping();
  }
}, 10_000);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`WebRTC signaling server listening on ws://0.0.0.0:${PORT}`);
  console.log('Hospital application messages are not relayed by this server.');
});

function shutdown() {
  clearInterval(heartbeat);
  for (const peer of peers.values()) {
    if (peer.disconnectTimer) clearTimeout(peer.disconnectTimer);
  }
  for (const socket of webSocketServer.clients) {
    socket.close(1001, 'Signaling server shutting down');
  }
  webSocketServer.close(() => server.close());
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
