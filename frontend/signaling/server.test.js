import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createServer as createNetServer } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import { WebSocket } from 'ws';

async function availablePort() {
  const server = createNetServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
  return port;
}

function waitForMessage(socket, predicate, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      socket.off('message', onMessage);
      reject(new Error('Timed out waiting for signaling test message.'));
    }, timeoutMs);
    const onMessage = (raw) => {
      const message = JSON.parse(raw.toString());
      if (!predicate(message)) return;
      clearTimeout(timeout);
      socket.off('message', onMessage);
      resolve(message);
    };
    socket.on('message', onMessage);
  });
}

test('signaling relays SDP/ICE but rejects application messages', async () => {
  const port = await availablePort();
  const turnSecret = 'unit-test-turn-rest-secret';
  const serverProcess = spawn(process.execPath, ['signaling/server.js'], {
    cwd: new URL('..', import.meta.url),
    env: {
      ...process.env,
      SIGNALING_PORT: String(port),
      SIGNALING_DISCONNECT_GRACE_MS: '200',
      TURN_URLS: 'turn:turn.example.test:3478?transport=udp,turns:turn.example.test:5349?transport=tcp',
      TURN_SHARED_SECRET: turnSecret,
      TURN_CREDENTIAL_TTL_SECONDS: '1800',
    },
    stdio: 'ignore',
  });
  let first;
  let second;
  try {
    const healthUrl = `http://127.0.0.1:${port}/health`;
    let started = false;
    for (let attempt = 0; attempt < 30; attempt += 1) {
      try {
        const response = await fetch(healthUrl);
        if (response.ok) {
          started = true;
          break;
        }
      } catch {
        await delay(100);
      }
    }
    assert.equal(started, true, 'signaling server should become healthy');

    const iceResponse = await fetch(`http://127.0.0.1:${port}/ice-config`);
    assert.equal(iceResponse.headers.get('cache-control'), 'no-store');
    const { iceServers } = await iceResponse.json();
    assert.equal(iceServers.length, 2);
    assert.deepEqual(iceServers[1].urls, [
      'turn:turn.example.test:3478?transport=udp',
      'turns:turn.example.test:5349?transport=tcp',
    ]);
    const expiresAt = Number(iceServers[1].username.split(':', 1)[0]);
    assert.ok(expiresAt >= Math.floor(Date.now() / 1000) + 1799);
    assert.equal(
      iceServers[1].credential,
      createHmac('sha1', turnSecret).update(iceServers[1].username).digest('base64'),
    );

    first = new WebSocket(`ws://127.0.0.1:${port}`);
    second = new WebSocket(`ws://127.0.0.1:${port}`);
    await Promise.all([once(first, 'open'), once(second, 'open')]);
    const firstRegistered = waitForMessage(first, (message) => message.type === 'registered');
    first.send(JSON.stringify({
      type: 'register',
      id: 'H001',
      role: 'hospital',
      hospitalId: 'H001',
      name: 'Test A',
    }));
    await firstRegistered;
    const secondRegistered = waitForMessage(second, (message) => message.type === 'registered');
    second.send(JSON.stringify({
      type: 'register',
      id: 'H002',
      role: 'hospital',
      hospitalId: 'H002',
      name: 'Test B',
    }));
    await secondRegistered;
    const relayedSdp = waitForMessage(second, (message) => message.type === 'signal');
    first.send(JSON.stringify({
      type: 'signal',
      to: 'H002',
      signal: {
        type: 'offer',
        description: { type: 'offer', sdp: 'test-offer' },
      },
    }));
    const offer = await relayedSdp;
    assert.equal(offer.signal.description.sdp, 'test-offer');

    const rejectedApplicationMessage = waitForMessage(first, (message) => message.type === 'error');
    first.send(JSON.stringify({
      type: 'application',
      to: 'H002',
      payload: { type: 'HEARTBEAT' },
    }));
    assert.match((await rejectedApplicationMessage).message, /Only SDP and ICE/);

    first.terminate();
    await once(first, 'close');
    await delay(50);
    assert.equal((await fetch(healthUrl)).ok, true, 'signaling server should survive an offline peer');
    const reconnectedPresence = waitForMessage(
      second,
      (message) => message.type === 'presence'
        && message.peers.some((peer) => peer.id === 'H001'),
    );
    first = new WebSocket(`ws://127.0.0.1:${port}`);
    await once(first, 'open');
    const reconnectedRegistration = waitForMessage(first, (message) => message.type === 'registered');
    first.send(JSON.stringify({
      type: 'register',
      id: 'H001',
      role: 'hospital',
      hospitalId: 'H001',
      name: 'Test A',
    }));
    await reconnectedRegistration;
    assert.ok((await reconnectedPresence).peers.some((peer) => peer.id === 'H001'));
  } finally {
    first?.close();
    second?.close();
    serverProcess.kill('SIGTERM');
    await Promise.race([once(serverProcess, 'exit'), delay(1000)]);
  }
});
