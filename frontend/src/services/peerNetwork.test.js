import assert from 'node:assert/strict';
import test from 'node:test';
import { getIceServers, getSignalingUrl, PeerNetwork } from './peerNetwork.js';

const handlers = {};

test('ICE configuration loads from the same-origin signaling endpoint', async () => {
  const expected = [{ urls: ['stun:stun.example.test:3478'] }];
  const iceServers = await getIceServers(async (url) => {
    assert.equal(url, '/ice-config');
    return { ok: true, json: async () => ({ iceServers: expected }) };
  }, '');

  assert.deepEqual(iceServers, expected);
});

test('ICE configuration errors are not silently replaced with STUN-only defaults', async () => {
  await assert.rejects(
    getIceServers(async () => ({ ok: false, status: 503 }), ''),
    /HTTP 503/,
  );
  await assert.rejects(
    getIceServers(async () => ({ ok: true, json: async () => ({ iceServers: [] }) }), ''),
    /empty ICE server configuration/,
  );
});

test('signaling keeps the original local development endpoint and proxies deployed HTTPS', () => {
  assert.equal(getSignalingUrl({
    protocol: 'http:',
    hostname: '192.168.1.20',
    host: '192.168.1.20:5173',
  }, true), 'ws://192.168.1.20:8001');
  assert.equal(getSignalingUrl({
    protocol: 'https:',
    hostname: 'hospital.example.test',
    host: 'hospital.example.test',
  }, true), 'wss://hospital.example.test/ws');
  assert.equal(getSignalingUrl({
    protocol: 'http:',
    hostname: 'localhost',
    host: 'localhost:30080',
  }, false), 'ws://localhost:30080/ws');
});

test('referral desks can establish peer connections with any hospital selected by the coordinator', () => {
  const referralDesk = new PeerNetwork({
    id: 'OTHER-12345678-1234-1234-1234-123456789abc',
    role: 'other',
    targetHospitalId: 'H001',
  }, handlers);
  const hospitalA = new PeerNetwork({
    id: 'H001',
    role: 'hospital',
    hospitalId: 'H001',
  }, handlers);

  assert.equal(referralDesk.isRelevantPeer({ id: 'H001', role: 'hospital', hospitalId: 'H001' }), true);
  assert.equal(referralDesk.isRelevantPeer({ id: 'H003', role: 'hospital', hospitalId: 'H003' }), true);
  assert.equal(referralDesk.shouldInitiate({ id: 'H003', role: 'hospital' }), true);
  assert.equal(hospitalA.isRelevantPeer({
    id: 'OTHER-12345678-1234-1234-1234-123456789abc',
    role: 'other',
    targetHospitalId: 'H003',
  }), true);
  assert.equal(hospitalA.shouldInitiate({
    id: 'OTHER-12345678-1234-1234-1234-123456789abc',
    role: 'other',
  }), false);
});

test('call termination and acknowledgement are delivered over the peer data channel', () => {
  const callerId = 'OTHER-12345678-1234-1234-1234-123456789abc';
  const callId = 'call-to-end';
  const received = [];
  let caller;
  let hospital;

  caller = new PeerNetwork({ id: callerId, role: 'other' }, {
    ...handlers,
    onPeerMessage: (message) => {
      if (message.direction === 'received') received.push(message);
    },
  });
  hospital = new PeerNetwork({ id: 'H001', role: 'hospital', hospitalId: 'H001' }, {
    ...handlers,
    onPeerMessage: (message) => {
      if (message.direction === 'received' && message.type === 'CALL_END') {
        hospital.sendToPeer(callerId, 'CALL_END_ACK', { callId }, message.lamportTimestamp + 1);
      }
    },
  });
  caller.peers.set('H001', {
    channel: {
      readyState: 'open',
      send: (raw) => hospital.handlePeerMessage(callerId, raw),
    },
  });
  hospital.peers.set(callerId, {
    channel: {
      readyState: 'open',
      send: (raw) => caller.handlePeerMessage('H001', raw),
    },
  });

  caller.sendToPeer('H001', 'CALL_END', { callId }, 1);

  assert.deepEqual(received.map(({ type, payload }) => ({ type, payload })), [
    { type: 'CALL_END_ACK', payload: { callId } },
  ]);
});

test('a transient data channel error is recorded as a connection event, not a user-facing failure', () => {
  const localEvents = [];
  const errors = [];
  const network = new PeerNetwork({ id: 'H001', role: 'hospital', hospitalId: 'H001' }, {
    onLocalEvent: (type, text) => localEvents.push({ type, text }),
    onError: (message) => errors.push(message),
  });
  const peer = { id: 'H003' };
  const channel = { readyState: 'open' };

  network.configureDataChannel(peer, channel);
  channel.onerror();

  assert.equal(errors.length, 0);
  assert.equal(localEvents[0].type, 'DATACHANNEL_ERROR');
  assert.match(localEvents[0].text, /checking connection state/);
});
