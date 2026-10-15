import { createUuid } from './uuid.js';

const HOSPITAL_IDS = new Set(['H001', 'H002', 'H003']);
const MAX_RECENT_MESSAGES = 2048;
const MAX_ICE_CANDIDATES = 64;

export async function getIceServers(fetchConfig = fetch, configured = import.meta.env?.VITE_ICE_SERVERS) {
  if (configured) {
    const iceServers = JSON.parse(configured);
    if (!Array.isArray(iceServers) || iceServers.length === 0) {
      throw new Error('VITE_ICE_SERVERS must be a non-empty JSON array.');
    }
    return iceServers;
  }
  const response = await fetchConfig('/ice-config');
  if (!response.ok) {
    throw new Error(`ICE server configuration request failed (HTTP ${response.status}).`);
  }
  const { iceServers } = await response.json();
  if (!Array.isArray(iceServers) || iceServers.length === 0) {
    throw new Error('The signaling service returned an empty ICE server configuration.');
  }
  return iceServers;
}

export function getSignalingUrl(location = window.location, isDevelopment = import.meta.env?.DEV) {
  if (import.meta.env?.VITE_SIGNALING_URL) {
    return import.meta.env.VITE_SIGNALING_URL;
  }
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  if (isDevelopment && protocol === 'ws:') {
    return `${protocol}//${location.hostname}:8001`;
  }
  return `${protocol}//${location.host}/ws`;
}

export class PeerNetwork {
  constructor(identity, handlers) {
    this.identity = identity;
    this.handlers = handlers;
    this.socket = null;
    this.peers = new Map();
    this.roster = [];
    this.localStream = null;
    this.shouldReconnect = false;
    this.reconnectDelay = 500;
    this.reconnectTimer = null;
    this.seenMessages = new Set();
    this.iceServers = [];
  }

  async connect() {
    if (this.shouldReconnect) return;
    this.shouldReconnect = true;
    try {
      this.iceServers = await getIceServers();
    } catch (error) {
      this.shouldReconnect = false;
      this.handlers.onError(`Could not load WebRTC ICE server configuration: ${error.message}`);
      return;
    }
    if (!this.shouldReconnect) return;
    this.openSignalingSocket();
  }

  openSignalingSocket() {
    if (!this.shouldReconnect) return;
    const socket = new WebSocket(getSignalingUrl());
    this.socket = socket;
    this.handlers.onSignalingState('connecting');
    this.handlers.onLocalEvent('SIGNALING_CONNECTING', 'Connecting to the WebSocket signaling service.');

    socket.addEventListener('open', () => {
      if (this.socket !== socket || !this.shouldReconnect) return;
      this.reconnectDelay = 500;
      socket.send(JSON.stringify({
        type: 'register',
        id: this.identity.id,
        role: this.identity.role,
        hospitalId: this.identity.hospitalId,
        targetHospitalId: this.identity.targetHospitalId,
        name: this.identity.name,
      }));
    });

    socket.addEventListener('message', (event) => {
      if (this.socket !== socket) return;
      this.handleSignalingMessage(event.data);
    });

    socket.addEventListener('close', (event) => {
      if (this.socket !== socket) return;
      this.socket = null;
      this.handlers.onSignalingState('disconnected');
      this.handlers.onLocalEvent('SIGNALING_DISCONNECTED', `Signaling disconnected (${event.code}).`);
      if (this.shouldReconnect) {
        const delay = this.reconnectDelay;
        this.reconnectDelay = Math.min(this.reconnectDelay * 2, 10_000);
        this.reconnectTimer = window.setTimeout(() => this.openSignalingSocket(), delay);
      }
    });

    socket.addEventListener('error', () => {
      this.handlers.onError('Could not connect to the WebSocket signaling server.');
    });
  }

  handleSignalingMessage(rawMessage) {
    let message;
    try {
      message = JSON.parse(rawMessage);
    } catch {
      this.handlers.onError('The signaling server sent an invalid message.');
      return;
    }

    if (message.type === 'registered') {
      this.handlers.onSignalingState('connected');
      this.handlers.onLocalEvent('SIGNALING_REGISTERED', `Registered as ${message.peer.name} (${message.peer.id}).`);
    } else if (message.type === 'presence') {
      this.roster = Array.isArray(message.peers) ? message.peers : [];
      this.handlers.onPresence(this.roster);
      this.syncPeers();
    } else if (message.type === 'signal') {
      this.handlers.onLocalEvent('SIGNALING_RECEIVED', `Received ${message.signal.type} signaling from ${message.from}.`);
      this.handlePeerSignal(message.from, message.signal).catch((error) => {
        this.handlers.onError(`WebRTC signaling with ${message.from} failed: ${error.message}`);
      });
    } else if (message.type === 'signal-error' || message.type === 'error') {
      this.handlers.onError(message.message);
    } else if (message.type === 'replaced') {
      this.handlers.onError(message.message);
      this.disconnect();
    }
  }

  isRelevantPeer(peer) {
    if (!peer || peer.id === this.identity.id) return false;
    if (this.identity.role === 'hospital') {
      return peer.role === 'hospital'
        || peer.role === 'other';
    }
    return peer.role === 'hospital';
  }

  shouldInitiate(peer) {
    if (this.identity.role === 'hospital' && peer.role === 'hospital') {
      return this.identity.id.localeCompare(peer.id) < 0;
    }
    return peer.role === 'hospital';
  }

  syncPeers() {
    const onlineIds = new Set(
      this.roster.filter((peer) => this.isRelevantPeer(peer)).map((peer) => peer.id),
    );
    for (const [peerId, peer] of this.peers) {
      if (!onlineIds.has(peerId)) {
        this.closePeer(peerId);
        this.handlers.onPeerRemoved(peerId);
      } else {
        peer.metadata = this.roster.find((item) => item.id === peerId);
      }
    }
    for (const peer of this.roster) {
      if (this.isRelevantPeer(peer) && this.shouldInitiate(peer)) {
        this.ensurePeer(peer.id, true);
      }
    }
  }

  ensurePeer(peerId, createOffer = false) {
    if (!this.peers.has(peerId)) {
      const metadata = this.roster.find((peer) => peer.id === peerId);
      if (!metadata || !this.isRelevantPeer(metadata)) return null;
      const connection = new RTCPeerConnection({ iceServers: this.iceServers });
      const peer = {
        id: peerId,
        metadata,
        connection,
        channel: null,
        pendingCandidates: [],
        makingOffer: false,
        remoteStream: new MediaStream(),
      };
      this.peers.set(peerId, peer);

      connection.addTransceiver('audio', { direction: 'sendrecv' });
      connection.addTransceiver('video', { direction: 'sendrecv' });
      connection.onicecandidate = ({ candidate }) => {
        if (candidate) {
          this.sendSignal(peerId, {
            type: 'candidate',
            candidate: candidate.toJSON(),
          });
        }
      };
      connection.ontrack = ({ track, streams }) => {
        const stream = streams[0];
        if (stream) {
          peer.remoteStream = stream;
        } else {
          peer.remoteStream.addTrack(track);
        }
        this.handlers.onRemoteStream(peerId, peer.remoteStream);
        track.onunmute = () => this.handlers.onRemoteStream(peerId, peer.remoteStream);
      };
      connection.ondatachannel = ({ channel }) => this.configureDataChannel(peer, channel);
      connection.onconnectionstatechange = () => this.emitPeerState(peer);
      connection.oniceconnectionstatechange = () => this.emitPeerState(peer);
      connection.onicegatheringstatechange = () => this.emitPeerState(peer);
      this.handlers.onPeerState(peerId, this.peerState(peer));
      if (createOffer) {
        const channel = connection.createDataChannel('hospital-cluster', { ordered: true });
        this.configureDataChannel(peer, channel);
        this.makeOffer(peer).catch((error) => {
          this.handlers.onError(`Could not create an offer for ${peerId}: ${error.message}`);
        });
      }
    }
    return this.peers.get(peerId);
  }

  peerState(peer) {
    return {
      connectionState: peer.connection.connectionState,
      iceConnectionState: peer.connection.iceConnectionState,
      dataChannelState: peer.channel?.readyState || 'connecting',
      signalingState: peer.connection.signalingState,
    };
  }

  emitPeerState(peer) {
    this.handlers.onPeerState(peer.id, this.peerState(peer));
    if (peer.connection.connectionState === 'failed') {
      this.handlers.onLocalEvent('PEER_FAILED', `${peer.id} WebRTC connection failed; reconnecting.`);
      this.schedulePeerReconnect(peer.id);
    } else if (peer.connection.connectionState === 'connected') {
      this.handlers.onLocalEvent('PEER_CONNECTED', `Direct WebRTC connection established with ${peer.id}.`);
    }
  }

  configureDataChannel(peer, channel) {
    peer.channel = channel;
    channel.onopen = () => {
      this.emitPeerState(peer);
      this.handlers.onLocalEvent('DATACHANNEL_OPEN', `Peer-to-peer data channel open with ${peer.id}.`);
    };
    channel.onclose = () => {
      this.emitPeerState(peer);
      this.handlers.onLocalEvent('DATACHANNEL_CLOSED', `Peer-to-peer data channel closed with ${peer.id}.`);
    };
    channel.onerror = () => {
      this.handlers.onLocalEvent(
        'DATACHANNEL_ERROR',
        `The data channel with ${peer.id} reported a transport error; checking connection state.`,
      );
    };
    channel.onmessage = (event) => this.handlePeerMessage(peer.id, event.data);
  }

  async makeOffer(peer) {
    if (!this.shouldInitiate(peer.metadata) || peer.makingOffer) return;
    peer.makingOffer = true;
    try {
      const offer = await peer.connection.createOffer();
      await peer.connection.setLocalDescription(offer);
      this.sendSignal(peer.id, {
        type: 'offer',
        description: peer.connection.localDescription,
      });
    } finally {
      peer.makingOffer = false;
    }
  }

  async handlePeerSignal(peerId, signal) {
    const metadata = this.roster.find((peer) => peer.id === peerId);
    if (!metadata || !this.isRelevantPeer(metadata)) {
      throw new Error('The sending peer is not in this node\'s allowed topology.');
    }
    const peer = this.ensurePeer(peerId, false);
    if (!peer) throw new Error('Could not create a peer connection.');

    if (signal.type === 'offer') {
      if (this.shouldInitiate(metadata) && peer.connection.signalingState !== 'stable') {
        this.handlers.onLocalEvent('SIGNALING_IGNORED', `Ignored colliding offer from ${peerId}.`);
        return;
      }
      await peer.connection.setRemoteDescription(signal.description);
      await this.flushCandidates(peer);
      const answer = await peer.connection.createAnswer();
      await peer.connection.setLocalDescription(answer);
      this.sendSignal(peerId, {
        type: 'answer',
        description: peer.connection.localDescription,
      });
    } else if (signal.type === 'answer') {
      if (peer.connection.signalingState !== 'have-local-offer') {
        this.handlers.onLocalEvent('SIGNALING_IGNORED', `Ignored stale answer from ${peerId}.`);
        return;
      }
      await peer.connection.setRemoteDescription(signal.description);
      await this.flushCandidates(peer);
    } else if (signal.type === 'candidate') {
      if (!signal.candidate) return;
      if (!peer.connection.remoteDescription) {
        if (peer.pendingCandidates.length < MAX_ICE_CANDIDATES) {
          peer.pendingCandidates.push(signal.candidate);
        }
        return;
      }
      await peer.connection.addIceCandidate(signal.candidate);
    }
    this.emitPeerState(peer);
  }

  async flushCandidates(peer) {
    const candidates = peer.pendingCandidates.splice(0);
    for (const candidate of candidates) {
      await peer.connection.addIceCandidate(candidate);
    }
  }

  sendSignal(peerId, signal) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      this.handlers.onError(`Signaling is disconnected; could not send ${signal.type} to ${peerId}.`);
      return false;
    }
    this.socket.send(JSON.stringify({ type: 'signal', to: peerId, signal }));
    this.handlers.onLocalEvent('SIGNALING_SENT', `Sent ${signal.type} signaling to ${peerId}.`);
    return true;
  }

  sendToPeer(peerId, type, payload, lamportTimestamp) {
    const peer = this.peers.get(peerId);
    if (!peer || peer.channel?.readyState !== 'open') {
      throw new Error(`${peerId} is not connected through a peer-to-peer data channel.`);
    }
    const message = {
      id: createUuid(),
      senderId: this.identity.id,
      type,
      payload,
      lamportTimestamp,
      sentAt: new Date().toISOString(),
    };
    peer.channel.send(JSON.stringify(message));
    this.handlers.onPeerMessage({
      ...message,
      direction: 'sent',
      transport: 'WebRTC DataChannel',
    });
    return message.id;
  }

  broadcast(type, payload, lamportTimestamp) {
    const connected = [...this.peers.keys()].filter(
      (peerId) => this.peers.get(peerId)?.channel?.readyState === 'open',
    );
    for (const peerId of connected) {
      this.sendToPeer(peerId, type, payload, lamportTimestamp);
    }
    return connected;
  }

  handlePeerMessage(peerId, rawMessage) {
    let message;
    try {
      message = JSON.parse(rawMessage);
    } catch {
      this.handlers.onError(`Received invalid peer data from ${peerId}.`);
      return;
    }
    if (
      !message
      || typeof message.id !== 'string'
      || typeof message.senderId !== 'string'
      || message.senderId !== peerId
      || typeof message.type !== 'string'
      || !Number.isSafeInteger(message.lamportTimestamp)
      || message.lamportTimestamp < 0
    ) {
      this.handlers.onError(`Rejected malformed peer data from ${peerId}.`);
      return;
    }
    if (this.seenMessages.has(message.id)) return;
    this.seenMessages.add(message.id);
    if (this.seenMessages.size > MAX_RECENT_MESSAGES) {
      this.seenMessages.delete(this.seenMessages.values().next().value);
    }
    this.handlers.onPeerMessage({
      ...message,
      direction: 'received',
      transport: 'WebRTC DataChannel',
    });
  }

  async attachLocalStream(stream, peerId) {
    const peer = this.peers.get(peerId);
    if (!peer || peer.connection.connectionState !== 'connected') {
      throw new Error(`A live WebRTC connection to ${peerId} is required before starting video.`);
    }
    for (const track of stream.getTracks()) {
      const sender = peer.connection.getSenders().find((item) => item.track?.kind === track.kind);
      const transceiver = peer.connection.getTransceivers().find(
        (item) => item.receiver.track.kind === track.kind,
      );
      const targetSender = sender || transceiver?.sender;
      if (!targetSender) throw new Error(`Could not negotiate the ${track.kind} media track.`);
      await targetSender.replaceTrack(track);
    }
    this.localStream = stream;
  }

  async removeLocalStream(peerId) {
    const peer = this.peers.get(peerId);
    if (peer) {
      await Promise.all(
        peer.connection.getSenders()
          .filter((sender) => sender.track)
          .map((sender) => sender.replaceTrack(null)),
      );
    }
    if (![...this.peers.values()].some(
      (item) => item.id !== peerId && item.connection.getSenders().some((sender) => sender.track),
    )) {
      for (const track of this.localStream?.getTracks() || []) track.stop();
      this.localStream = null;
    }
  }

  schedulePeerReconnect(peerId) {
    const peer = this.peers.get(peerId);
    if (!peer || peer.reconnectTimer) return;
    peer.reconnectTimer = window.setTimeout(() => {
      const metadata = this.roster.find((item) => item.id === peerId);
      this.closePeer(peerId);
      if (metadata && this.shouldInitiate(metadata)) {
        this.ensurePeer(peerId, true);
      }
    }, 1500);
  }

  closePeer(peerId) {
    const peer = this.peers.get(peerId);
    if (!peer) return;
    if (peer.reconnectTimer) window.clearTimeout(peer.reconnectTimer);
    peer.channel?.close();
    peer.connection.close();
    this.peers.delete(peerId);
    this.handlers.onPeerState(peerId, {
      connectionState: 'closed',
      iceConnectionState: 'closed',
      dataChannelState: 'closed',
      signalingState: 'closed',
    });
  }

  clearPeers() {
    for (const peerId of [...this.peers.keys()]) this.closePeer(peerId);
  }

  disconnect() {
    this.shouldReconnect = false;
    if (this.reconnectTimer) window.clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.clearPeers();
    for (const track of this.localStream?.getTracks() || []) track.stop();
    this.localStream = null;
    const socket = this.socket;
    this.socket = null;
    if (socket && socket.readyState < WebSocket.CLOSING) socket.close(1000, 'Node disconnected');
    this.handlers.onSignalingState('disconnected');
    this.handlers.onPresence([]);
  }
}

export { HOSPITAL_IDS };
