import { useCallback, useEffect, useRef, useState } from 'react';
import { HOSPITALS, IcuMutex, LamportClock } from '../services/hospitalProtocol';
import { PeerNetwork } from '../services/peerNetwork';
import { createUuid } from '../services/uuid';

const HEARTBEAT_INTERVAL_MS = 2_000;
const HEARTBEAT_TIMEOUT_MS = 8_000;
const CALL_END_ACK_TIMEOUT_MS = 3_000;

function compareHospitalIds(left, right) {
  return Number(left.replace(/\D/g, '')) - Number(right.replace(/\D/g, ''));
}

export default function useHospitalNetwork(identity, onConsultationComplete) {
  const [signalingState, setSignalingState] = useState('disconnected');
  const [roster, setRoster] = useState([]);
  const [peerStates, setPeerStates] = useState({});
  const [remoteStreams, setRemoteStreams] = useState({});
  const [nodeReports, setNodeReports] = useState({});
  const [events, setEvents] = useState([]);
  const [errors, setErrors] = useState([]);
  const [coordinator, setCoordinator] = useState({ leaderId: null, epoch: 0 });
  const [lastHeartbeatAt, setLastHeartbeatAt] = useState(null);
  const [localIcuBeds, setLocalIcuBeds] = useState(identity.initialIcuBeds || 0);
  const [requestResults, setRequestResults] = useState({});
  const [calls, setCalls] = useState({});
  const [localStream, setLocalStream] = useState(null);
  const [mediaError, setMediaError] = useState('');

  const networkRef = useRef(null);
  const clockRef = useRef(new LamportClock());
  const mutexRef = useRef(new IcuMutex());
  const coordinatorRef = useRef(coordinator);
  const electionEpochRef = useRef(0);
  const electionRoundsRef = useRef(new Map());
  const electionTimersRef = useRef(new Map());
  const callEndTimersRef = useRef(new Map());
  const announcedDataChannelsRef = useRef(new Set());
  const heartbeatTimeRef = useRef(null);
  const failureTriggeredRef = useRef('');
  const requestCounterRef = useRef(0);
  const onConsultationCompleteRef = useRef(onConsultationComplete);
  const callsRef = useRef(calls);
  const remoteStreamsRef = useRef({});
  const peerFailureReportedRef = useRef(new Set());
  const markCallConnectedRef = useRef(null);
  const activeRef = useRef(true);
  const icuBedsRef = useRef(localIcuBeds);

  onConsultationCompleteRef.current = onConsultationComplete;
  callsRef.current = calls;

  const addEvent = useCallback((event) => {
    if (!activeRef.current) return;
    setEvents((previous) => [
      {
        id: `${Date.now()}-${createUuid()}`,
        occurredAt: new Date().toISOString(),
        kind: event.kind || 'LOCAL',
        direction: event.direction || 'local',
        source: event.source || identity.id,
        target: event.target || '',
        lamportTimestamp: event.lamportTimestamp ?? clockRef.current.value,
        text: event.text,
      },
      ...previous,
    ].slice(0, 100));
  }, [identity.id]);

  const reportError = useCallback((message) => {
    if (!activeRef.current) return;
    setErrors((previous) => [
      { id: createUuid(), message, at: new Date().toISOString() },
      ...previous,
    ].slice(0, 5));
  }, []);

  markCallConnectedRef.current = (peerId) => {
    const call = callsRef.current[peerId];
    const hasLiveLocalMedia = networkRef.current?.localStream?.getTracks().some(
      (track) => track.readyState === 'live' && track.enabled,
    );
    const peerIsConnected = networkRef.current?.peers.get(peerId)?.connection.connectionState === 'connected';
    if (call?.status === 'connecting' && hasLiveLocalMedia && peerIsConnected) {
      const next = { ...callsRef.current, [peerId]: { ...call, status: 'connected' } };
      callsRef.current = next;
      setCalls(next);
      addEvent({
        kind: 'WEBRTC',
        source: peerId,
        target: identity.id,
        text: `WebRTC media connection is ready for the video consultation with ${peerId}.`,
      });
    }
  };

  const updateCoordinator = useCallback((next) => {
    coordinatorRef.current = next;
    setCoordinator(next);
    heartbeatTimeRef.current = Date.now();
    setLastHeartbeatAt(heartbeatTimeRef.current);
  }, []);

  const sendPeer = useCallback((peerId, type, payload = {}) => {
    const network = networkRef.current;
    if (!network) throw new Error('This hospital node is disconnected.');
    const timestamp = clockRef.current.tick();
    return network.sendToPeer(peerId, type, payload, timestamp);
  }, []);

  const cleanupCallMedia = useCallback((peerId) => {
    const network = networkRef.current;
    if (network) {
      network.removeLocalStream(peerId)
        .then(() => setLocalStream(network.localStream))
        .catch((error) => reportError(error.message));
    }
    delete remoteStreamsRef.current[peerId];
    setRemoteStreams((previous) => {
      if (!previous[peerId]) return previous;
      const next = { ...previous };
      delete next[peerId];
      return next;
    });
  }, [reportError]);

  const broadcast = useCallback((type, payload = {}) => {
    const network = networkRef.current;
    if (!network) throw new Error('This hospital node is disconnected.');
    const sentTo = [];
    const failures = [];
    const connected = [...network.peers.entries()]
      .filter(([, peer]) => (
        peer.connection.connectionState === 'connected'
        && peer.channel?.readyState === 'open'
        && (type !== 'NODE_STATUS' || peer.metadata?.role === 'hospital')
      ))
      .map(([peerId]) => peerId);
    for (const peerId of connected) {
      try {
        sendPeer(peerId, type, payload);
        sentTo.push(peerId);
      } catch (error) {
        const peer = network.peers.get(peerId);
        if (peer?.connection.connectionState === 'connected' && peer.channel?.readyState === 'open') {
          failures.push(`${peerId}: ${error.message}`);
        } else {
          addEvent({
            kind: 'WEBRTC',
            target: peerId,
            text: `Skipped ${type} because ${peerId} disconnected during the broadcast.`,
          });
        }
      }
    }
    if (failures.length > 0) reportError(`Some peer operations could not be sent: ${failures.join('; ')}`);
    return sentTo;
  }, [addEvent, reportError, sendPeer]);

  const publishNodeStatus = useCallback(() => {
    if (identity.role !== 'hospital' || !networkRef.current) return;
    broadcast('NODE_STATUS', {
      hospitalId: identity.hospitalId,
      name: identity.name,
      online: true,
      icuBeds: localIcuBeds,
      role: coordinatorRef.current.leaderId === identity.id ? 'Coordinator' : 'Hospital',
      coordinatorId: coordinatorRef.current.leaderId,
      epoch: coordinatorRef.current.epoch,
    });
  }, [broadcast, identity, localIcuBeds]);

  const peerIds = useCallback(() => Object.keys(peerStates).filter(
    (peerId) => peerId.startsWith('H')
      && peerStates[peerId].connectionState === 'connected'
      && peerStates[peerId].dataChannelState === 'open',
  ), [peerStates]);

  const announceCoordinator = useCallback((leaderId, epoch) => {
    if (!HOSPITALS.some((hospital) => hospital.id === leaderId) || !Number.isSafeInteger(epoch) || epoch < 1) {
      return false;
    }
    const current = coordinatorRef.current;
    const leaderOrder = compareHospitalIds(leaderId, current.leaderId || leaderId);
    if (leaderId === current.leaderId && epoch === current.epoch) return false;
    if (epoch < current.epoch || epoch === current.epoch && current.leaderId && leaderOrder < 0) {
      return false;
    }
    if (leaderId !== identity.id) {
      const reachableHigher = peerIds().some((peerId) => compareHospitalIds(peerId, leaderId) > 0);
      if (reachableHigher) return false;
    }
    updateCoordinator({ leaderId, epoch, algorithm: 'BULLY' });
    addEvent({
      kind: 'ELECTION',
      source: leaderId,
      target: 'cluster',
      text: `${leaderId} is coordinator for election epoch ${epoch}.`,
    });
    publishNodeStatus();
    return true;
  }, [addEvent, identity.id, peerIds, publishNodeStatus, updateCoordinator]);

  const startBullyElection = useCallback((requestedEpoch) => {
    if (identity.role !== 'hospital') {
      reportError('Only a hospital node can participate in coordinator election.');
      return;
    }
    const epoch = requestedEpoch || Math.max(
      coordinatorRef.current.epoch,
      electionEpochRef.current,
    ) + 1;
    if (epoch < electionEpochRef.current) return;
    electionEpochRef.current = epoch;
    let round = electionRoundsRef.current.get(epoch);
    if (!round) {
      round = { acknowledgements: new Set(), announced: false };
      electionRoundsRef.current.set(epoch, round);
    }
    addEvent({
      kind: 'ELECTION',
      target: 'cluster',
      lamportTimestamp: clockRef.current.tick(),
      text: `${identity.id} starts a Bully election at epoch ${epoch}.`,
    });

    const higherPeers = peerIds().filter((peerId) => compareHospitalIds(peerId, identity.id) > 0);
    for (const peerId of higherPeers) {
      try {
        sendPeer(peerId, 'ELECTION', {
          candidateId: identity.id,
          epoch,
        });
        addEvent({
          kind: 'ELECTION',
          direction: 'sent',
          target: peerId,
          text: `Sent ELECTION to higher-priority node ${peerId}.`,
        });
      } catch (error) {
        const peer = networkRef.current?.peers.get(peerId);
        if (peer?.connection.connectionState === 'connected' && peer.channel?.readyState === 'open') {
          reportError(error.message);
        } else {
          addEvent({
            kind: 'WEBRTC',
            target: peerId,
            text: `Skipped the hospital status update because ${peerId} disconnected during delivery.`,
          });
        }
      }
    }

    if (electionTimersRef.current.has(epoch)) {
      window.clearTimeout(electionTimersRef.current.get(epoch));
    }
    const timer = window.setTimeout(() => {
      electionTimersRef.current.delete(epoch);
      const currentRound = electionRoundsRef.current.get(epoch);
      if (!currentRound || currentRound.announced || currentRound.acknowledgements.size > 0) return;
      currentRound.announced = true;
      const accepted = announceCoordinator(identity.id, epoch);
      if (accepted) {
        broadcast('COORDINATOR', { leaderId: identity.id, epoch, algorithm: 'BULLY' });
      }
    }, 1_500);
    electionTimersRef.current.set(epoch, timer);
  }, [addEvent, announceCoordinator, broadcast, identity.id, identity.role, peerIds, reportError, sendPeer]);

  const stopNode = useCallback((announce = false) => {
    const network = networkRef.current;
    if (!network) return;
    if (announce) {
      try {
        broadcast('NODE_STOPPING', { hospitalId: identity.hospitalId });
      } catch {
        reportError('Could not notify peers before disconnecting this node.');
      }
    }
    network.disconnect();
    networkRef.current = null;
    setSignalingState('disconnected');
    setRoster([]);
    setPeerStates({});
    setRemoteStreams({});
    setLocalStream(null);
    setCalls({});
    callsRef.current = {};
    for (const timer of electionTimersRef.current.values()) window.clearTimeout(timer);
    electionTimersRef.current.clear();
    electionRoundsRef.current.clear();
  }, [broadcast, identity.hospitalId, reportError]);

  const startNode = useCallback(() => {
    if (networkRef.current) return;
    if (typeof RTCPeerConnection === 'undefined' || typeof WebSocket === 'undefined') {
      reportError('This browser does not support WebRTC or WebSocket.');
      return;
    }
    clockRef.current = new LamportClock();
    activeRef.current = true;
    let network;
    try {
      network = new PeerNetwork(identity, {
      onSignalingState: setSignalingState,
      onPresence: (peers) => {
        setRoster(peers);
        const onlineHospitals = new Set(
          peers
            .filter((peer) => peer.role === 'hospital')
            .map((peer) => peer.hospitalId),
        );
        setNodeReports((previous) => {
          let changed = false;
          const next = { ...previous };
          for (const [hospitalId, report] of Object.entries(previous)) {
            const online = onlineHospitals.has(hospitalId);
            if (report.online !== online) {
              next[hospitalId] = { ...report, online };
              changed = true;
            }
          }
          return changed ? next : previous;
        });
      },
      onPeerState: (peerId, state) => {
        setPeerStates((previous) => ({ ...previous, [peerId]: state }));
        if (state.connectionState === 'connected' && state.dataChannelState === 'open') {
          peerFailureReportedRef.current.delete(peerId);
        }
        if (
          state.connectionState === 'failed'
          && !peerFailureReportedRef.current.has(peerId)
        ) {
          peerFailureReportedRef.current.add(peerId);
          const hospitalName = HOSPITALS.find((hospital) => hospital.id === peerId)?.name || peerId;
          reportError(
            `The direct connection to ${hospitalName} failed. Make sure both nodes are online. If they are on different networks, configure a TURN server.`,
          );
        }
        if (state.connectionState === 'failed' || state.connectionState === 'closed') {
          const call = callsRef.current[peerId];
          if (call && call.status !== 'ended') {
            const timer = callEndTimersRef.current.get(peerId);
            if (timer) window.clearTimeout(timer);
            callEndTimersRef.current.delete(peerId);
            const next = { ...callsRef.current, [peerId]: { ...call, status: 'ended', endUnconfirmed: true } };
            callsRef.current = next;
            setCalls(next);
            cleanupCallMedia(peerId);
          }
        }
        if (
          identity.role === 'hospital'
          && peerId.startsWith('H')
          && state.dataChannelState === 'open'
          && !announcedDataChannelsRef.current.has(peerId)
        ) {
          announcedDataChannelsRef.current.add(peerId);
          try {
            sendPeer(peerId, 'NODE_STATUS', {
              hospitalId: identity.hospitalId,
              name: identity.name,
              online: identity.role === 'hospital',
              icuBeds: identity.initialIcuBeds || 0,
              role: 'Hospital',
              coordinatorId: null,
              epoch: 0,
            });
          } catch (error) {
            reportError(error.message);
          }
        } else if (state.dataChannelState !== 'open') {
          announcedDataChannelsRef.current.delete(peerId);
        }
        if (state.connectionState === 'connected') {
          markCallConnectedRef.current?.(peerId);
          addEvent({
            kind: 'WEBRTC',
            source: identity.id,
            target: peerId,
            text: `Direct WebRTC connection state is connected with ${peerId}.`,
          });
        }
      },
      onPeerRemoved: (peerId) => {
        setPeerStates((previous) => {
          const next = { ...previous };
          delete next[peerId];
          announcedDataChannelsRef.current.delete(peerId);
          return next;
        });
        setRemoteStreams((previous) => {
          const next = { ...previous };
          delete next[peerId];
          delete remoteStreamsRef.current[peerId];
          return next;
        });
        const call = callsRef.current[peerId];
        if (call && call.status !== 'ended') {
          const timer = callEndTimersRef.current.get(peerId);
          if (timer) window.clearTimeout(timer);
          callEndTimersRef.current.delete(peerId);
          const next = { ...callsRef.current, [peerId]: { ...call, status: 'ended', endUnconfirmed: true } };
          callsRef.current = next;
          setCalls(next);
          cleanupCallMedia(peerId);
        }
      },
      onRemoteStream: (peerId, stream) => {
        remoteStreamsRef.current[peerId] = stream;
        setRemoteStreams((previous) => ({ ...previous, [peerId]: stream }));
        markCallConnectedRef.current?.(peerId);
      },
      onPeerMessage: (message) => {
        if (message.direction === 'received') {
          clockRef.current.receive(message.lamportTimestamp);
          addEvent({
            kind: 'P2P',
            direction: 'received',
            source: message.senderId,
            target: identity.id,
            lamportTimestamp: clockRef.current.value,
            text: `${message.type} from ${message.senderId} over WebRTC DataChannel.`,
          });
          handlePeerMessageRef.current?.(message);
        } else {
          addEvent({
            kind: 'P2P',
            direction: 'sent',
            source: identity.id,
            target: message.senderId,
            lamportTimestamp: message.lamportTimestamp,
            text: `${message.type} to ${message.senderId} over WebRTC DataChannel.`,
          });
        }
      },
      onLocalEvent: (type, text) => addEvent({ kind: 'SIGNALING', text: `${type}: ${text}` }),
      onError: reportError,
      });
      networkRef.current = network;
      network.connect();
    } catch (error) {
      network?.disconnect();
      networkRef.current = null;
      reportError(`Could not start the hospital network: ${error.message}`);
    }
  }, [addEvent, cleanupCallMedia, identity, reportError, sendPeer]);

  const handlePeerMessageRef = useRef(null);

  const handlePeerMessage = useCallback((message) => {
    const { senderId, type, payload = {} } = message;
    if (type === 'NODE_STATUS') {
      if (payload.hospitalId !== senderId || !HOSPITALS.some((hospital) => hospital.id === senderId)) return;
      setNodeReports((previous) => ({ ...previous, [senderId]: payload }));
      if (payload.role === 'Coordinator' && payload.coordinatorId) {
        const epoch = payload.epoch || 0;
        announceCoordinator(payload.coordinatorId, epoch);
        if (
          identity.role === 'hospital'
          && compareHospitalIds(identity.id, payload.coordinatorId) > 0
        ) {
          startBullyElection(Math.max(epoch, electionEpochRef.current) + 1);
        }
      }
    } else if (type === 'ELECTION') {
      const candidateId = payload.candidateId;
      const epoch = payload.epoch;
      if (candidateId !== senderId || !Number.isSafeInteger(epoch) || epoch < 1) return;
      electionEpochRef.current = Math.max(electionEpochRef.current, epoch);
      addEvent({
        kind: 'ELECTION',
        source: senderId,
        target: identity.id,
        text: `${identity.id} received ELECTION from ${senderId} at epoch ${epoch}.`,
      });
      if (compareHospitalIds(identity.id, candidateId) > 0 && identity.role === 'hospital') {
        sendPeer(senderId, 'ELECTION_OK', { epoch });
        startBullyElection(epoch);
      }
    } else if (type === 'ELECTION_OK') {
      const round = electionRoundsRef.current.get(payload.epoch);
      if (round) round.acknowledgements.add(senderId);
      addEvent({
        kind: 'ELECTION',
        source: senderId,
        target: identity.id,
        text: `${senderId} replied OK; it will continue the election.`,
      });
    } else if (type === 'COORDINATOR') {
      const epoch = payload.epoch;
      const leaderId = payload.leaderId;
      if (!Number.isSafeInteger(epoch) || leaderId !== senderId) return;
      if (announceCoordinator(leaderId, epoch)) {
        broadcast('COORDINATOR', {
          leaderId,
          epoch,
          algorithm: payload.algorithm || 'BULLY',
        });
      }
    } else if (type === 'HEARTBEAT') {
      if (
        senderId === coordinatorRef.current.leaderId
        && payload.coordinatorId === senderId
        && payload.epoch === coordinatorRef.current.epoch
      ) {
        heartbeatTimeRef.current = Date.now();
        setLastHeartbeatAt(heartbeatTimeRef.current);
        addEvent({
          kind: 'HEARTBEAT',
          source: senderId,
          target: identity.id,
          text: `Coordinator heartbeat received from ${senderId}.`,
        });
      }
    } else if (type === 'ICU_REQUEST') {
      if (identity.role !== 'hospital' || typeof payload.requestId !== 'string') return;
      const result = mutexRef.current.enqueue(message);
      if (result.duplicate) {
        if (result.result) sendPeer(senderId, 'ICU_RESULT', result.result);
        return;
      }
      addEvent({
        kind: 'MUTEX',
        source: senderId,
        target: identity.id,
        lamportTimestamp: message.lamportTimestamp,
        text: `ICU request ${payload.requestId} queued by Lamport time.`,
      });
      queueMicrotask(() => {
        let request = mutexRef.current.acquireNext();
        while (request) {
          const available = Math.max(0, Number(icuBedsRef.current) || 0);
          const grant = available > 0;
          if (grant) {
            icuBedsRef.current = available - 1;
            setLocalIcuBeds(icuBedsRef.current);
          }
          const response = {
            requestId: request.payload.requestId,
            hospitalId: identity.hospitalId,
            granted: grant,
            availableIcuBeds: Math.max(0, available - (grant ? 1 : 0)),
            reason: grant ? 'ICU bed reserved under the hospital mutex.' : 'No ICU beds are available.',
          };
          mutexRef.current.finish(request, response);
          sendPeer(request.senderId, 'ICU_RESULT', response);
          addEvent({
            kind: 'MUTEX',
            source: identity.id,
            target: request.senderId,
            text: grant
              ? `ICU bed granted to ${request.senderId}; ${response.availableIcuBeds} remain.`
              : `ICU request from ${request.senderId} rejected; no beds remain.`,
          });
          request = mutexRef.current.acquireNext();
        }
        publishNodeStatus();
      });
    } else if (type === 'ICU_RESULT') {
      setRequestResults((previous) => ({
        ...previous,
        [payload.requestId]: { ...payload, receivedAt: new Date().toISOString() },
      }));
      addEvent({
        kind: 'MUTEX',
        source: senderId,
        target: identity.id,
        text: payload.granted
          ? `${senderId} granted the ICU request.`
          : `${senderId} rejected the ICU request: ${payload.reason}`,
      });
    } else if (type === 'CALL_REQUEST') {
      setCalls((previous) => {
        const next = {
          ...previous,
          [senderId]: {
            status: 'incoming',
            callId: payload.callId,
            context: payload.context || null,
            mode: 'video',
          },
        };
        callsRef.current = next;
        return next;
      });
    } else if (type === 'CALL_ACCEPT') {
      const existing = callsRef.current[senderId];
      if (existing && payload.callId === existing.callId) {
        setCalls((previous) => {
          const next = { ...previous, [senderId]: { ...previous[senderId], status: 'connecting' } };
          callsRef.current = next;
          return next;
        });
        window.setTimeout(() => markCallConnectedRef.current?.(senderId), 0);
        enableMediaForPeer(senderId).then(() => {
          markCallConnectedRef.current?.(senderId);
        }).catch((error) => {
          const current = callsRef.current[senderId];
          if (current?.callId !== payload.callId) return;
          const retryable = {
            ...callsRef.current,
            [senderId]: { ...current, status: 'media-error' },
          };
          callsRef.current = retryable;
          setCalls(retryable);
          setMediaError(error.message);
          reportError(`Could not enable call media: ${error.message}`);
        });
      }
    } else if (type === 'CALL_END') {
      const existing = callsRef.current[senderId];
      if (existing && payload.callId === existing.callId) {
        try {
          sendPeer(senderId, 'CALL_END_ACK', { callId: payload.callId });
        } catch (error) {
          reportError(`The call ended, but confirmation could not be sent: ${error.message}`);
        }
        const timer = callEndTimersRef.current.get(senderId);
        if (timer) window.clearTimeout(timer);
        callEndTimersRef.current.delete(senderId);
        const next = { ...callsRef.current, [senderId]: { ...existing, status: 'ended' } };
        callsRef.current = next;
        setCalls(next);
        cleanupCallMedia(senderId);
        addEvent({
          kind: 'WEBRTC',
          source: senderId,
          target: identity.id,
          text: `The call with ${senderId} ended.`,
        });
      }
    } else if (type === 'CALL_END_ACK') {
      const existing = callsRef.current[senderId];
      if (existing && existing.callId === payload.callId && existing.status === 'ending') {
        const timer = callEndTimersRef.current.get(senderId);
        if (timer) window.clearTimeout(timer);
        callEndTimersRef.current.delete(senderId);
        const next = { ...callsRef.current, [senderId]: { ...existing, status: 'ended' } };
        callsRef.current = next;
        setCalls(next);
        addEvent({
          kind: 'WEBRTC',
          source: identity.id,
          target: senderId,
          text: `The call with ${senderId} ended and was confirmed by the other participant.`,
        });
      }
    } else if (type === 'CALL_REJECT') {
      const existing = callsRef.current[senderId];
      if (existing && payload.callId === existing.callId) {
        const next = { ...callsRef.current, [senderId]: { ...existing, status: 'ended' } };
        callsRef.current = next;
        setCalls(next);
        cleanupCallMedia(senderId);
      }
    } else if (type === 'CONSULTATION_COMPLETE') {
      const call = callsRef.current[senderId];
      if (
        call?.status === 'connected'
        && call.context?.request_id === payload.request_id
        && call.context?.hospital_id === payload.hospital_id
      ) {
        Promise.resolve(onConsultationCompleteRef.current?.({
          request_id: payload.request_id,
          hospital_id: payload.hospital_id,
        })).then((completed) => {
          if (!completed) return;
          const current = callsRef.current[senderId];
          if (current?.callId !== call.callId) return;
          const next = { ...callsRef.current, [senderId]: { ...current, consultationCompleted: true } };
          callsRef.current = next;
          setCalls(next);
        }).catch((error) => reportError(`Consultation completion failed: ${error.message}`));
      } else {
        reportError('Ignored an unmatched consultation-completion message.');
      }
    } else if (type === 'SIMULATE_FAILURE') {
      addEvent({
        kind: 'LOCAL',
        source: senderId,
        target: identity.id,
        text: `Failure simulation requested by ${senderId}; disconnecting this node.`,
      });
      window.setTimeout(() => stopNode(false), 20);
    } else if (type === 'NODE_STOPPING') {
      setNodeReports((previous) => ({
        ...previous,
        [senderId]: { ...previous[senderId], online: false },
      }));
    } else if (type === 'CLUSTER_RESET') {
      resetLocalState(false);
    }
  }, [
    addEvent,
    announceCoordinator,
    broadcast,
    cleanupCallMedia,
    identity.hospitalId,
    identity.id,
    identity.role,
    publishNodeStatus,
    reportError,
    sendPeer,
    startBullyElection,
    stopNode,
  ]);

  const resetLocalState = useCallback((sendToPeers = true) => {
    if (sendToPeers) broadcast('CLUSTER_RESET', {});
    const hospital = HOSPITALS.find((item) => item.id === identity.hospitalId);
    icuBedsRef.current = hospital?.initialIcuBeds || 0;
    setLocalIcuBeds(icuBedsRef.current);
    setRequestResults({});
    setCalls({});
    callsRef.current = {};
    setNodeReports({});
    mutexRef.current = new IcuMutex();
    electionEpochRef.current = 0;
    failureTriggeredRef.current = '';
    updateCoordinator({ leaderId: null, epoch: 0 });
    setEvents([]);
    addEvent({
      kind: 'LOCAL',
      text: sendToPeers ? 'Reset request broadcast over peer data channels.' : 'Cluster reset received over WebRTC.',
    });
    publishNodeStatus();
  }, [addEvent, broadcast, identity.hospitalId, publishNodeStatus, updateCoordinator]);

  const enableMediaForPeer = useCallback(async (peerId) => {
    setMediaError('');
    if (!navigator.mediaDevices?.getUserMedia) {
      throw new Error('Camera and microphone require HTTPS or localhost in a supported browser.');
    }
    const network = networkRef.current;
    if (!network) throw new Error('This hospital node is disconnected.');
    let stream = network.localStream;
    if (!stream || stream.getTracks().every((track) => track.readyState !== 'live')) {
      try {
        stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: true });
      } catch (videoError) {
        try {
          stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
          setMediaError('Camera access is unavailable; continuing with microphone audio.');
        } catch {
          throw videoError;
        }
      }
    }
    await network.attachLocalStream(stream, peerId);
    setLocalStream(new MediaStream(stream.getTracks()));
    return stream;
  }, []);

  const prepareVideoCall = useCallback(async () => {
    setMediaError('');
    if (!navigator.mediaDevices?.getUserMedia) {
      throw new Error('Camera and microphone require HTTPS or localhost in a supported browser.');
    }
    let stream = networkRef.current?.localStream;
    if (!stream || stream.getTracks().every((track) => track.readyState !== 'live')) {
      try {
        stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: true });
      } catch (videoError) {
        try {
          stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: false });
          setMediaError('Camera access is unavailable; continuing with microphone audio.');
        } catch {
          throw videoError;
        }
      }
    }
    if (!networkRef.current) throw new Error('This hospital node is disconnected.');
    networkRef.current.localStream = stream;
    setLocalStream(stream);
    return stream;
  }, []);

  const retryCallMedia = useCallback(async (peerId) => {
    const call = callsRef.current[peerId];
    if (!call || call.status !== 'media-error') return;
    try {
      await enableMediaForPeer(peerId);
      const next = {
        ...callsRef.current,
        [peerId]: { ...call, status: 'connecting' },
      };
      callsRef.current = next;
      setCalls(next);
      markCallConnectedRef.current?.(peerId);
    } catch (error) {
      setMediaError(error.message);
      reportError(`Could not enable call media: ${error.message}`);
    }
  }, [enableMediaForPeer, reportError]);

  const setMediaTrackEnabled = useCallback((kind, enabled) => {
    const stream = networkRef.current?.localStream;
    const tracks = kind === 'audio'
      ? stream?.getAudioTracks() || []
      : stream?.getVideoTracks() || [];
    if (tracks.length === 0) {
      reportError(kind === 'audio' ? 'No microphone track is available.' : 'No camera track is available.');
      return;
    }
    tracks.forEach((track) => {
      track.enabled = enabled;
    });
    setLocalStream(new MediaStream(stream.getTracks()));
    if (!enabled) {
      for (const peerId of Object.keys(callsRef.current)) {
        markCallConnectedRef.current?.(peerId);
      }
    }
  }, [reportError]);

  handlePeerMessageRef.current = handlePeerMessage;

  useEffect(() => {
    activeRef.current = true;
    return () => {
      activeRef.current = false;
      networkRef.current?.disconnect();
      networkRef.current = null;
      for (const timer of electionTimersRef.current.values()) window.clearTimeout(timer);
      electionTimersRef.current.clear();
      for (const timer of callEndTimersRef.current.values()) window.clearTimeout(timer);
      callEndTimersRef.current.clear();
    };
  }, []);

  useEffect(() => {
    if (signalingState !== 'connected') return undefined;
    publishNodeStatus();
    return undefined;
  }, [localIcuBeds, coordinator, signalingState, publishNodeStatus]);

  useEffect(() => {
    if (signalingState !== 'connected' || identity.role !== 'hospital') return undefined;
    const timer = window.setInterval(() => {
      const current = coordinatorRef.current;
      if (!current.leaderId) return;
      if (current.leaderId === identity.id) {
        broadcast('HEARTBEAT', {
          coordinatorId: identity.id,
          epoch: current.epoch,
        });
        return;
      }

      const lastBeat = heartbeatTimeRef.current;
      if (
        lastBeat !== null
        && Date.now() - lastBeat > HEARTBEAT_TIMEOUT_MS
        && failureTriggeredRef.current !== `${current.leaderId}:${current.epoch}`
      ) {
        failureTriggeredRef.current = `${current.leaderId}:${current.epoch}`;
        addEvent({
          kind: 'FAILURE',
          source: current.leaderId,
          target: identity.id,
          text: `Coordinator heartbeat timed out after ${HEARTBEAT_TIMEOUT_MS / 1000}s.`,
        });
        startBullyElection();
      }
    }, HEARTBEAT_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [addEvent, broadcast, identity.id, identity.role, signalingState, startBullyElection]);

  const requestIcuBed = useCallback((targetHospitalId) => {
    if (!HOSPITALS.some((hospital) => hospital.id === targetHospitalId)) {
      reportError('Choose a configured hospital before requesting an ICU bed.');
      return null;
    }
    if (identity.role !== 'hospital' || targetHospitalId === identity.hospitalId) {
      reportError('An ICU request must come from a hospital node and target another hospital.');
      return null;
    }
    const requestId = `${identity.id}-${Date.now()}-${++requestCounterRef.current}`;
    setRequestResults((previous) => ({
      ...previous,
      [requestId]: { requestId, pending: true, hospitalId: targetHospitalId },
    }));
    try {
      sendPeer(targetHospitalId, 'ICU_REQUEST', {
        requestId,
        hospitalId: identity.hospitalId,
      });
      addEvent({
        kind: 'MUTEX',
        direction: 'sent',
        source: identity.id,
        target: targetHospitalId,
        text: `Requested an ICU bed from ${targetHospitalId}.`,
      });
      return requestId;
    } catch (error) {
      setRequestResults((previous) => ({
        ...previous,
        [requestId]: { requestId, pending: false, error: error.message },
      }));
      reportError(error.message);
      return null;
    }
  }, [addEvent, identity.hospitalId, identity.id, identity.role, reportError, sendPeer]);

  const startVideoCall = useCallback((peerId, context = null) => {
    const state = peerStates[peerId];
    if (state?.connectionState !== 'connected' || state?.dataChannelState !== 'open') {
      reportError(`Cannot call ${peerId}: its direct WebRTC data channel is not connected.`);
      return;
    }
    const callId = createUuid();
    try {
      sendPeer(peerId, 'CALL_REQUEST', { callId, context, mode: 'video' });
      const next = {
        ...callsRef.current,
        [peerId]: { status: 'calling', callId, context, mode: 'video' },
      };
      callsRef.current = next;
      setCalls(next);
      addEvent({
        kind: 'WEBRTC',
        source: identity.id,
        target: peerId,
        text: `Started a video call request to ${peerId} over the peer data channel.`,
      });
    } catch (error) {
      reportError(error.message);
    }
  }, [addEvent, identity.id, peerStates, reportError, sendPeer]);

  const acceptVideoCall = useCallback(async (peerId) => {
    const call = callsRef.current[peerId];
    if (!call || call.status !== 'incoming') return;
    const connecting = {
      ...callsRef.current,
      [peerId]: { ...call, status: 'connecting' },
    };
    callsRef.current = connecting;
    setCalls(connecting);
    try {
      await enableMediaForPeer(peerId);
      sendPeer(peerId, 'CALL_ACCEPT', { callId: call.callId });
      markCallConnectedRef.current?.(peerId);
    } catch (error) {
      const retryable = {
        ...callsRef.current,
        [peerId]: { ...call, status: 'incoming' },
      };
      callsRef.current = retryable;
      setCalls(retryable);
      setMediaError(error.message);
      reportError(`Could not accept the video call: ${error.message}`);
    }
  }, [enableMediaForPeer, reportError, sendPeer]);

  const rejectVideoCall = useCallback((peerId) => {
    const call = callsRef.current[peerId];
    if (!call) return;
    try {
      sendPeer(peerId, 'CALL_REJECT', { callId: call.callId });
    } catch (error) {
      reportError(error.message);
    }
    const next = { ...callsRef.current };
    delete next[peerId];
    callsRef.current = next;
    setCalls(next);
  }, [reportError, sendPeer]);

  const completeConnectedConsultation = useCallback(async (peerId, call) => {
    if (call?.status !== 'connected' || !call.context?.request_id || !call.context?.hospital_id) {
      return false;
    }
    if (identity.role === 'other') {
      const complete = onConsultationCompleteRef.current;
      if (!complete) {
        reportError('The patient session could not submit the consultation receipt.');
        return false;
      }
      const completed = await complete({
        request_id: call.context.request_id,
        hospital_id: call.context.hospital_id,
      });
      if (!completed) return false;
    } else {
      try {
        sendPeer(peerId, 'CONSULTATION_COMPLETE', {
          request_id: call.context.request_id,
          hospital_id: call.context.hospital_id,
        });
      } catch (error) {
        reportError(error.message);
        return false;
      }
    }
    addEvent({
      kind: 'P2P',
      source: identity.id,
      target: peerId,
      text: `Completed WebRTC consultation for request ${call.context.request_id}.`,
    });
    const next = { ...callsRef.current, [peerId]: { ...call, consultationCompleted: true } };
    callsRef.current = next;
    setCalls(next);
    return true;
  }, [addEvent, identity.id, identity.role, reportError, sendPeer]);

  const endVideoCall = useCallback((peerId) => {
    const call = callsRef.current[peerId];
    if (!call || call.status === 'ended' || call.status === 'ending') return;
    const ending = { ...callsRef.current, [peerId]: { ...call, status: 'ending' } };
    callsRef.current = ending;
    setCalls(ending);
    const finishEnd = async () => {
      let consultationCompleted = Boolean(call.consultationCompleted);
      if (call.status === 'connected' && call.context?.request_id) {
        consultationCompleted = await completeConnectedConsultation(peerId, call);
      }
      cleanupCallMedia(peerId);
      let sent = false;
      try {
        sendPeer(peerId, 'CALL_END', { callId: call.callId });
        sent = true;
      } catch (error) {
        reportError(`Could not notify ${peerId} that the call ended: ${error.message}`);
      }
      const current = callsRef.current[peerId];
      if (!current || current.callId !== call.callId) return;
      const next = {
        ...callsRef.current,
        [peerId]: {
          ...current,
          status: sent ? 'ending' : 'ended',
          endUnconfirmed: !sent,
          consultationCompleted,
        },
      };
      callsRef.current = next;
      setCalls(next);
      if (sent) {
        const timer = window.setTimeout(() => {
          const latest = callsRef.current[peerId];
          if (latest?.callId !== call.callId || latest.status !== 'ending') return;
          const unconfirmed = { ...callsRef.current, [peerId]: { ...latest, status: 'ended', endUnconfirmed: true } };
          callsRef.current = unconfirmed;
          setCalls(unconfirmed);
          callEndTimersRef.current.delete(peerId);
          reportError(`${peerId} did not confirm that the call ended. Their connection may have been lost.`);
        }, CALL_END_ACK_TIMEOUT_MS);
        callEndTimersRef.current.set(peerId, timer);
      }
      addEvent({
        kind: 'WEBRTC',
        source: identity.id,
        target: peerId,
        text: sent ? `Requested call termination with ${peerId}.` : `Ended the local call with ${peerId}; notification failed.`,
      });
    };
    finishEnd().catch((error) => {
      cleanupCallMedia(peerId);
      reportError(`Call ending failed: ${error.message}`);
      const current = callsRef.current[peerId];
      if (current?.callId === call.callId) {
        const next = { ...callsRef.current, [peerId]: { ...current, status: 'ended', endUnconfirmed: true } };
        callsRef.current = next;
        setCalls(next);
      }
    });
  }, [addEvent, cleanupCallMedia, completeConnectedConsultation, identity.id, reportError, sendPeer]);

  const completeConsultation = useCallback(async (peerId) => {
    const call = callsRef.current[peerId];
    if (!await completeConnectedConsultation(peerId, call)) {
      reportError('A connected video consultation for a pending patient request is required.');
    }
  }, [completeConnectedConsultation, reportError]);

  const simulateNodeFailure = useCallback((peerId) => {
    if (peerId === identity.id) {
      stopNode(true);
      return;
    }
    try {
      sendPeer(peerId, 'SIMULATE_FAILURE', { requestedBy: identity.id });
      addEvent({
        kind: 'LOCAL',
        source: identity.id,
        target: peerId,
        text: `Asked ${peerId} to simulate a node failure over WebRTC.`,
      });
    } catch (error) {
      reportError(error.message);
    }
  }, [addEvent, identity.id, reportError, sendPeer, stopNode]);

  const resetSimulation = useCallback(() => resetLocalState(true), [resetLocalState]);

  return {
    signalingState,
    roster,
    peerStates,
    remoteStreams,
    nodeReports,
    events,
    errors,
    coordinator,
    lastHeartbeatAt,
    localIcuBeds,
    requestResults,
    calls,
    localStream,
    mediaError,
    startNode,
    stopNode: () => stopNode(true),
    startBullyElection,
    requestIcuBed,
    reportError,
    startVideoCall,
    prepareVideoCall,
    retryCallMedia,
    acceptVideoCall,
    rejectVideoCall,
    endVideoCall,
    completeConsultation,
    setMediaTrackEnabled,
    simulateNodeFailure,
    resetSimulation,
  };
}
