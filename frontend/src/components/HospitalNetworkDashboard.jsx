import { useEffect, useMemo, useRef, useState } from 'react';
import { HOSPITALS } from '../services/hospitalProtocol';

function VideoTile({ label, stream, muted = false }) {
  const videoRef = useRef(null);
  const [playbackBlocked, setPlaybackBlocked] = useState(false);
  useEffect(() => {
    if (videoRef.current) {
      videoRef.current.srcObject = stream || null;
      if (stream) {
        videoRef.current.play()
          .then(() => setPlaybackBlocked(false))
          .catch(() => setPlaybackBlocked(!muted));
      }
    }
    return () => {
      if (videoRef.current) videoRef.current.srcObject = null;
    };
  }, [muted, stream]);

  return (
    <div className="hospital-video-tile">
      <video ref={videoRef} autoPlay playsInline muted={muted} controls={!muted} />
      <span>{label}</span>
      {playbackBlocked && (
        <button type="button" className="video-playback-button" onClick={() => {
          videoRef.current?.play()
            .then(() => setPlaybackBlocked(false))
            .catch(() => setPlaybackBlocked(true));
        }}>
          Enable remote audio
        </button>
      )}
    </div>
  );
}

function nodeIsOnline(roster, hospitalId) {
  return roster.some((peer) => peer.role === 'hospital' && peer.hospitalId === hospitalId);
}

function facilityName(peerId) {
  return HOSPITALS.find((hospital) => hospital.id === peerId)?.name
    || (peerId?.startsWith('OTHER-') ? 'Patient' : peerId);
}

function friendlyError(message) {
  if (/camera|microphone|media|permission/i.test(message)) {
    return 'Camera or microphone access is unavailable. Check browser permissions and try again.';
  }
  if (/signaling|websocket|peer|data channel|webrtc|ice|sdp/i.test(message)) {
    return 'Could not connect to the hospital. Check that both hospitals are online and try again.';
  }
  return 'This action could not be completed. Please try again.';
}

export default function HospitalNetworkDashboard({ identity, network, consultation = null }) {
  const attemptedConsultationRef = useRef(null);
  const targetHospital = identity.role === 'other'
    ? consultation?.selected_hospital?.hospital_id || identity.targetHospitalId
    : '';

  const hospitalRows = useMemo(() => HOSPITALS.map((hospital) => {
    const ownNode = identity.hospitalId === hospital.id;
    const report = ownNode
      ? {
          name: identity.name,
          icuBeds: network.localIcuBeds,
          online: network.signalingState === 'connected',
          role: network.coordinator.leaderId === hospital.id ? 'On-duty lead' : 'Hospital team',
          coordinatorId: network.coordinator.leaderId,
        }
      : network.nodeReports[hospital.id];
    const isOnline = nodeIsOnline(network.roster, hospital.id);
    const state = ownNode ? {
      connectionState: network.signalingState === 'connected' ? 'connected' : network.signalingState,
      dataChannelState: Object.values(network.peerStates).some(
        (peer) => peer.connectionState === 'connected' && peer.dataChannelState === 'open',
      ) ? 'open' : 'waiting',
    } : network.peerStates[hospital.id];

    return {
      ...hospital,
      ownNode,
      name: hospital.name,
      online: ownNode ? network.signalingState === 'connected' : report?.online ?? isOnline,
      reportedOnline: report?.online,
      icuBeds: ownNode ? network.localIcuBeds : report?.icuBeds ?? hospital.initialIcuBeds,
      role: report?.role === 'Coordinator' || network.coordinator.leaderId === hospital.id
        ? 'On-duty lead'
        : 'Hospital team',
      connectionState: state?.connectionState || 'not connected',
      dataChannelState: state?.dataChannelState || 'closed',
    };
  }), [identity, network]);

  const calls = Object.entries(network.calls);
  const otherPeers = network.roster.filter((peer) => peer.role === 'other');
  const audioTrack = network.localStream?.getAudioTracks().find((track) => track.readyState === 'live');
  const videoTrack = network.localStream?.getVideoTracks().find((track) => track.readyState === 'live');
  const edgeIsConnected = (leftId, rightId) => {
    if (identity.role !== 'hospital') return false;
    const peerId = identity.id === leftId ? rightId : identity.id === rightId ? leftId : null;
    const state = peerId ? network.peerStates[peerId] : null;
    return state?.connectionState === 'connected' && state.dataChannelState === 'open';
  };

  useEffect(() => {
    if (!consultation) {
      attemptedConsultationRef.current = null;
      return;
    }
    if (
      identity.role !== 'other'
      || targetHospital !== consultation.selected_hospital?.hospital_id
      || network.signalingState !== 'connected'
    ) return;
    const consultationKey = `${consultation.request_id}:${targetHospital}`;
    if (attemptedConsultationRef.current === consultationKey) return;
    const existingCall = network.calls[targetHospital];
    if (existingCall) {
      if (existingCall.status === 'ended') attemptedConsultationRef.current = consultationKey;
      return;
    }
    const peer = network.peerStates[targetHospital];
    if (peer?.connectionState !== 'connected' || peer.dataChannelState !== 'open') return;
    attemptedConsultationRef.current = consultationKey;
    network.startVideoCall(targetHospital, {
      request_id: consultation.request_id,
      hospital_id: targetHospital,
    });
  }, [consultation, identity.role, network, targetHospital]);

  return (
    <section id="calls" className="panel network-panel">
      <header className="section-header network-header">
        <div>
              <p className="eyebrow">Care coordination</p>
              <h2>{identity.role === 'hospital' ? 'Hospital staff network' : 'Patient care connection'}</h2>
              <p>{identity.role === 'hospital'
                ? 'Connect with patients who have a placement at your hospital.'
                : 'A secure video consultation starts when a hospital is selected for your request.'}</p>
        </div>
        <div className={`network-state ${network.signalingState === 'connected' ? 'online' : ''}`}>
          <span className="live-dot" />
              {identity.role === 'hospital' ? 'Your hospital' : 'Your patient session'} is {network.signalingState === 'connected' ? 'online' : 'offline'}
        </div>
      </header>

      <div className="network-toolbar">
        {network.signalingState === 'connected' ? (
          <button type="button" className="secondary-button" onClick={network.stopNode}>Go offline</button>
        ) : (
          <button type="button" className="primary-button" onClick={network.startNode}>Go online</button>
        )}
        {identity.role === 'hospital' && (
          <>
            <button
              type="button"
              className="secondary-button"
              onClick={() => network.startBullyElection()}
              disabled={network.signalingState !== 'connected'}
            >
              Refresh care desk lead
            </button>
          </>
        )}
      </div>

      {identity.role !== 'hospital' && <div className="network-map" aria-label="Hospital availability">
        <svg className="network-links" viewBox="0 0 1000 320" preserveAspectRatio="none" aria-hidden="true">
          <line x1="200" y1="130" x2="500" y2="230" className={edgeIsConnected('H001', 'H002') ? 'link-live' : ''} />
          <line x1="800" y1="130" x2="500" y2="230" className={edgeIsConnected('H002', 'H003') ? 'link-live' : ''} />
          <line x1="200" y1="130" x2="800" y2="130" className={edgeIsConnected('H001', 'H003') ? 'link-live' : ''} />
        </svg>
        <div className="network-node-row">
          {hospitalRows.map((hospital) => {
            const connection = network.peerStates[hospital.id];
            const isConnected = hospital.ownNode
              ? network.signalingState === 'connected'
              : connection?.connectionState === 'connected' && connection.dataChannelState === 'open';
            return (
              <article className={`network-node ${hospital.online ? 'node-online' : 'node-offline'} ${isConnected ? 'node-connected' : ''}`} key={hospital.id}>
                <div className="node-mark" aria-hidden="true">+</div>
                <strong>{hospital.name}</strong>
                <span>{hospital.id}{hospital.ownNode ? ' · You' : ''}</span>
                <small>{hospital.role}</small>
                <span className={`node-status ${hospital.online ? 'status-online' : 'status-offline'}`} role="status">
                  <i /> {hospital.online ? 'Available' : 'Unavailable'}
                </span>
                <div className="node-beds"><b>{hospital.icuBeds}</b> ICU beds</div>
                <small>{isConnected ? 'Staff contact available' : 'Staff contact unavailable'}</small>
                {network.coordinator.leaderId === hospital.id && <span className="leader-badge">Duty lead</span>}
                {!hospital.ownNode && identity.role === 'hospital' && (
                  <div className="node-actions">
                    <button
                      type="button"
                      onClick={() => network.requestIcuBed(hospital.id)}
                      disabled={!isConnected}
                    >
                      Request ICU bed
                    </button>
                    <button
                      type="button"
                      onClick={() => network.startVideoCall(hospital.id)}
                      disabled={!isConnected || Boolean(network.calls[hospital.id]?.status && network.calls[hospital.id]?.status !== 'ended')}
                    >
                      Call staff
                    </button>
                    <button
                      type="button"
                      onClick={() => network.simulateNodeFailure(hospital.id)}
                      disabled={!isConnected}
                    >
                      Mark unavailable
                    </button>
                  </div>
                )}
              </article>
            );
          })}
        </div>
        {identity.role === 'hospital' && network.coordinator.leaderId && (
          <div className="coordinator-banner">
            On-duty lead: <strong>{facilityName(network.coordinator.leaderId)}</strong>
            <span>{network.lastHeartbeatAt ? 'Service checks are active' : 'Waiting for a service check'}</span>
          </div>
        )}
      </div>}

      {(network.mediaError || network.errors.length > 0) && (
        <div className="network-errors">
          {network.mediaError && <p>{friendlyError(network.mediaError)}</p>}
          {network.errors.map((error) => (
            <p key={error.id}>{friendlyError(error.message)} <small>{error.message}</small></p>
          ))}
        </div>
      )}

      {calls.length > 0 && (
        <div className="video-call-grid">
          {calls.map(([peerId, call]) => (
            <article className="network-control-card video-call-card" key={peerId}>
              <div className="section-header">
                <div>
                  <h3>Video consultation with {facilityName(peerId)}</h3>
                  <p>{call.status === 'connected'
                    ? 'Connected'
                    : call.status === 'ending'
                      ? 'Ending call…'
                      : call.status === 'ended'
                        ? call.context?.request_id && !call.consultationCompleted
                          ? 'Call ended before consultation was completed'
                          : 'Call ended'
                        : call.status === 'incoming'
                          ? 'Incoming call'
                          : call.status === 'media-error'
                            ? 'Media permission is needed to continue'
                            : call.status === 'connecting'
                              ? 'Connecting…'
                              : 'Calling…'}</p>
                </div>
                {call.status === 'incoming' && (
                  <div className="network-toolbar">
                    <button type="button" className="primary-button" onClick={() => network.acceptVideoCall(peerId)}>Accept</button>
                    <button type="button" className="secondary-button" onClick={() => network.rejectVideoCall(peerId)}>Reject</button>
                  </div>
                )}
              </div>
              {call.status === 'calling' && (
                <>
                  <div className="network-toolbar">
                    <button
                      type="button"
                      className="primary-button"
                      onClick={() => network.prepareVideoCall().catch((error) => network.reportError(error.message))}
                    >
                      {network.localStream ? 'Camera and microphone ready' : 'Enable camera and microphone'}
                    </button>
                    <span>Waiting for the hospital to accept.</span>
                  </div>
                  {network.localStream && <div className="video-tiles"><VideoTile label="You" stream={network.localStream} muted /></div>}
                </>
              )}
              {call.status === 'connecting' && <p>Waiting for the other person to join…</p>}
              {call.status === 'media-error' && (
                <button type="button" className="primary-button" onClick={() => network.retryCallMedia(peerId)}>
                  Retry camera and microphone
                </button>
              )}
              {call.status === 'connected' && (
                <>
                  <div className="video-tiles">
                    <VideoTile label="You" stream={network.localStream} muted />
                    <VideoTile label={facilityName(peerId)} stream={network.remoteStreams[peerId]} />
                  </div>
                  <div className="network-toolbar">
                    <button
                      type="button"
                      className="secondary-button"
                      disabled={!audioTrack}
                      onClick={() => network.setMediaTrackEnabled('audio', !audioTrack?.enabled)}
                    >
                      {audioTrack?.enabled ? 'Mute microphone' : 'Unmute microphone'}
                    </button>
                    <button
                      type="button"
                      className="secondary-button"
                      disabled={!videoTrack}
                      onClick={() => network.setMediaTrackEnabled('video', !videoTrack?.enabled)}
                    >
                      {videoTrack?.enabled ? 'Turn camera off' : 'Turn camera on'}
                    </button>
                  </div>
                  {call.context?.request_id && (
                    <button type="button" className="primary-button" onClick={() => network.completeConsultation(peerId)}>
                      Complete consultation
                    </button>
                  )}
                </>
              )}
              {call.status !== 'ended' && (
                <button
                  type="button"
                  className="secondary-button"
                  disabled={call.status === 'ending'}
                  onClick={() => call.status === 'incoming' ? network.rejectVideoCall(peerId) : network.endVideoCall(peerId)}
                >
                  {call.status === 'incoming' ? 'Decline call' : 'End call'}
                </button>
              )}
              {call.status === 'ended' && (
                <p className={`alert ${call.endUnconfirmed ? 'error' : 'success'}`} role="status">
                  {call.endUnconfirmed
                    ? 'Call ended on this device, but the other participant could not be reached.'
                    : call.context?.request_id && !call.consultationCompleted
                      ? 'Call ended. The consultation was not completed.'
                      : 'Call completed.'}
                </p>
              )}
            </article>
          ))}
        </div>
      )}

      {identity.role === 'hospital' && otherPeers.length > 0 && (
        <div className="network-control-card">
          <h3>Connected patients</h3>
          {otherPeers.map((peer) => (
            <div className="other-user-row" key={peer.id}>
              <span>{peer.name}</span>
              <button type="button" className="secondary-button" onClick={() => network.startVideoCall(peer.id)}>
                Video call
              </button>
            </div>
          ))}
        </div>
      )}

    </section>
  );
}
