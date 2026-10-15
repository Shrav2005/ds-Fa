import { useEffect, useState } from 'react';
import Dashboard from './components/Dashboard';
import EmergencyRequestForm from './components/EmergencyRequestForm';
import AllocationResult from './components/AllocationResult';
import HospitalPatientHistory from './components/HospitalPatientHistory';
import HospitalNetworkDashboard from './components/HospitalNetworkDashboard';
import SynchronizationStory from './components/SynchronizationStory';
import useHospitalNetwork from './hooks/useHospitalNetwork';
import { HOSPITALS } from './services/hospitalProtocol';
import { createUuid } from './services/uuid';
import {
  confirmEmergencyRequest,
  cancelEmergencyRequest,
  completeEmergencyConsultation,
  fetchHospitalsStatus,
  submitEmergencyRequest,
} from './services/api';

const IDENTITY_STORAGE_KEY = 'hospital_network_identity';

function HospitalWorkspace({ identity, onLogout }) {
  const userType = identity.role === 'hospital' ? 'hospital' : 'patient';
  const [statusData, setStatusData] = useState(null);
  const [statusError, setStatusError] = useState('');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [pendingConsultation, setPendingConsultation] = useState(null);
  const [consultationComplete, setConsultationComplete] = useState(false);
  const [consultationSubmitting, setConsultationSubmitting] = useState(false);
  const [refreshing, setRefreshing] = useState(false);

  const refreshStatus = async () => {
    try {
      const data = await fetchHospitalsStatus();
      setStatusData(data);
      setStatusError('');
    } catch (error) {
      console.error('Hospital availability refresh failed:', error);
      setStatusError('Hospital availability could not be loaded. Please check the hospital services and try again.');
    }
  };

  const refreshSystem = async () => {
    setRefreshing(true);
    await refreshStatus();
    setRefreshing(false);
  };

  useEffect(() => {
    if (userType !== 'patient') return undefined;
    refreshStatus();

    const timer = setInterval(() => {
      refreshStatus();
    }, 2000);

    return () => clearInterval(timer);
  }, [userType]);

  const handleEmergencyRequest = async (payload) => {
    setLoading(true);
    setConsultationComplete(false);
    try {
      const response = await submitEmergencyRequest(payload);
      setResult(response);
      if (response.success) {
        setPendingConsultation(response);
      } else {
        setPendingConsultation(null);
      }
    } catch (error) {
      setPendingConsultation(null);
      setResult({
        success: false,
        message: error.message || 'We could not send this bed request. Please try again.',
        comparison: [],
      });
    } finally {
      setLoading(false);
    }
  };

  const handleConsultationComplete = async ({ request_id, hospital_id }) => {
    if (
      identity.role !== 'other'
      || pendingConsultation?.request_id !== request_id
      || pendingConsultation?.selected_hospital?.hospital_id !== hospital_id
    ) return;
    setConsultationSubmitting(true);
    try {
      await completeEmergencyConsultation({ request_id, hospital_id });
      setConsultationComplete(true);
      setResult((current) => current ? ({
        ...current,
        message: 'Consultation completed. Confirm to allocate the selected bed.',
      }) : current);
      return true;
    } catch (error) {
      setConsultationComplete(false);
      setResult((current) => current ? ({
        ...current,
        success: false,
        message: error.message || 'The consultation could not be confirmed. Please try again.',
      }) : current);
      return false;
    } finally {
      setConsultationSubmitting(false);
    }
  };

  const network = useHospitalNetwork(identity, handleConsultationComplete);

  const handleConfirmReservation = async () => {
    if (!pendingConsultation || !consultationComplete || loading) return;
    setLoading(true);
    try {
      const response = await confirmEmergencyRequest({
        request_id: pendingConsultation.request_id,
      });
      setResult(response);
      if (response.success) {
        setResult({
          ...response,
          message: `Seat allocated at ${response.selected_hospital.hospital_name}.`,
        });
        setPendingConsultation(null);
        setConsultationComplete(false);
        await refreshStatus();
      } else {
        setResult({
          ...pendingConsultation,
          ...response,
          status: 'CONSULTATION_REQUIRED',
        });
      }
    } catch (error) {
      setResult({
        ...pendingConsultation,
        success: false,
        status: 'CONSULTATION_REQUIRED',
        message: error.message || 'The bed could not be reserved. Please try again.',
      });
    } finally {
      setLoading(false);
    }
  };

  const handleCancelConsultation = async () => {
    if (!pendingConsultation) return;
    setLoading(true);
    try {
      await cancelEmergencyRequest({ request_id: pendingConsultation.request_id });
      const hospitalId = pendingConsultation.selected_hospital?.hospital_id;
      if (hospitalId && network.calls[hospitalId]) {
        network.endVideoCall(hospitalId);
      }
      setPendingConsultation(null);
      setConsultationComplete(false);
      setResult({
        success: true,
        status: 'CANCELLED',
        message: 'Bed request cancelled. No bed was reserved.',
      });
    } catch {
      setResult({
        ...pendingConsultation,
        success: false,
        message: 'This request could not be cancelled. Please try again.',
      });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-icon">+</div>
          <div>
            <h1>Emergency Care</h1>
            <small>Bed Coordination Centre</small>
          </div>
        </div>

        <nav className="nav-links">
          {userType === 'patient' && <a href="#dashboard">Hospital availability</a>}
          <a href="#calls">Hospitals &amp; staff</a>
          <a href="#synchronization">Hospital coordination story</a>
          {userType === 'patient' && <a href="#request">Bed request</a>}
          {userType === 'hospital' && <a href="#patient-history">Patient history</a>}
        </nav>
      </aside>

      <main className="main-content">
        <header className="top-bar">
          <div>
            <p className="eyebrow">{userType === 'hospital' ? 'Hospital workspace' : 'Patient care'}</p>
            <h2>{userType === 'hospital' ? 'Hospital operations' : 'Find a hospital bed'}</h2>
          </div>
          <div className="top-actions">
            {userType === 'patient' && (
              <button type="button" className="secondary-button" onClick={refreshSystem} disabled={refreshing}>
                {refreshing ? 'Updating…' : 'Refresh availability'}
              </button>
            )}
            <span className="identity-chip">{identity.name} · {identity.role === 'hospital' ? identity.hospitalId : 'Patient'}</span>
            <button
              type="button"
              className="secondary-button"
              onClick={() => {
                network.stopNode();
                onLogout();
              }}
            >
              Log out
            </button>
            {userType === 'patient' && (
              <div className={`top-status ${statusData?.coordinator_status === 'ok' ? 'available' : statusError ? 'unavailable' : 'checking'}`}>
                <span className="live-dot" />
                {statusData?.coordinator_status === 'ok' ? 'Care desk available' : statusError ? 'Care desk unavailable' : 'Checking care desk'}
              </div>
            )}
          </div>
        </header>

        {userType === 'patient' && (
          <section id="dashboard">
            <Dashboard statusData={statusData} statusError={statusError} />
          </section>
        )}

        <HospitalNetworkDashboard
          identity={identity}
          network={network}
          consultation={pendingConsultation}
        />

        {userType === 'patient' ? (
          <div className="main-grid">
            <section id="request">
              <EmergencyRequestForm
                onSubmit={handleEmergencyRequest}
                loading={loading}
                disabled={Boolean(pendingConsultation)}
                patientName={identity.name}
              />
            </section>

            <section id="result">
              <AllocationResult
                result={result}
                onClear={() => setResult(null)}
                consultationComplete={consultationComplete}
                confirming={loading || consultationSubmitting}
                onConfirmReservation={handleConfirmReservation}
                onCancelConsultation={handleCancelConsultation}
              />
            </section>
          </div>
        ) : (
          <HospitalPatientHistory hospitalId={identity.hospitalId} />
        )}

        <SynchronizationStory />

      </main>
    </div>
  );
}

function readSavedIdentity() {
  try {
    const saved = sessionStorage.getItem(IDENTITY_STORAGE_KEY);
    if (!saved) return null;
    const identity = JSON.parse(saved);
    if (identity.role === 'hospital' && HOSPITALS.some((hospital) => hospital.id === identity.hospitalId)) {
      return identity;
    }
    if (
      identity.role === 'other'
      && /^OTHER-[0-9a-f-]{36}$/i.test(identity.id)
      && HOSPITALS.some((hospital) => hospital.id === identity.targetHospitalId)
    ) return identity;
  } catch {
    return null;
  }
  return null;
}

function LoginGate({ onLogin }) {
  const [role, setRole] = useState('hospital');
  const [hospitalId, setHospitalId] = useState(HOSPITALS[0].id);
  const [name, setName] = useState('');
  const [error, setError] = useState('');

  const submit = (event) => {
    event.preventDefault();
    const displayName = name.trim();
    if (!displayName) {
      setError('Enter your name to continue.');
      return;
    }
    const hospital = HOSPITALS.find((item) => item.id === hospitalId);
    const identity = role === 'hospital'
      ? {
          id: hospital.id,
          role,
          hospitalId: hospital.id,
          name: displayName,
          initialIcuBeds: hospital.initialIcuBeds,
        }
      : {
          id: `OTHER-${createUuid()}`,
          role: 'other',
          targetHospitalId: HOSPITALS[0].id,
          name: displayName,
        };
    onLogin(identity);
  };

  return (
    <main className="login-page">
      <section className="login-card">
        <div className="brand-icon">+</div>
        <p className="eyebrow">Hospital care coordination</p>
        <h1>Welcome to the care desk</h1>
        <p className="muted-text">Choose Patient Login to find a bed, or Hospital Login to manage your patient history.</p>
        <form onSubmit={submit}>
          <label htmlFor="login-role">Login as</label>
          <select id="login-role" value={role} onChange={(event) => setRole(event.target.value)}>
            <option value="other">Patient Login</option>
            <option value="hospital">Hospital Login</option>
          </select>
          <label htmlFor="login-name">Your name</label>
          <input id="login-name" value={name} onChange={(event) => setName(event.target.value)} placeholder={role === 'hospital' ? 'Staff name' : 'Patient name'} />
          {role === 'hospital' ? (
            <>
              <label htmlFor="login-hospital">Hospital</label>
              <select id="login-hospital" value={hospitalId} onChange={(event) => setHospitalId(event.target.value)}>
                {HOSPITALS.map((hospital) => <option value={hospital.id} key={hospital.id}>{hospital.name} ({hospital.id})</option>)}
              </select>
            </>
          ) : null}
          {error && <p className="network-error-text">{error}</p>}
          <button className="primary-button" type="submit">Continue</button>
        </form>
        <small>Training system. Use fictional patient details only.</small>
      </section>
    </main>
  );
}

export default function App() {
  const [identity, setIdentity] = useState(readSavedIdentity);

  const logIn = (nextIdentity) => {
    sessionStorage.setItem(IDENTITY_STORAGE_KEY, JSON.stringify(nextIdentity));
    setIdentity(nextIdentity);
  };

  const logOut = () => {
    sessionStorage.removeItem(IDENTITY_STORAGE_KEY);
    setIdentity(null);
  };

  return identity
    ? <HospitalWorkspace key={identity.id} identity={identity} onLogout={logOut} />
    : <LoginGate onLogin={logIn} />;
}
