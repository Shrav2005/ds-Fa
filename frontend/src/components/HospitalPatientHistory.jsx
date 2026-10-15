import { useCallback, useEffect, useState } from 'react';
import { dischargePatient, fetchHospitalPatients } from '../services/api';

function formatDate(value) {
  if (!value) return 'Date unavailable';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? 'Date unavailable' : date.toLocaleString();
}

export default function HospitalPatientHistory({ hospitalId }) {
  const [patients, setPatients] = useState([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  const [discharging, setDischarging] = useState('');
  const [notice, setNotice] = useState('');

  const refreshPatients = useCallback(async () => {
    try {
      const response = await fetchHospitalPatients(hospitalId);
      setPatients(response.patients);
      setError('');
    } catch (requestError) {
      setError(requestError.message || 'Patient history could not be loaded.');
    } finally {
      setLoading(false);
    }
  }, [hospitalId]);

  useEffect(() => {
    refreshPatients();
    const timer = window.setInterval(refreshPatients, 10_000);
    return () => window.clearInterval(timer);
  }, [refreshPatients]);

  const handleDischarge = async (patient) => {
    if (!window.confirm(`Discharge ${patient.patient_name || patient.patient_id} and release their ${patient.bed_type} bed?`)) {
      return;
    }
    setDischarging(patient.request_id);
    setNotice('');
    try {
      const response = await dischargePatient({
        hospital_id: hospitalId,
        request_id: patient.request_id,
      });
      setNotice(response.message);
      await refreshPatients();
    } catch (requestError) {
      setError(requestError.message || 'The patient could not be discharged.');
    } finally {
      setDischarging('');
    }
  };

  return (
    <section id="patient-history" className="panel">
      <div className="section-header">
        <div>
          <p className="eyebrow">Hospital-only view</p>
          <h2>Patient history</h2>
        </div>
        <button type="button" className="secondary-button" onClick={refreshPatients} disabled={loading}>
          Refresh history
        </button>
      </div>
      <p className="muted-text">Confirmed placements at your hospital. Discharging a patient releases their bed and keeps the record in history.</p>
      {error && <p className="network-error-text" role="alert">{error}</p>}
      {notice && <p className="alert success" role="status">{notice}</p>}
      {loading ? (
        <p className="muted-text">Loading patient history…</p>
      ) : patients.length === 0 ? (
        <p className="muted-text">No patient placements are recorded for this hospital.</p>
      ) : (
        <div className="allocation-list">
          {patients.map((patient) => (
            <article className="allocation-item" key={patient.request_id}>
              <div>
                <strong>{patient.patient_name || 'Patient'}</strong>
                <div className="allocation-meta">
                  <span>{patient.patient_id}</span>
                  <span>{patient.bed_type} bed</span>
                  <span>{patient.priority} priority</span>
                  <span>{formatDate(patient.created_at)}</span>
                  <span>{patient.status === 'DISCHARGED' ? 'Discharged' : patient.status === 'DISCHARGING' ? 'Discharging' : 'Admitted'}</span>
                </div>
              </div>
              {patient.status === 'COMPLETED' && (
                <button
                  className="small-button"
                  type="button"
                  disabled={Boolean(discharging)}
                  onClick={() => handleDischarge(patient)}
                >
                  {discharging === patient.request_id ? 'Discharging…' : 'Discharge patient'}
                </button>
              )}
            </article>
          ))}
        </div>
      )}
    </section>
  );
}
