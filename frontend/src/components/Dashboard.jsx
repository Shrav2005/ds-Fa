import { HOSPITALS } from '../services/hospitalProtocol';

function capacityTotal(hospitals, bedType) {
  return hospitals.reduce((total, hospital) => total + Number(hospital.beds?.[bedType] || 0), 0);
}

function serviceLabel(hospital) {
  if (hospital.status !== 'ONLINE') return 'Unavailable';
  if (hospital.rpc_status === 'ONLINE' && hospital.beacon_status === 'ONLINE') return 'Available';
  if (hospital.beacon_status === 'ONLINE') return 'Checking';
  return 'Awaiting update';
}

export default function Dashboard({ statusData, statusError = '' }) {
  if (!statusData) {
    return (
      <section className="panel overview-panel" aria-live="polite">
        <div className="section-header">
          <div>
            <p className="eyebrow">Regional care network</p>
            <h2>Hospital availability</h2>
          </div>
        </div>
        <p className={statusError ? 'alert error' : 'muted-text'} role={statusError ? 'alert' : undefined}>
          {statusError || 'Checking current hospital availability…'}
        </p>
      </section>
    );
  }

  const hospitals = statusData.hospitals || [];
  const capacity = [
    { label: 'ICU beds', key: 'ICU' },
    { label: 'Emergency beds', key: 'Emergency' },
    { label: 'General beds', key: 'General' },
  ];
  const coordinatorAvailable = statusData.coordinator_status === 'ok';

  return (
    <section className="panel overview-panel" aria-label="Hospital availability overview">
      <div className="section-header overview-heading">
        <div className="overview-title">
          <p className="eyebrow">Regional care network</p>
          <h2>Hospital availability</h2>
          <p className="muted-text">Live bed capacity and service status across participating hospitals.</p>
        </div>
        <span className={`overview-status ${coordinatorAvailable ? 'available' : ''}`}>
          <i />
          <span>
            <small>Network status</small>
            <strong>{coordinatorAvailable ? 'Operational' : 'Checking services'}</strong>
          </span>
        </span>
      </div>

      <div className="capacity-summary">
        <div className="capacity-summary-card">
          <span className="capacity-icon" aria-hidden="true">H</span>
          <div>
            <span>Connected hospitals</span>
            <strong>{statusData.online_hospitals || 0}<small> / {hospitals.length}</small></strong>
          </div>
        </div>
        {capacity.map((item) => {
          const icon = item.key === 'ICU' ? 'ICU' : item.key === 'Emergency' ? 'ER' : 'GEN';
          const label = item.key === 'ICU' ? 'Available ICU beds' : `Available ${item.label.toLowerCase()}`;
          return (
            <div className="capacity-summary-card" key={item.key}>
              <span className="capacity-icon" aria-hidden="true">{icon}</span>
              <div>
                <span>{label}</span>
                <strong>{capacityTotal(hospitals, item.key)}</strong>
              </div>
            </div>
          );
        })}
      </div>

      <div className="facility-list">
        {hospitals.map((hospital) => {
          const available = hospital.status === 'ONLINE';
          const name = HOSPITALS.find((item) => item.id === hospital.hospital_id)?.name
            || hospital.hospital_name;
          return (
            <article className={`facility-card ${available ? 'facility-available' : 'facility-unavailable'}`} key={hospital.hospital_id}>
              <div className="facility-heading">
                <div>
                  <span className="facility-symbol" aria-hidden="true">+</span>
                  <div>
                    <h3>{name}</h3>
                    <small>Facility ID · {hospital.hospital_id}</small>
                  </div>
                </div>
                <span className={`facility-pill ${available ? 'available' : 'unavailable'}`}>
                  <i />{available ? 'Available' : 'Unavailable'}
                </span>
              </div>

              <div className="facility-capacity">
                {capacity.map((item) => (
                  <div key={item.key}>
                    <span>{item.label}</span>
                    <strong>{hospital.beds?.[item.key] || 0}</strong>
                  </div>
                ))}
              </div>

              <div className="facility-update">
                <span>Service status</span>
                <strong>{serviceLabel(hospital)}</strong>
              </div>
              {hospital.beacon_status === 'ONLINE' && hospital.heartbeat_age_seconds !== null && hospital.heartbeat_age_seconds !== undefined && (
                <small className="last-update">Last checked {hospital.heartbeat_age_seconds}s ago</small>
              )}
            </article>
          );
        })}
      </div>
    </section>
  );
}
