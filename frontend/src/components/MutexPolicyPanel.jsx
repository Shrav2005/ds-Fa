import { useEffect, useState } from 'react';
import { fetchMutexPolicy, updateMutexPolicy } from '../services/api';

export default function MutexPolicyPanel() {
  const [mutex, setMutex] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const refresh = async () => {
    try {
      setMutex(await fetchMutexPolicy());
      setError('');
    } catch (requestError) {
      setError(requestError.message || 'Could not load reservation-lock status.');
    }
  };

  useEffect(() => {
    refresh();
    const timer = setInterval(refresh, 3000);
    return () => clearInterval(timer);
  }, []);

  const changePolicy = async (event) => {
    const policy = event.target.value;
    setLoading(true);
    setError('');
    try {
      setMutex(await updateMutexPolicy(policy));
    } catch (requestError) {
      setError(requestError.message || 'Could not update the mutual-exclusion policy.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="panel mutex-panel" id="mutex">
      <div className="section-header">
        <div>
          <h2>Reservation Mutual Exclusion</h2>
          <p className="muted-text">The coordinator grants one bed-reservation critical section at a time.</p>
        </div>
        <label className="mutex-policy-select">
          Queue policy
          <select disabled={loading || !mutex} onChange={changePolicy} value={mutex?.policy || 'FAIR'}>
            <option value="FAIR">Fair · Lamport FIFO</option>
            <option value="PRIORITY">Urgency first · may starve normal</option>
          </select>
        </label>
      </div>
      {error && <p className="alert error">{error}</p>}
      {mutex && (
        <>
          <div className="mutex-live-state">
            <span className={`mutex-lock-indicator ${mutex.lock_state.active ? 'locked' : 'free'}`}>
              {mutex.lock_state.active ? 'CRITICAL SECTION OCCUPIED' : 'CRITICAL SECTION FREE'}
            </span>
            <span>{mutex.lock_state.waiting} request(s) waiting</span>
            {mutex.lock_state.active && <span>Active request {mutex.lock_state.active}</span>}
          </div>
          <p className="muted-text mutex-policy-description">{mutex.description}</p>
          <p className="mutex-self-study">
            Lodha–Kshemkalyani is a fair distributed mutual-exclusion algorithm; this live reservation
            path uses a centralized FIFO lock by default. Switch to urgency-first to see the fairness
            trade-off: critical/high waiters may pass normal requests. MongoDB's atomic bed decrement
            remains the final protection against overbooking.
          </p>
        </>
      )}
    </section>
  );
}
