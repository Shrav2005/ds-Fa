const API_BASE_URL = import.meta.env.VITE_API_BASE_URL
  || window.location.origin;

async function callApi(path, options) {
  const response = await fetch(`${API_BASE_URL}${path}`, options);
  let data;
  try {
    data = await response.json();
  } catch {
    throw new Error(`The coordinator returned an invalid response (HTTP ${response.status}).`);
  }
  if (!response.ok) {
    throw new Error(data.message || `The coordinator request failed (HTTP ${response.status}).`);
  }
  return data;
}

function jsonOptions(payload) {
  return {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  };
}

export async function fetchHospitalsStatus() {
  const response = await fetch(`${API_BASE_URL}/api/hospitals/status`);
  if (!response.ok) {
    throw new Error(`Could not load hospital status (HTTP ${response.status}).`);
  }

  const data = await response.json();
  if (!Array.isArray(data.hospitals)) {
    throw new Error('The coordinator returned an invalid hospital status response.');
  }
  return data;
}

export function fetchGlobalStateSnapshots() {
  return callApi('/api/global-state/snapshots');
}

export function fetchElectionStatus() {
  return callApi('/api/election/status');
}

export function runBerkeleyClockSync() {
  return callApi('/api/clocks/berkeley-sync', jsonOptions({}));
}

export function startBullyElection(payload = {}) {
  return callApi('/api/election/start', jsonOptions(payload));
}

export function startRingElection(payload = {}) {
  return callApi('/api/election/ring-start', jsonOptions(payload));
}

export function simulateTransferDeadlock() {
  return callApi('/api/deadlock/demo', jsonOptions({}));
}

export function fetchMutexPolicy() {
  return callApi('/api/mutex/policy');
}

export function updateMutexPolicy(policy) {
  return callApi('/api/mutex/policy', jsonOptions({ policy }));
}

export function fetchAnalyticsSummary() {
  return callApi('/api/analytics/summary');
}

export function getAnalyticsExportUrl() {
  return `${API_BASE_URL}/api/analytics/export`;
}

export function archiveAnalyticsToHdfs() {
  return callApi('/api/analytics/hdfs/export', jsonOptions({}));
}

export function captureGlobalStateSnapshot() {
  return callApi('/api/global-state/snapshots', { method: 'POST' });
}

export async function fetchLogs() {
  return callApi('/api/logs');
}

export async function submitEmergencyRequest(payload) {
  return callApi('/api/emergency/plan', jsonOptions(payload));
}

export function confirmEmergencyRequest(payload) {
  return callApi('/api/emergency/confirm', jsonOptions(payload));
}

export function cancelEmergencyRequest(payload) {
  return callApi('/api/emergency/cancel', jsonOptions(payload));
}

export function completeEmergencyConsultation(payload) {
  return callApi('/api/emergency/consultation-complete', jsonOptions(payload));
}

export async function fetchHospitalPatients(hospitalId) {
  const data = await callApi(`/api/hospitals/${encodeURIComponent(hospitalId)}/patients`);
  if (!Array.isArray(data.patients)) {
    throw new Error('The coordinator returned an invalid patient history response.');
  }
  return data;
}

export function dischargePatient(payload) {
  return callApi('/api/bed/deallocate', jsonOptions(payload));
}
