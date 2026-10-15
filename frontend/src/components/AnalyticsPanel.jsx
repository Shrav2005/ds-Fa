import { useEffect, useState } from 'react';
import {
  archiveAnalyticsToHdfs,
  fetchAnalyticsSummary,
  getAnalyticsExportUrl,
} from '../services/api';

function Breakdown({ title, values, emptyText }) {
  const entries = Object.entries(values || {}).sort(([first], [second]) => first.localeCompare(second));
  return (
    <div className="analytics-breakdown">
      <h3>{title}</h3>
      {entries.length ? entries.map(([label, value]) => (
        <div className="analytics-row" key={label}>
          <span>{label}</span><strong>{value}</strong>
        </div>
      )) : <p className="muted-text analytics-empty">{emptyText}</p>}
    </div>
  );
}

export default function AnalyticsPanel() {
  const [report, setReport] = useState(null);
  const [loading, setLoading] = useState(false);
  const [archiving, setArchiving] = useState(false);
  const [archiveMessage, setArchiveMessage] = useState('');
  const [error, setError] = useState('');

  const refresh = async () => {
    setLoading(true);
    setError('');
    try {
      setReport(await fetchAnalyticsSummary());
    } catch {
      setError('The activity report could not be loaded. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  const archiveToHdfs = async () => {
    setArchiving(true);
    setArchiveMessage('');
    try {
      const result = await archiveAnalyticsToHdfs();
      setArchiveMessage(`Archived ${result.record_count} records to ${result.hdfs_path}.`);
    } catch (archiveError) {
      setArchiveMessage(archiveError.message || 'The report could not be archived to HDFS.');
    } finally {
      setArchiving(false);
    }
  };

  return (
    <section className="panel analytics-panel" id="analytics">
      <div className="section-header analytics-heading">
        <div>
          <h2>Activity report</h2>
          <p className="muted-text">
            A summary of recent bed requests and confirmed placements.
          </p>
        </div>
        <div className="analytics-actions">
          <button type="button" className="secondary-button" onClick={refresh} disabled={loading}>
            {loading ? 'Updating...' : 'Refresh report'}
          </button>
          <a className="analytics-export" href={getAnalyticsExportUrl()} download>
            Download report
          </a>
          <button type="button" className="secondary-button" onClick={archiveToHdfs} disabled={archiving}>
            {archiving ? 'Archiving...' : 'Archive to HDFS'}
          </button>
        </div>
      </div>
      {error && <p className="alert error">{error}</p>}
      {archiveMessage && <p className="analytics-archive-status" role="status">{archiveMessage}</p>}
      {!report && loading && <p className="muted-text">Preparing the latest report…</p>}
      {report && (
        <>
          <p className="analytics-source">
            {report.record_count} recent service records · {report.engine}
          </p>
          <div className="analytics-totals">
            <div className="analytics-total"><span>Total requests</span><strong>{report.summary.total_requests}</strong></div>
            <div className="analytics-total"><span>Confirmed reservations</span><strong>{report.summary.total_reservations}</strong></div>
          </div>
          <div className="analytics-grid">
            <Breakdown title="Requests by bed type" values={report.summary.requests_by_bed_type} emptyText="No request records yet." />
            <Breakdown title="Requests by receiving hospital" values={report.summary.requests_by_hospital} emptyText="No requests have been assigned." />
            <Breakdown title="Confirmed beds by hospital" values={report.summary.reservations_by_hospital} emptyText="No confirmed reservations yet." />
            <Breakdown title="Requests by hour" values={report.summary.requests_by_hour} emptyText="No request times yet." />
          </div>
          <p className="analytics-privacy">Exports contain aggregate dimensions only. Patient IDs and names are not included.</p>
        </>
      )}
    </section>
  );
}
