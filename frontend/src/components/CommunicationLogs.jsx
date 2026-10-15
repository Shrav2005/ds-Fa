export default function CommunicationLogs({ logs }) {
  return (
    <section className="panel">
      <div className="section-header">
        <h2>Communication Logs</h2>
      </div>

      <div className="log-box">
        {logs && logs.length > 0 ? (
          logs.map((log, index) => (
            <div className="log-line" key={`${log.timestamp}-${index}`}>
              <span>
                {log.timestamp}
                {log.lamport_timestamp !== null && log.lamport_timestamp !== undefined
                  ? ` · L${log.lamport_timestamp} ${log.node_id || ''}`
                  : ''}
                {log.vector_clock && Object.keys(log.vector_clock).length > 0
                  ? ` · V{${Object.entries(log.vector_clock)
                    .sort(([first], [second]) => first.localeCompare(second))
                    .map(([node, counter]) => `${node}:${counter}`)
                    .join(',')}}`
                  : ''}
              </span>
              <p>{log.message}</p>
            </div>
          ))
        ) : (
          <p className="muted-text">No communication logs yet.</p>
        )}
      </div>
    </section>
  );
}
