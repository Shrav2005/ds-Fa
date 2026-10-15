import { useState } from 'react';
import { simulateTransferDeadlock } from '../services/api';

function graphLayout(analysis) {
  const nodes = [...new Set(
    analysis.transactions.flatMap((transaction) => [
      transaction.transaction_id,
      ...analysis.wait_for_graph
        .filter((edge) => edge.from === transaction.transaction_id)
        .map((edge) => edge.to),
    ]),
  )];
  const center = { x: 250, y: 145 };
  const radius = nodes.length > 2 ? 105 : 78;
  const positions = new Map(nodes.map((node, index) => {
    const angle = -Math.PI / 2 + (2 * Math.PI * index) / Math.max(nodes.length, 1);
    return [node, {
      x: center.x + radius * Math.cos(angle),
      y: center.y + radius * Math.sin(angle),
    }];
  }));
  return { nodes, positions, center };
}

function WaitForGraph({ analysis }) {
  const { nodes, positions, center } = graphLayout(analysis);
  return (
    <svg className="wait-for-graph" viewBox="0 0 500 290" role="img" aria-label="Wait-for graph of hospital transfer transactions">
      <defs>
        <marker id="wait-arrow" markerWidth="9" markerHeight="9" refX="7" refY="4.5" orient="auto">
          <path d="M0,0 L9,4.5 L0,9 z" fill="#bd744b" />
        </marker>
        <marker id="released-arrow" markerWidth="9" markerHeight="9" refX="7" refY="4.5" orient="auto">
          <path d="M0,0 L9,4.5 L0,9 z" fill="#57876b" />
        </marker>
      </defs>
      {analysis.wait_for_graph.map((edge, index) => {
        const start = positions.get(edge.from);
        const end = positions.get(edge.to);
        if (!start || !end) return null;
        const dx = end.x - start.x;
        const dy = end.y - start.y;
        const length = Math.hypot(dx, dy) || 1;
        const offset = analysis.wait_for_graph.some(
          (candidate) => candidate.from === edge.to && candidate.to === edge.from,
        ) ? (edge.from < edge.to ? -28 : 28) : 0;
        const controlX = (start.x + end.x) / 2 - (dy / length) * offset;
        const controlY = (start.y + end.y) / 2 + (dx / length) * offset;
        const color = edge.status === 'RESOLVED' ? '#57876b' : '#bd744b';
        return (
          <g key={`${edge.from}-${edge.to}-${index}`}>
            <path
              d={`M ${start.x} ${start.y} Q ${controlX} ${controlY} ${end.x} ${end.y}`}
              fill="none"
              markerEnd={edge.status === 'RESOLVED' ? 'url(#released-arrow)' : 'url(#wait-arrow)'}
              stroke={color}
              strokeWidth="2.5"
            />
            <text x={controlX} y={controlY - 7} className="wait-for-edge-label">
              {edge.status === 'RESOLVED' ? 'released' : 'waits for'}
            </text>
          </g>
        );
      })}
      {nodes.map((node) => {
        const position = positions.get(node) || center;
        const transaction = analysis.transactions.find((entry) => entry.transaction_id === node);
        const isVictim = transaction?.status === 'ABORTED';
        return (
          <g key={node}>
            <circle
              cx={position.x}
              cy={position.y}
              fill={isVictim ? '#f8e9e5' : '#eef4e9'}
              r="32"
              stroke={isVictim ? '#b96955' : '#7c9978'}
              strokeWidth="2"
            />
            <text x={position.x} y={position.y - 2} className="wait-for-node-label">{node.replace('TRANSFER-', 'T')}</text>
            <text x={position.x} y={position.y + 14} className="wait-for-clock-label">
              {transaction ? `L${transaction.lamport_timestamp}` : ''}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

export default function DeadlockPanel() {
  const [analysis, setAnalysis] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const runDemo = async () => {
    setLoading(true);
    setError('');
    try {
      setAnalysis(await simulateTransferDeadlock());
    } catch (requestError) {
      setError(requestError.message || 'Could not run the transfer deadlock demonstration.');
    } finally {
      setLoading(false);
    }
  };

  const resolvedEdges = analysis?.wait_for_graph.map((edge) => ({
    ...edge,
    status: analysis.victim
      && (edge.from === analysis.victim.transaction_id || edge.to === analysis.victim.transaction_id)
      ? 'RESOLVED'
      : 'WAITING',
  })) || [];

  return (
    <section className="panel deadlock-panel" id="deadlock">
      <div className="section-header">
        <div>
          <h2>Transfer Deadlock Detector</h2>
          <p className="muted-text">Distributed edge-chasing · Knapp classification: distributed detection</p>
        </div>
        <button type="button" onClick={runDemo} disabled={loading}>
          {loading ? 'Tracing probes...' : 'Simulate transfer deadlock'}
        </button>
      </div>
      <p className="deadlock-intro">
        Two simulated transfers acquire ICU capacity in opposite hospital order. The coordinator sends
        Chandy–Misra–Haas probes through the wait-for graph, detects the returning probe, and aborts
        the youngest transaction by Lamport timestamp. No real bed count is changed.
      </p>
      {error && <p className="alert error">{error}</p>}
      {!analysis ? (
        <p className="muted-text">Run the scenario to build the wait-for graph and follow the distributed probes.</p>
      ) : (
        <>
          <div className={`deadlock-outcome ${analysis.deadlock_detected ? 'detected' : 'clear'}`}>
            {analysis.deadlock_detected
              ? `Deadlock detected. ${analysis.victim.transaction_id} (L${analysis.victim.lamport_timestamp}) was selected as the youngest victim.`
              : 'No deadlock detected.'}
          </div>
          <WaitForGraph analysis={{ ...analysis, wait_for_graph: resolvedEdges }} />
          <div className="deadlock-transactions">
            {analysis.transactions.map((transaction) => (
              <div className={`deadlock-transaction ${transaction.status.toLowerCase()}`} key={transaction.transaction_id}>
                <strong>{transaction.transaction_id.replace('TRANSFER-', 'Transfer ')}</strong>
                <span>L{transaction.lamport_timestamp} · {transaction.status}</span>
                <small>Holds {transaction.holds.join(', ')} · waits for {transaction.waits_for.join(', ')}</small>
              </div>
            ))}
          </div>
          <details className="probe-trace">
            <summary>Edge-chasing probe trace · {analysis.probes.length} messages</summary>
            <ol>
              {analysis.probes.map((probe, index) => (
                <li key={`${probe.initiator}-${probe.sender}-${probe.receiver}-${index}`}>
                  ({probe.initiator}, {probe.sender}, {probe.receiver})
                  {probe.receiver === probe.initiator && ' · probe returned to initiator'}
                </li>
              ))}
            </ol>
          </details>
        </>
      )}
    </section>
  );
}
