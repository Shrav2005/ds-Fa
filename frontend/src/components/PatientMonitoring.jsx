import { useEffect, useState } from 'react';

const heartRates = [88, 89, 91, 90, 92, 90];
const spO2Values = [97, 97, 96, 98, 97, 98];
const temperatures = [37.0, 37.1, 37.1, 37.0, 36.9, 37.2];

export default function PatientMonitoring() {
  const [index, setIndex] = useState(0);

  useEffect(() => {
    const timer = setInterval(() => {
      setIndex((current) => (current + 1) % heartRates.length);
    }, 1500);

    return () => clearInterval(timer);
  }, []);

  return (
    <section className="panel">
      <div className="section-header">
        <h2>Simulated Patient Monitoring Stream</h2>
      </div>

      <div className="monitoring-grid">
        <div className="monitor-box">
          <span>Heart Rate</span>
          <strong>{heartRates[index]} bpm</strong>
        </div>
        <div className="monitor-box">
          <span>SpO2</span>
          <strong>{spO2Values[index]}%</strong>
        </div>
        <div className="monitor-box">
          <span>Temperature</span>
          <strong>{temperatures[index].toFixed(1)}°C</strong>
        </div>
      </div>
    </section>
  );
}
