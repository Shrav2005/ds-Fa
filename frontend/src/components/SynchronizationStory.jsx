import { useEffect, useState } from 'react';
import { SYNCHRONIZATION_STORY_STEPS } from '../services/synchronizationStory.js';

const STORY_STEP_MS = 5000;
const HOSPITALS = [
  { id: 'H001', name: 'Hospital A' },
  { id: 'H002', name: 'Hospital B' },
  { id: 'H003', name: 'Hospital C' },
];

function HospitalCard({ hospital, status, detail, marker }) {
  return (
    <article className={`story-hospital ${status}`}>
      <span className="story-hospital-icon" aria-hidden="true">+</span>
      <strong>{hospital.name}</strong>
      <small>{hospital.id}</small>
      {marker && <span className="story-marker">{marker}</span>}
      <span className="story-hospital-detail">{detail}</span>
    </article>
  );
}

function StoryVisual({ scene }) {
  if (scene === 'order') {
    return (
      <div className="story-order-flow" role="img" aria-label="Hospital A sends a message to Hospital B, which receives it afterwards">
        <HospitalCard hospital={HOSPITALS[0]} status="ready" detail="Update sent · L2" />
        <div className="story-arrow"><span>Update arrives</span><i /></div>
        <HospitalCard hospital={HOSPITALS[1]} status="ready" detail="Update received · L3" />
      </div>
    );
  }

  if (scene === 'parallel') {
    return (
      <div className="story-parallel-flow" role="img" aria-label="Hospital A and Hospital B work at the same time">
        <HospitalCard hospital={HOSPITALS[0]} status="ready" detail="Checking beds" marker="Work 1" />
        <div className="story-together"><span aria-hidden="true">↔</span><strong>At the same time</strong></div>
        <HospitalCard hospital={HOSPITALS[1]} status="ready" detail="Helping a patient" marker="Work 2" />
      </div>
    );
  }

  if (scene === 'heartbeat') {
    return (
      <>
        <div className="story-hospital-row" role="img" aria-label="Hospitals A and B respond to heartbeat checks; Hospital C does not respond">
          {HOSPITALS.map((hospital, index) => {
            const missing = index === 2;
            return (
              <HospitalCard
                key={hospital.id}
                hospital={hospital}
                status={missing ? 'quiet' : 'ready'}
                detail={missing ? 'No reply yet' : '“I’m here!”'}
                marker={missing ? 'Check again' : '♥'}
              />
            );
          })}
        </div>
        <p className="story-note">No reply means “suspected offline”—it does not prove the hospital has failed.</p>
      </>
    );
  }

  if (scene === 'election') {
    return (
      <div className="story-election-flow" role="img" aria-label="Hospital A sends the leader election to Hospital B; Hospital B wins while Hospital C is offline">
        <HospitalCard hospital={HOSPITALS[0]} status="ready" detail="Starts the vote" />
        <div className="story-arrow"><span>Pass the vote</span><i /></div>
        <HospitalCard hospital={HOSPITALS[1]} status="leader" detail="Chosen team leader" marker="LEADER" />
        <HospitalCard hospital={HOSPITALS[2]} status="quiet" detail="Not reachable" marker="Offline" />
      </div>
    );
  }

  return (
    <div className="story-turns-flow" role="img" aria-label="Hospital B uses the shared reservation record while Hospital A waits its turn">
      <div className="story-queue-column">
        <HospitalCard hospital={HOSPITALS[1]} status="leader" detail="Updating a reservation" marker="Using it now" />
        <HospitalCard hospital={HOSPITALS[0]} status="waiting" detail="Waiting for its turn" marker="Next" />
      </div>
      <div className="story-shared-resource">
        <span aria-hidden="true">▤</span>
        <strong>Shared reservation record</strong>
        <small>Only one update at a time</small>
      </div>
    </div>
  );
}

export default function SynchronizationStory() {
  const [stepIndex, setStepIndex] = useState(0);
  const [playing, setPlaying] = useState(false);
  const step = SYNCHRONIZATION_STORY_STEPS[stepIndex];
  const isLastStep = stepIndex === SYNCHRONIZATION_STORY_STEPS.length - 1;

  useEffect(() => {
    if (!playing) return undefined;
    if (isLastStep) {
      setPlaying(false);
      return undefined;
    }
    const timer = window.setTimeout(() => setStepIndex((current) => current + 1), STORY_STEP_MS);
    return () => window.clearTimeout(timer);
  }, [playing, isLastStep, stepIndex]);

  const selectStep = (index) => {
    setPlaying(false);
    setStepIndex(index);
  };

  const play = () => {
    if (isLastStep) setStepIndex(0);
    setPlaying(true);
  };

  return (
    <section className="panel sync-story" aria-labelledby="sync-story-title">
      <header className="sync-heading">
        <div>
          <p className="eyebrow">A simple story · follow the hospitals</p>
          <h2 id="sync-story-title">How hospitals coordinate</h2>
          <p>Press play and watch a short story about hospitals sharing updates and taking turns.</p>
        </div>
        <span className="sync-demo-badge">DEMO ONLY · no real hospital data changes</span>
      </header>

      <nav className="story-step-nav" aria-label="Hospital coordination story steps">
        {SYNCHRONIZATION_STORY_STEPS.map((item, index) => (
          <button
            type="button"
            className={`story-step-button ${index === stepIndex ? 'selected' : ''} ${index < stepIndex ? 'complete' : ''}`}
            key={item.label}
            aria-current={index === stepIndex ? 'step' : undefined}
            onClick={() => selectStep(index)}
          >
            <span>{index + 1}</span>{item.label}
          </button>
        ))}
      </nav>

      <div className="story-progress" role="progressbar" aria-label="Story progress" aria-valuemin="1" aria-valuemax={SYNCHRONIZATION_STORY_STEPS.length} aria-valuenow={stepIndex + 1}>
        <span style={{ width: `${((stepIndex + 1) / SYNCHRONIZATION_STORY_STEPS.length) * 100}%` }} />
      </div>

      <div className="story-stage" aria-live="polite">
        <div className="story-stage-heading">
          <span>PART {stepIndex + 1} OF {SYNCHRONIZATION_STORY_STEPS.length}</span>
          <h3>{step.title}</h3>
        </div>
        <StoryVisual scene={step.scene} />
        <p className="story-caption">{step.story}</p>
      </div>

      <footer className="story-controls">
        <p>{playing ? 'Playing automatically · each part lasts 5 seconds' : 'Ready when you are · press Play story for a hands-free demo'}</p>
        <div>
          <button type="button" className="secondary-button" onClick={() => selectStep(Math.max(stepIndex - 1, 0))} disabled={stepIndex === 0}>Previous</button>
          <button type="button" className="secondary-button" onClick={playing ? () => setPlaying(false) : play}>
            {playing ? 'Pause story' : isLastStep ? 'Play again' : 'Play story'}
          </button>
          <button type="button" className="primary-button" onClick={() => selectStep(isLastStep ? 0 : stepIndex + 1)}>
            {isLastStep ? 'Start again' : 'Next part'}
          </button>
        </div>
      </footer>
    </section>
  );
}
