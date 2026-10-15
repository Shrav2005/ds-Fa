export const SYNCHRONIZATION_STORY_STEPS = [
  {
    title: 'First, agree on the order',
    label: 'Event order',
    story: 'Hospital A sends an update. Hospital B receives it and knows it happened after the send.',
    scene: 'order',
  },
  {
    title: 'Some things happen at the same time',
    label: 'Parallel work',
    story: 'Hospital A checks a bed while Hospital B handles a patient. Neither needs to wait for the other.',
    scene: 'parallel',
  },
  {
    title: 'Check who is still responding',
    label: 'Heartbeat check',
    story: 'Hospitals send regular “I’m here” signals. Hospital C misses one, so the others suspect it may be offline.',
    scene: 'heartbeat',
  },
  {
    title: 'Choose a team leader',
    label: 'Pick a leader',
    story: 'Only hospitals that respond take part. Of the reachable hospitals, Hospital B has the highest ID and is chosen.',
    scene: 'election',
  },
  {
    title: 'Take turns changing shared information',
    label: 'Take turns',
    story: 'The coordinator gives Hospital B permission to update the shared reservation first. Hospital A waits, then gets its turn after B is done.',
    scene: 'turns',
  },
];