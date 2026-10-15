import assert from 'node:assert/strict';
import test from 'node:test';
import { SYNCHRONIZATION_STORY_STEPS } from './synchronizationStory.js';

test('plain-language synchronization story covers the mechanisms in order', () => {
  assert.deepEqual(
    SYNCHRONIZATION_STORY_STEPS.map(({ label }) => label),
    ['Event order', 'Parallel work', 'Heartbeat check', 'Pick a leader', 'Take turns'],
  );
  assert.ok(SYNCHRONIZATION_STORY_STEPS.every(({ title, story, scene }) => title && story && scene));
});