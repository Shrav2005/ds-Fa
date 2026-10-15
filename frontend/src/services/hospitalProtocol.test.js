import assert from 'node:assert/strict';
import test from 'node:test';
import { IcuMutex, LamportClock } from './hospitalProtocol.js';

test('Lamport clock ticks locally and merges received timestamps', () => {
  const clock = new LamportClock();
  assert.equal(clock.tick(), 1);
  assert.equal(clock.receive(8), 9);
  assert.equal(clock.tick(), 10);
});

test('ICU mutex orders requests by timestamp and sender and caches grants', () => {
  const mutex = new IcuMutex();
  const later = {
    senderId: 'H003',
    lamportTimestamp: 4,
    payload: { requestId: 'later' },
  };
  const firstBySender = {
    senderId: 'H001',
    lamportTimestamp: 4,
    payload: { requestId: 'first' },
  };
  mutex.enqueue(later);
  mutex.enqueue(firstBySender);

  const first = mutex.acquireNext();
  assert.equal(first.payload.requestId, 'first');
  mutex.finish(first, { granted: true });
  const second = mutex.acquireNext();
  assert.equal(second.payload.requestId, 'later');
  mutex.finish(second, { granted: false });

  assert.deepEqual(mutex.enqueue(later), {
    duplicate: true,
    result: { granted: false },
  });
});

test('ICU mutex permits only one active request at a time', () => {
  const mutex = new IcuMutex();
  mutex.enqueue({
    senderId: 'H001',
    lamportTimestamp: 1,
    payload: { requestId: 'first' },
  });
  mutex.enqueue({
    senderId: 'H003',
    lamportTimestamp: 2,
    payload: { requestId: 'second' },
  });

  const first = mutex.acquireNext();
  assert.equal(mutex.acquireNext(), null);
  mutex.finish(first, { granted: true });
  assert.equal(mutex.acquireNext().payload.requestId, 'second');
});
