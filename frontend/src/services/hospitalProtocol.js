export const HOSPITALS = [
  { id: 'H001', name: 'Hospital A', initialIcuBeds: 2 },
  { id: 'H002', name: 'Hospital B', initialIcuBeds: 0 },
  { id: 'H003', name: 'Hospital C', initialIcuBeds: 4 },
];

export class LamportClock {
  constructor(initialValue = 0) {
    this.value = initialValue;
  }

  tick() {
    this.value += 1;
    return this.value;
  }

  receive(remoteTimestamp) {
    if (!Number.isSafeInteger(remoteTimestamp) || remoteTimestamp < 0) {
      throw new Error('Received an invalid Lamport timestamp.');
    }
    this.value = Math.max(this.value, remoteTimestamp) + 1;
    return this.value;
  }
}

export class IcuMutex {
  constructor() {
    this.locked = false;
    this.queue = [];
    this.completed = new Map();
  }

  enqueue(request) {
    const completed = this.completed.get(request.payload.requestId);
    if (completed) return { duplicate: true, result: completed };
    if (this.queue.some((item) => item.payload.requestId === request.payload.requestId)) {
      return { duplicate: true, result: null };
    }
    this.queue.push(request);
    this.queue.sort((left, right) => (
      left.lamportTimestamp - right.lamportTimestamp
      || left.senderId.localeCompare(right.senderId)
      || left.payload.requestId.localeCompare(right.payload.requestId)
    ));
    return { duplicate: false, result: null };
  }

  acquireNext() {
    if (this.locked) return null;
    this.locked = true;
    return this.queue.shift() || null;
  }

  finish(request, result) {
    this.completed.set(request.payload.requestId, result);
    if (this.completed.size > 512) {
      this.completed.delete(this.completed.keys().next().value);
    }
    this.locked = false;
  }
}
