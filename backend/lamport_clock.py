import threading


class LamportClock:
    """Maintain a Lamport clock, optionally persisting each advance."""

    def __init__(self, node_id, advance=None):
        self.node_id = node_id
        self._advance_persisted = advance
        self._value = 0
        self._lock = threading.Lock()

    @staticmethod
    def _valid_timestamp(timestamp):
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            raise ValueError("Lamport timestamps must be non-negative integers.")
        if timestamp < 0:
            raise ValueError("Lamport timestamps must be non-negative integers.")
        if isinstance(timestamp, float) and not timestamp.is_integer():
            raise ValueError("Lamport timestamps must be non-negative integers.")
        return int(timestamp)

    def _advance(self, observed):
        candidate = max(self._value, observed)
        if self._advance_persisted is None:
            candidate += 1
        else:
            candidate = self._advance_persisted(self.node_id, candidate)
            candidate = self._valid_timestamp(candidate)
            if candidate <= max(self._value, observed):
                raise ValueError("The persisted Lamport clock did not advance.")
        self._value = candidate
        return candidate

    def tick(self):
        """Record a local event and return its logical timestamp."""
        with self._lock:
            return self._advance(self._value)

    def receive(self, remote_timestamp):
        """Record receipt of a message and return the next causal timestamp."""
        remote_timestamp = self._valid_timestamp(remote_timestamp)
        with self._lock:
            return self._advance(remote_timestamp)
