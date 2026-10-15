import threading


class VectorClock:
    """Track per-node logical counters and compare event causality."""

    def __init__(self, node_id, advance=None):
        self.node_id = node_id
        self._advance_persisted = advance
        self._value = {node_id: 0}
        self._lock = threading.Lock()

    @staticmethod
    def _validate(vector):
        if not isinstance(vector, dict):
            raise ValueError("A vector clock must be an object of node counters.")
        validated = {}
        for node_id, counter in vector.items():
            if (
                not isinstance(node_id, str)
                or not node_id
                or "." in node_id
                or node_id.startswith("$")
                or isinstance(counter, bool)
                or not isinstance(counter, (int, float))
                or counter < 0
                or isinstance(counter, float) and not counter.is_integer()
            ):
                raise ValueError("Vector clocks require safe node names and non-negative integer counters.")
            validated[node_id] = int(counter)
        return validated

    def _advance(self, observed):
        candidate = dict(self._value)
        for node_id, counter in observed.items():
            candidate[node_id] = max(candidate.get(node_id, 0), counter)

        if self._advance_persisted is not None:
            persisted = self._validate(self._advance_persisted(self.node_id, candidate))
            if persisted.get(self.node_id, 0) <= candidate[self.node_id]:
                raise ValueError("The persisted vector clock did not advance.")
            candidate = persisted
        else:
            candidate[self.node_id] = candidate.get(self.node_id, 0) + 1

        self._value = candidate
        return dict(candidate)

    def tick(self):
        """Record a local event and return a copy of the vector timestamp."""
        with self._lock:
            return self._advance({})

    def receive(self, remote_vector):
        """Merge a received vector timestamp, then record the local receive event."""
        observed = self._validate(remote_vector)
        with self._lock:
            return self._advance(observed)

    @staticmethod
    def compare(first, second):
        """Return before, after, equal, or concurrent for two vector timestamps."""
        first = VectorClock._validate(first)
        second = VectorClock._validate(second)
        nodes = first.keys() | second.keys()
        first_le_second = all(first.get(node, 0) <= second.get(node, 0) for node in nodes)
        second_le_first = all(second.get(node, 0) <= first.get(node, 0) for node in nodes)
        if first_le_second and second_le_first:
            return "equal"
        if first_le_second:
            return "before"
        if second_le_first:
            return "after"
        return "concurrent"
