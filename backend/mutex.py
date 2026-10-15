"""Coordinator-managed mutual exclusion for hospital bed reservations."""

from contextlib import contextmanager
from threading import Condition


PRIORITY_RANK = {"Normal": 0, "High": 1, "Critical": 2}
VALID_POLICIES = {"FAIR", "PRIORITY"}


class CentralizedMutex:
    """Grant one reservation critical section at a time in a selectable queue order."""

    def __init__(self, policy="FAIR"):
        if policy not in VALID_POLICIES:
            raise ValueError("Mutex policy must be FAIR or PRIORITY.")
        self._condition = Condition()
        self._policy = policy
        self._sequence = 0
        self._active = None
        self._waiters = []

    @property
    def policy(self):
        with self._condition:
            return self._policy

    def status(self):
        with self._condition:
            return {
                "policy": self._policy,
                "active": self._active["request_id"] if self._active else None,
                "waiting": len(self._waiters),
            }

    def set_policy(self, policy):
        if policy not in VALID_POLICIES:
            raise ValueError("Mutex policy must be FAIR or PRIORITY.")
        with self._condition:
            self._policy = policy
            self._condition.notify_all()
        return policy

    def _order_key(self, ticket):
        if self._policy == "PRIORITY":
            return (
                -PRIORITY_RANK[ticket["priority"]],
                ticket["lamport_timestamp"],
                ticket["sequence"],
            )
        return ticket["lamport_timestamp"], ticket["sequence"]

    @contextmanager
    def acquire(self, request_id, priority, lamport_timestamp):
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("A reservation request ID is required.")
        if priority not in PRIORITY_RANK:
            raise ValueError("Priority must be Critical, High, or Normal.")
        if (
            isinstance(lamport_timestamp, bool)
            or not isinstance(lamport_timestamp, int)
            or lamport_timestamp < 0
        ):
            raise ValueError("The mutex requires a non-negative Lamport timestamp.")

        with self._condition:
            self._sequence += 1
            ticket = {
                "request_id": request_id,
                "priority": priority,
                "lamport_timestamp": lamport_timestamp,
                "sequence": self._sequence,
            }
            self._waiters.append(ticket)
            while self._active is not None or min(self._waiters, key=self._order_key) is not ticket:
                self._condition.wait()
            self._waiters.remove(ticket)
            self._active = ticket

        try:
            yield
        finally:
            with self._condition:
                if self._active is not ticket:
                    raise RuntimeError("Reservation mutex ownership was lost.")
                self._active = None
                self._condition.notify_all()
