import math
import threading
import time


def estimate_clock_offset(
    coordinator_sent_ms,
    hospital_received_ms,
    hospital_sent_ms,
    coordinator_received_ms,
):
    """Estimate hospital-minus-coordinator offset and round-trip delay using NTP timestamps."""
    values = (
        coordinator_sent_ms,
        hospital_received_ms,
        hospital_sent_ms,
        coordinator_received_ms,
    )
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in values):
        raise ValueError("Clock sample timestamps must be numbers.")
    if coordinator_received_ms < coordinator_sent_ms or hospital_sent_ms < hospital_received_ms:
        raise ValueError("Clock sample timestamps must be ordered within each node.")

    offset_ms = (
        (hospital_received_ms - coordinator_sent_ms)
        + (hospital_sent_ms - coordinator_received_ms)
    ) / 2
    round_trip_ms = (
        coordinator_received_ms - coordinator_sent_ms
        - (hospital_sent_ms - hospital_received_ms)
    )
    return {
        "estimated_offset_ms": round(offset_ms, 3),
        "round_trip_ms": round(round_trip_ms, 3),
    }


def calculate_berkeley_adjustments(clock_offsets_ms):
    """Calculate adjustments that bring every supplied clock to their mean."""
    if not isinstance(clock_offsets_ms, dict) or not clock_offsets_ms:
        raise ValueError("Clock offsets must be a non-empty object.")

    for node_id, offset_ms in clock_offsets_ms.items():
        if (
            not isinstance(node_id, str)
            or not node_id
            or isinstance(offset_ms, bool)
            or not isinstance(offset_ms, (int, float))
            or not math.isfinite(offset_ms)
        ):
            raise ValueError("Clock offsets must map node names to finite numbers.")

    average_offset_ms = sum(clock_offsets_ms.values()) / len(clock_offsets_ms)
    return {
        "average_offset_ms": round(average_offset_ms, 3),
        "adjustments_ms": {
            node_id: round(average_offset_ms - offset_ms, 3)
            for node_id, offset_ms in clock_offsets_ms.items()
        },
    }


class ClockAdjustment:
    """Track a simulated wall-clock adjustment without changing the host clock."""

    def __init__(self):
        self._lock = threading.Lock()
        self._offset_ms = 0.0

    def get_ms(self):
        with self._lock:
            return self._offset_ms

    def set_ms(self, offset_ms):
        if (
            isinstance(offset_ms, bool)
            or not isinstance(offset_ms, (int, float))
            or not math.isfinite(offset_ms)
        ):
            raise ValueError("Clock adjustment must be a finite number.")
        with self._lock:
            self._offset_ms = float(offset_ms)
            return self._offset_ms

    def now_ms(self):
        return time.time_ns() / 1_000_000 + self.get_ms()
