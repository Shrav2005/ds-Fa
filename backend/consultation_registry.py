"""Short-lived receipts for consultations completed by the WebRTC UI."""

import threading
import time


class ConsultationRegistry:
    TTL_SECONDS = 3600

    def __init__(self):
        self._lock = threading.Lock()
        self._completed = {}

    def _prune(self, now):
        self._completed = {
            request_id: receipt
            for request_id, receipt in self._completed.items()
            if now - receipt["completed_at"] <= self.TTL_SECONDS
        }

    def mark_completed(self, request_id, hospital_id):
        if not isinstance(request_id, str) or not request_id.strip():
            raise ValueError("Request ID is required.")
        if not isinstance(hospital_id, str) or not hospital_id.strip():
            raise ValueError("Hospital ID is required.")
        with self._lock:
            now = time.time()
            self._prune(now)
            existing = self._completed.get(request_id)
            if existing and existing["hospital_id"] != hospital_id:
                raise ValueError("This request was completed with a different hospital.")
            self._completed[request_id] = {
                "hospital_id": hospital_id,
                "completed_at": now,
            }
            return dict(self._completed[request_id])

    def has_completed(self, request_id, hospital_id=None):
        with self._lock:
            self._prune(time.time())
            receipt = self._completed.get(request_id)
            return bool(receipt and (hospital_id is None or receipt["hospital_id"] == hospital_id))
