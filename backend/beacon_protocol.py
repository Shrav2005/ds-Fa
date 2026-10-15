import threading
import time
from datetime import datetime, timezone

import grpc
import requests


BEACON_INTERVAL_SECONDS = 2
BEACON_TIMEOUT_SECONDS = 8
COORDINATOR_BEACON_INTERVAL_SECONDS = 2
COORDINATOR_BEACON_TIMEOUT_SECONDS = 8


class HospitalBeaconRegistry:
    """Track the coordinator's most recent receipt of each hospital beacon."""

    def __init__(self, timeout=BEACON_TIMEOUT_SECONDS):
        self.timeout = timeout
        self._beacons = {}
        self._lock = threading.Lock()

    def record(self, hospital_id, hospital_name):
        now = time.monotonic()
        beacon = {
            "hospital_id": hospital_id,
            "hospital_name": hospital_name,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "last_seen": now,
        }
        with self._lock:
            self._beacons[hospital_id] = beacon
        return self.get_status(hospital_id, now)

    def get_status(self, hospital_id, now=None):
        now = time.monotonic() if now is None else now
        with self._lock:
            beacon = self._beacons.get(hospital_id)
            if beacon is None:
                return {
                    "beacon_status": "WAITING",
                    "last_beacon_at": None,
                    "heartbeat_age_seconds": None,
                }
            age = max(0, int(now - beacon["last_seen"]))
            return {
                "beacon_status": "ONLINE" if age <= self.timeout else "STALE",
                "last_beacon_at": beacon["received_at"],
                "heartbeat_age_seconds": age,
            }


def send_hospital_beacons(coordinator_url, make_beacon, stop_event, interval=BEACON_INTERVAL_SECONDS):
    """Periodically send a hospital's beacon to the coordinator until stopped."""
    endpoint = f"{coordinator_url.rstrip('/')}/api/hospitals/beacon"
    last_error = None
    while not stop_event.is_set():
        try:
            response = requests.post(endpoint, json=make_beacon(), timeout=2)
            response.raise_for_status()
            if last_error:
                print("Coordinator beacon connection restored.", flush=True)
            last_error = None
        except requests.RequestException as exc:
            error = str(exc)
            if error != last_error:
                print(f"Coordinator beacon delivery failed: {error}", flush=True)
            last_error = error
        stop_event.wait(interval)


def send_coordinator_heartbeats(
    hospitals,
    make_beacon,
    send_heartbeat,
    stop_event,
    interval=COORDINATOR_BEACON_INTERVAL_SECONDS,
):
    """Send coordinator heartbeats to configured hospitals until stopped."""
    errors = {}
    while not stop_event.is_set():
        beacon = make_beacon()
        if beacon is not None:
            for hospital in hospitals:
                hospital_id = hospital["hospital_id"]
                try:
                    result = send_heartbeat(hospital, beacon)
                    if not result or not result.get("accepted"):
                        message = (
                            result.get("message", "Hospital did not accept the coordinator heartbeat.")
                            if isinstance(result, dict)
                            else "Hospital did not accept the coordinator heartbeat."
                        )
                        raise RuntimeError(message)
                    if hospital_id in errors:
                        print(
                            f"Coordinator heartbeat connection restored for {hospital_id}.",
                            flush=True,
                        )
                        del errors[hospital_id]
                except (grpc.RpcError, requests.RequestException, RuntimeError, OSError) as exc:
                    error = str(exc)
                    if errors.get(hospital_id) != error:
                        print(
                            f"Coordinator heartbeat delivery to {hospital_id} failed: {error}",
                            flush=True,
                        )
                    errors[hospital_id] = error
        stop_event.wait(interval)


def monitor_coordinator_heartbeats(
    get_status,
    on_failure,
    stop_event,
    interval=COORDINATOR_BEACON_INTERVAL_SECONDS,
):
    """Trigger one election for each coordinator-heartbeat timeout episode."""
    election_triggered = False
    while not stop_event.is_set():
        status = get_status()
        if status["beacon_status"] == "ONLINE":
            election_triggered = False
        elif status["beacon_status"] == "STALE" and not election_triggered:
            election_triggered = True
            on_failure(status)
        stop_event.wait(interval)
