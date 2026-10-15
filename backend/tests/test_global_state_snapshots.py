import unittest
from unittest.mock import patch

import coordinator
from lamport_clock import LamportClock
from vector_clock import VectorClock


class SnapshotStore:
    def __init__(self):
        self.snapshots = []

    def create_global_snapshot(self, snapshot):
        self.snapshots.insert(0, dict(snapshot))
        return dict(snapshot)

    def latest_global_snapshots(self):
        return list(self.snapshots)

    def append_log(self, source, message, lamport_timestamp=0, vector_clock=None):
        pass


class GlobalStateSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.store = SnapshotStore()
        self.hospitals = [
            {
                "hospital_id": "H001",
                "hospital_name": "City Hospital",
                "status": "ONLINE",
                "beds": {"ICU": 2, "Emergency": 3, "General": 4},
                "rpc_status": "ONLINE",
                "lamport_timestamp": 12,
            },
            {
                "hospital_id": "H002",
                "hospital_name": "Metro Hospital",
                "status": "OFFLINE",
                "beds": {"ICU": 0, "Emergency": 0, "General": 0},
                "rpc_status": "OFFLINE",
                "lamport_timestamp": None,
            },
        ]
        self.patches = [
            patch.object(coordinator, "MONGO_STORE", self.store),
            patch.object(coordinator, "COORDINATOR_CLOCK", LamportClock("TEST_COORDINATOR")),
            patch.object(coordinator, "COORDINATOR_VECTOR_CLOCK", VectorClock("TEST_COORDINATOR")),
            patch.object(coordinator, "COORDINATOR_VECTOR_CLOCK", VectorClock("TEST_COORDINATOR")),
            patch.object(coordinator, "get_all_hospital_statuses", return_value=self.hospitals),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = coordinator.app.test_client()

    def test_capture_persists_hospital_local_state_and_clock(self):
        response = self.client.post("/api/global-state/snapshots")
        body = response.get_json()

        self.assertEqual(response.status_code, 201)
        self.assertEqual(body["snapshot"]["consistency"], "BEST_EFFORT")
        self.assertEqual(body["snapshot"]["hospitals"][0]["lamport_timestamp"], 12)
        self.assertEqual(body["snapshot"]["hospitals"][1]["status"], "OFFLINE")
        self.assertEqual(len(self.store.snapshots), 1)
        self.assertEqual(
            self.store.snapshots[0]["snapshot_id"],
            body["snapshot"]["snapshot_id"],
        )

    def test_recent_snapshots_endpoint_returns_persisted_history(self):
        self.store.snapshots.append({"snapshot_id": "saved-snapshot"})

        response = self.client.get("/api/global-state/snapshots")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["snapshots"][0]["snapshot_id"], "saved-snapshot")


if __name__ == "__main__":
    unittest.main()
