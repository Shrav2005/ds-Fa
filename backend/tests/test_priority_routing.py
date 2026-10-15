import unittest
from unittest.mock import Mock, patch

import coordinator
from lamport_clock import LamportClock
from vector_clock import VectorClock


class PriorityRoutingTests(unittest.TestCase):
    def setUp(self):
        self.hospitals = [
            {
                "hospital_id": "H001",
                "hospital_name": "Small Hospital",
                "status": "ONLINE",
                "beds": {"ICU": 1},
            },
            {
                "hospital_id": "H002",
                "hospital_name": "Large Hospital",
                "status": "ONLINE",
                "beds": {"ICU": 4},
            },
        ]
        patcher = patch.object(coordinator, "add_log")
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_urgent_requests_use_largest_capacity_and_normal_requests_best_fit(self):
        critical = coordinator.select_best_hospital("ICU", self.hospitals, "Critical")
        high = coordinator.select_best_hospital("ICU", self.hospitals, "High")
        normal = coordinator.select_best_hospital("ICU", self.hospitals, "Normal")

        self.assertEqual(critical["hospital_id"], "H002")
        self.assertEqual(high["hospital_id"], "H002")
        self.assertEqual(normal["hospital_id"], "H001")

    def test_offline_and_empty_capacity_nodes_are_never_selected(self):
        statuses = [
            {**self.hospitals[0], "status": "OFFLINE"},
            {**self.hospitals[1], "beds": {"ICU": 0}},
        ]

        self.assertIsNone(coordinator.select_best_hospital("ICU", statuses, "Critical"))

    def test_request_rejects_unknown_priority(self):
        client = coordinator.app.test_client()
        response = client.post("/api/emergency/request", json={
            "patient_id": "P-1",
            "bed_type": "ICU",
            "priority": "Urgent-ish",
        })

        self.assertEqual(response.status_code, 400)
        self.assertIn("Priority", response.get_json()["message"])

    def test_urgent_priority_is_carried_to_the_hospital_reservation_rpc(self):
        client = Mock()
        client.call.return_value = {
            "success": True,
            "remaining_beds": 0,
            "lamport_timestamp": 4,
            "vector_clock": {"COORDINATOR": 2, "H001": 1},
        }
        hospital = {
            "hospital_id": "H001",
            "hospital_name": "Small Hospital",
            "host": "localhost",
            "rpc_port": 5001,
        }
        with (
            patch.object(coordinator, "COORDINATOR_CLOCK", LamportClock("COORDINATOR")),
            patch.object(coordinator, "COORDINATOR_VECTOR_CLOCK", VectorClock("COORDINATOR")),
            patch.object(coordinator, "HospitalRpcClient", return_value=client),
        ):
            result = coordinator.reserve_bed_at_hospital(
                hospital,
                "ICU",
                "P-2",
                priority="Critical",
            )

        self.assertTrue(result["success"])
        self.assertEqual(client.call.call_args.kwargs["priority"], "Critical")


if __name__ == "__main__":
    unittest.main()
