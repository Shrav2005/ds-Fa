import unittest
from unittest.mock import patch

import coordinator
from deadlock import detect_deadlocks
from lamport_clock import LamportClock
from vector_clock import VectorClock


def opposite_order_transfers():
    return [
        {
            "transaction_id": "TRANSFER-001",
            "lamport_timestamp": 10,
            "holds": ["H001:ICU"],
            "waits_for": ["H002:ICU"],
        },
        {
            "transaction_id": "TRANSFER-002",
            "lamport_timestamp": 20,
            "holds": ["H002:ICU"],
            "waits_for": ["H001:ICU"],
        },
    ]


class EdgeChasingTests(unittest.TestCase):
    def test_detects_cycle_and_aborts_youngest_lamport_transaction(self):
        result = detect_deadlocks(opposite_order_transfers())

        self.assertTrue(result["deadlock_detected"])
        self.assertEqual(result["classification"], "DISTRIBUTED_EDGE_CHASING")
        self.assertEqual(result["cycles"], [["TRANSFER-001", "TRANSFER-002"]])
        self.assertEqual(result["victim"]["transaction_id"], "TRANSFER-002")
        self.assertEqual(result["wait_for_graph"], [
            {"from": "TRANSFER-001", "to": "TRANSFER-002"},
            {"from": "TRANSFER-002", "to": "TRANSFER-001"},
        ])
        self.assertEqual(result["resolved_wait_for_graph"], [])
        self.assertTrue(any(
            probe["receiver"] == probe["initiator"]
            for probe in result["probes"]
        ))

    def test_reports_no_deadlock_when_wait_chain_does_not_return(self):
        transactions = [
            {
                "transaction_id": "T1",
                "lamport_timestamp": 1,
                "holds": ["A"],
                "waits_for": ["B"],
            },
            {
                "transaction_id": "T2",
                "lamport_timestamp": 2,
                "holds": ["B"],
                "waits_for": [],
            },
        ]

        result = detect_deadlocks(transactions)

        self.assertFalse(result["deadlock_detected"])
        self.assertIsNone(result["victim"])
        self.assertEqual(result["wait_for_graph"], [{"from": "T1", "to": "T2"}])

    def test_rejects_malformed_and_duplicate_transactions(self):
        with self.assertRaises(ValueError):
            detect_deadlocks([{
                "transaction_id": "T1",
                "lamport_timestamp": -1,
                "holds": [],
                "waits_for": [],
            }])
        with self.assertRaises(ValueError):
            detect_deadlocks([
                {"transaction_id": "T1", "lamport_timestamp": 1},
                {"transaction_id": "T1", "lamport_timestamp": 2},
            ])


class DeadlockApiTests(unittest.TestCase):
    def setUp(self):
        class Store:
            @staticmethod
            def append_log(*_args, **_kwargs):
                pass

        self.patches = [
            patch.object(coordinator, "COORDINATOR_CLOCK", LamportClock("TEST_COORDINATOR")),
            patch.object(coordinator, "COORDINATOR_VECTOR_CLOCK", VectorClock("TEST_COORDINATOR")),
            patch.object(coordinator, "MONGO_STORE", Store()),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = coordinator.app.test_client()

    def test_demo_endpoint_traces_probes_and_aborts_youngest_transfer(self):
        response = self.client.post("/api/deadlock/demo", json={})
        body = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["deadlock_detected"])
        self.assertEqual(body["victim"]["transaction_id"], "TRANSFER-002")
        self.assertEqual(body["transactions"][1]["status"], "ABORTED")

    def test_check_endpoint_validates_transaction_payload(self):
        response = self.client.post("/api/deadlock/check", json={"transactions": []})
        invalid = self.client.post("/api/deadlock/check", json={"transactions": "not a list"})

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()["deadlock_detected"])
        self.assertEqual(invalid.status_code, 400)


if __name__ == "__main__":
    unittest.main()
