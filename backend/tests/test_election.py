import threading
import socket
import unittest
from unittest.mock import patch

import coordinator
from election import run_bully_election
from grpc_service import HospitalRpcClient, start_grpc_server
from rpc_service import build_hospital_service


class BullyElectionTests(unittest.TestCase):
    def test_highest_live_process_wins_and_all_live_nodes_are_notified(self):
        requests = []
        announcements = []

        result = run_bully_election(
            ["H001", "H002", "H003"],
            send_election=lambda caller, target, epoch: (
                requests.append((caller, target, epoch)) or {"answer": True}
            ),
            announce_coordinator=lambda target, leader, epoch: (
                announcements.append((target, leader, epoch)) or {"accepted": True}
            ),
            initiator_id="H001",
            epoch=4,
        )

        self.assertEqual(result["leader_id"], "H003")
        self.assertEqual(result["epoch"], 4)
        self.assertEqual(requests, [
            ("H001", "H002", 4),
            ("H001", "H003", 4),
            ("H002", "H003", 4),
        ])
        self.assertEqual([entry[0] for entry in announcements], ["H001", "H002", "H003"])

    def test_election_can_start_at_a_live_process_after_lower_nodes_fail(self):
        result = run_bully_election(
            ["H002"],
            send_election=lambda *_: self.fail("A lone live node has no higher peer."),
            announce_coordinator=lambda *_: {"accepted": True},
            initiator_id="H002",
        )

        self.assertEqual(result["leader_id"], "H002")

    def test_numeric_process_ids_use_the_bully_priority_not_string_order(self):
        result = run_bully_election(
            ["H002", "H010"],
            send_election=lambda *_: {"answer": True},
            announce_coordinator=lambda *_: {"accepted": True},
            initiator_id="H002",
        )

        self.assertEqual(result["leader_id"], "H010")

    def test_rejects_missing_or_offline_initiator(self):
        with self.assertRaises(ValueError):
            run_bully_election([], lambda *_: {}, lambda *_: {})
        with self.assertRaises(ValueError):
            run_bully_election(["H001"], lambda *_: {}, lambda *_: {}, "H002")


class HospitalElectionStateTests(unittest.TestCase):
    def setUp(self):
        state = {
            "hospital_id": "H002",
            "hospital_name": "Metro Hospital",
            "status": "ONLINE",
            "beds": {"ICU": 1, "Emergency": 2, "General": 3},
        }
        self.operations = build_hospital_service(state, "localhost", 5002)["functions"]

    def test_election_and_coordinator_messages_update_local_role(self):
        reply = self.operations["election_message"]("H001", 2)
        announcement = self.operations["announce_coordinator"]("H003", 2)

        self.assertTrue(reply["answer"])
        self.assertTrue(announcement["accepted"])
        self.assertEqual(self.operations["get_election_state"](), {
            "hospital_id": "H002",
            "leader_id": "H003",
            "epoch": 2,
            "algorithm": "BULLY",
        })

    def test_stale_announcement_cannot_replace_the_newer_epoch(self):
        self.operations["election_message"]("H001", 3)
        stale = self.operations["announce_coordinator"]("H001", 2)

        self.assertFalse(stale["accepted"])
        self.assertIsNone(self.operations["get_election_state"]()["leader_id"])

    def test_election_messages_work_over_the_hospital_grpc_service(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        state = {
            "hospital_id": "H003",
            "hospital_name": "Central Hospital",
            "status": "ONLINE",
            "beds": {"ICU": 1, "Emergency": 2, "General": 3},
        }
        functions = build_hospital_service(state, "127.0.0.1", port)["functions"]
        server = start_grpc_server("127.0.0.1", port, functions)
        client = HospitalRpcClient("127.0.0.1", port)
        try:
            response = client.call("Election", caller_id="H001", epoch=3)
            announcement = client.call("Coordinator", leader_id="H003", epoch=3)
            election_state = client.call("GetElectionState")
        finally:
            client.close()
            server.stop(0).wait()

        self.assertTrue(response["answer"])
        self.assertTrue(announcement["accepted"])
        self.assertEqual(election_state["leader_id"], "H003")
        self.assertEqual(election_state["epoch"], 3.0)
        self.assertEqual(election_state["algorithm"], "BULLY")


class BullyElectionApiTests(unittest.TestCase):
    def setUp(self):
        self.hospitals = [
            {"hospital_id": node, "host": "localhost", "rpc_port": port}
            for node, port in [("H001", 5001), ("H002", 5002), ("H003", 5003)]
        ]
        self.state = {
            "status": "NO_LEADER",
            "leader_id": None,
            "epoch": 0,
            "events": [],
            "acknowledged_nodes": [],
        }
        self.patches = [
            patch.object(coordinator, "ELECTION_STATE", self.state),
            patch.object(coordinator, "ELECTION_LOCK", threading.Lock()),
            patch.object(coordinator, "get_hospital_config", return_value=self.hospitals),
            patch.object(coordinator, "ping_hospital", side_effect=lambda hospital: hospital["hospital_id"] != "H003"),
            patch.object(coordinator, "MONGO_STORE", type("Store", (), {"append_log": lambda *args: None})()),
            patch.object(coordinator, "call_hospital_rpc", side_effect=self.fake_rpc),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = coordinator.app.test_client()

    @staticmethod
    def fake_rpc(hospital, method, **payload):
        if method == "Election":
            return {"answer": True, "hospital_id": hospital["hospital_id"]}
        return {"accepted": True, "hospital_id": hospital["hospital_id"]}

    def test_api_elects_highest_reachable_hospital_and_exposes_role(self):
        response = self.client.post("/api/election/start", json={"initiator_id": "H001"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["leader_id"], "H002")
        self.assertEqual(response.get_json()["acknowledged_nodes"], ["H001", "H002"])
        self.assertEqual(self.client.get("/api/election/status").get_json()["epoch"], 1)


if __name__ == "__main__":
    unittest.main()
