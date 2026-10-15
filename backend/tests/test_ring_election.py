import socket
import threading
import unittest
from unittest.mock import patch

import coordinator
import grpc
from beacon_protocol import (
    monitor_coordinator_heartbeats,
    send_coordinator_heartbeats,
)
from ring_election import run_ring_election
from rpc_service import build_hospital_service
from grpc_service import HospitalRpcClient, start_grpc_server


class RingElectionTests(unittest.TestCase):
    def test_candidate_message_circulates_and_highest_live_id_wins(self):
        requests = []
        announcements = []

        result = run_ring_election(
            ["H001", "H002", "H003"],
            lambda target, candidates, epoch, initiator: (
                requests.append((target, candidates, epoch, initiator))
                or {"accepted": True, "candidate_ids": [*candidates, target]}
            ),
            lambda target, leader, epoch: (
                announcements.append((target, leader, epoch))
                or {"accepted": True}
            ),
            initiator_id="H002",
            epoch=4,
        )

        self.assertEqual(result["leader_id"], "H003")
        self.assertEqual(result["participants"], ["H002", "H003", "H001"])
        self.assertEqual(
            [request[0] for request in requests],
            ["H003", "H001"],
        )
        self.assertEqual(
            [entry[0] for entry in announcements],
            ["H002", "H003", "H001"],
        )
        self.assertEqual(result["failures"], [])

    def test_ring_skips_offline_nodes_and_reports_them(self):
        result = run_ring_election(
            ["H001", "H002", "H003"],
            lambda target, candidates, _epoch, _initiator: (
                None if target == "H003"
                else {"accepted": True, "candidate_ids": [*candidates, target]}
            ),
            lambda *_: {"accepted": True},
            initiator_id="H002",
            epoch=1,
        )

        self.assertEqual(result["leader_id"], "H002")
        self.assertEqual(result["participants"], ["H002", "H001"])
        self.assertEqual(result["failures"][0]["hospital_id"], "H003")

    def test_rejects_invalid_election_configuration(self):
        with self.assertRaises(ValueError):
            run_ring_election([], lambda *_: {}, lambda *_: {}, "H001", 1)
        with self.assertRaises(ValueError):
            run_ring_election(["H001"], lambda *_: {}, lambda *_: {}, "H002", 1)
        with self.assertRaises(ValueError):
            run_ring_election(["H001"], lambda *_: {}, lambda *_: {}, "H001", 0)


class HospitalRingElectionTests(unittest.TestCase):
    def setUp(self):
        state = {
            "hospital_id": "H002",
            "hospital_name": "Metro Hospital",
            "status": "ONLINE",
            "beds": {"ICU": 1, "Emergency": 2, "General": 3},
        }
        self.operations = build_hospital_service(
            state,
            "localhost",
            5002,
            ring_members=["H001", "H002", "H003"],
        )["functions"]

    def test_hospital_appends_its_id_and_accepts_elected_ring_leader(self):
        token = self.operations["ring_election_message"](
            ["H001"],
            "H001",
            3,
        )
        announcement = self.operations["announce_coordinator"]("H003", 3, "RING")

        self.assertEqual(token["candidate_ids"], ["H001", "H002"])
        self.assertTrue(announcement["accepted"])
        self.assertEqual(self.operations["get_election_state"]()["leader_id"], "H003")
        self.assertEqual(self.operations["get_election_state"]()["algorithm"], "RING")

    def test_ring_token_rejects_duplicate_processes_and_invalid_candidates(self):
        duplicate = self.operations["ring_election_message"](
            ["H001", "H002"],
            "H001",
            3,
        )
        invalid = self.operations["ring_election_message"](
            ["not-configured", "H001"],
            "H001",
            3,
        )

        self.assertFalse(duplicate["accepted"])
        self.assertFalse(invalid["accepted"])

    def test_same_epoch_announcements_converge_on_highest_leader_id(self):
        self.operations["announce_coordinator"]("H003", 3, "RING")
        rejected = self.operations["announce_coordinator"]("H001", 3, "BULLY")

        self.assertFalse(rejected["accepted"])
        self.assertEqual(self.operations["get_election_state"]()["leader_id"], "H003")

    def test_coordinator_heartbeat_status_becomes_online_then_stale(self):
        with patch("rpc_service.time.monotonic", return_value=100):
            operations = build_hospital_service({
                "hospital_id": "H002",
                "hospital_name": "Metro Hospital",
                "status": "ONLINE",
                "beds": {},
            }, "localhost", 5002)["functions"]
            waiting = operations["get_coordinator_heartbeat_status"]()
        self.assertEqual(waiting["beacon_status"], "WAITING")

        with patch("rpc_service.time.monotonic", return_value=101):
            accepted = operations["receive_coordinator_heartbeat"](
                "COORDINATOR",
                0,
                lamport_timestamp=5,
            )
            online = operations["get_coordinator_heartbeat_status"]()
        with patch("rpc_service.time.monotonic", return_value=110):
            stale = operations["get_coordinator_heartbeat_status"]()

        self.assertTrue(accepted["accepted"])
        self.assertEqual(online["beacon_status"], "ONLINE")
        self.assertEqual(stale["beacon_status"], "STALE")
        self.assertEqual(stale["heartbeat_age_seconds"], 9)

    def test_elected_hospital_emits_its_own_heartbeat(self):
        self.operations["announce_coordinator"]("H002", 2, "RING")

        heartbeat = self.operations["create_coordinator_heartbeat"]()

        self.assertEqual(heartbeat["coordinator_id"], "H002")
        self.assertEqual(heartbeat["epoch"], 2)
        self.assertIn("lamport_timestamp", heartbeat)

    def test_ring_message_and_coordinator_heartbeat_work_over_grpc(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        state = {
            "hospital_id": "H002",
            "hospital_name": "Metro Hospital",
            "status": "ONLINE",
            "beds": {},
        }
        operations = build_hospital_service(
            state,
            "127.0.0.1",
            port,
            ring_members=["H001", "H002", "H003"],
        )["functions"]
        server = start_grpc_server("127.0.0.1", port, operations)
        client = HospitalRpcClient("127.0.0.1", port)
        try:
            token = client.call(
                "RingElection",
                candidate_ids=["H001"],
                initiator_id="H001",
                epoch=2,
            )
            announcement = client.call(
                "Coordinator",
                leader_id="H002",
                epoch=2,
                algorithm="RING",
            )
            heartbeat = client.call(
                "CoordinatorHeartbeat",
                coordinator_id="H002",
                epoch=2,
                lamport_timestamp=5,
            )
            status = client.call("GetCoordinatorHeartbeatStatus")
            with self.assertRaises(grpc.RpcError) as error:
                client.call(
                    "CoordinatorHeartbeat",
                    coordinator_id="COORDINATOR",
                    epoch=0,
                    lamport_timestamp=-1,
                )
        finally:
            client.close()
            server.stop(0).wait()

        self.assertEqual(token["candidate_ids"], ["H001", "H002"])
        self.assertTrue(announcement["accepted"])
        self.assertTrue(heartbeat["accepted"])
        self.assertEqual(status["coordinator_id"], "H002")
        self.assertEqual(error.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)


class CoordinatorHeartbeatLoopTests(unittest.TestCase):
    def test_sender_sends_current_coordinator_beacon_to_each_hospital(self):
        class OneCycleEvent:
            def __init__(self):
                self.stopped = False

            def is_set(self):
                return self.stopped

            def wait(self, _interval):
                self.stopped = True

        hospitals = [{"hospital_id": "H001"}, {"hospital_id": "H002"}]
        sent = []
        stop_event = OneCycleEvent()

        send_coordinator_heartbeats(
            hospitals,
            lambda: {"coordinator_id": "COORDINATOR", "epoch": 0},
            lambda hospital, beacon: (
                sent.append((hospital["hospital_id"], beacon))
                or {"accepted": True}
            ),
            stop_event,
        )

        self.assertEqual([item[0] for item in sent], ["H001", "H002"])
        self.assertTrue(all(item[1]["epoch"] == 0 for item in sent))

    def test_monitor_triggers_once_per_heartbeat_timeout_episode(self):
        class OneCycleEvent:
            def __init__(self):
                self.stopped = False

            def is_set(self):
                return self.stopped

            def wait(self, _interval):
                self.stopped = True

        failures = []
        monitor_coordinator_heartbeats(
            lambda: {"beacon_status": "STALE", "heartbeat_age_seconds": 9},
            failures.append,
            OneCycleEvent(),
        )

        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["heartbeat_age_seconds"], 9)


class RingElectionApiTests(unittest.TestCase):
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
            patch.object(coordinator, "ping_hospital", return_value=True),
            patch.object(coordinator, "MONGO_STORE", type("Store", (), {"append_log": lambda *args: None})()),
            patch.object(coordinator, "call_hospital_rpc", side_effect=self.fake_rpc),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = coordinator.app.test_client()

    @staticmethod
    def fake_rpc(hospital, method, **payload):
        hospital_id = hospital["hospital_id"]
        if method == "GetElectionState":
            return {"epoch": 0}
        if method == "RingElection":
            return {
                "accepted": True,
                "candidate_ids": [*payload["candidate_ids"], hospital_id],
            }
        return {
            "accepted": True,
            "hospital_id": hospital_id,
            "leader_id": payload.get("leader_id"),
        }

    def test_ring_api_elects_highest_id_and_records_algorithm_trace(self):
        response = self.client.post("/api/election/ring-start", json={"initiator_id": "H001"})

        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertEqual(result["algorithm"], "RING")
        self.assertEqual(result["leader_id"], "H003")
        self.assertEqual(result["acknowledged_nodes"], ["H001", "H002", "H003"])
        self.assertTrue(result["success"])


if __name__ == "__main__":
    unittest.main()
