import unittest
from unittest.mock import patch

import coordinator
from beacon_protocol import HospitalBeaconRegistry, send_hospital_beacons
from lamport_clock import LamportClock
from rpc_service import build_hospital_service


class BeaconProtocolTests(unittest.TestCase):
    def test_registry_transitions_from_waiting_to_online_then_stale(self):
        registry = HospitalBeaconRegistry(timeout=8)

        waiting = registry.get_status("H001", now=100)
        with patch("beacon_protocol.time.monotonic", return_value=100):
            online = registry.record("H001", "City Hospital")
        stale = registry.get_status("H001", now=109)

        self.assertEqual(waiting["beacon_status"], "WAITING")
        self.assertEqual(online["beacon_status"], "ONLINE")
        self.assertEqual(stale["beacon_status"], "STALE")
        self.assertEqual(stale["heartbeat_age_seconds"], 9)

    def test_sender_posts_beacons_until_stop_event_is_set(self):
        class OneBeaconEvent:
            def __init__(self):
                self.stopped = False
                self.waited_intervals = []

            def is_set(self):
                return self.stopped

            def wait(self, interval):
                self.waited_intervals.append(interval)
                self.stopped = True

        class Response:
            @staticmethod
            def raise_for_status():
                pass

        stop_event = OneBeaconEvent()
        make_beacon = lambda: {"hospital_id": "H001"}
        with patch("beacon_protocol.requests.post", return_value=Response()) as post:
            send_hospital_beacons(
                "http://localhost:5000/",
                make_beacon,
                stop_event,
                interval=2,
            )

        post.assert_called_once_with(
            "http://localhost:5000/api/hospitals/beacon",
            json={"hospital_id": "H001"},
            timeout=2,
        )
        self.assertEqual(stop_event.waited_intervals, [2])

    def test_hospital_service_creates_clocked_beacon(self):
        state = {
            "hospital_id": "H001",
            "hospital_name": "City Hospital",
            "status": "ONLINE",
            "beds": {"ICU": 2, "Emergency": 3, "General": 4},
        }
        operations = build_hospital_service(state, "localhost", 5001)["functions"]

        first = operations["create_beacon"]()
        second = operations["create_beacon"]()

        self.assertEqual(first["hospital_id"], "H001")
        self.assertEqual(first["hospital_name"], "City Hospital")
        self.assertEqual(second["lamport_timestamp"], first["lamport_timestamp"] + 1)


class BeaconCoordinatorApiTests(unittest.TestCase):
    def setUp(self):
        self.hospital = {
            "hospital_id": "H001",
            "hospital_name": "City Hospital",
            "host": "localhost",
            "rpc_port": 5001,
            "http_port": 6001,
        }
        self.registry = HospitalBeaconRegistry()
        self.patches = [
            patch.object(coordinator, "get_hospital_config", return_value=[self.hospital]),
            patch.object(coordinator, "HOSPITAL_BEACONS", self.registry),
            patch.object(coordinator, "COORDINATOR_CLOCK", LamportClock("TEST_COORDINATOR")),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = coordinator.app.test_client()

    def test_configured_hospital_beacon_is_accepted_and_visible(self):
        response = self.client.post("/api/hospitals/beacon", json={
            "hospital_id": "H001",
            "hospital_name": "City Hospital",
            "lamport_timestamp": 12,
        })

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["success"])
        self.assertEqual(
            self.registry.get_status("H001")["beacon_status"],
            "ONLINE",
        )

    def test_unknown_hospital_and_invalid_clock_are_rejected(self):
        unknown = self.client.post("/api/hospitals/beacon", json={
            "hospital_id": "H999",
            "hospital_name": "Unknown",
            "lamport_timestamp": 1,
        })
        invalid_clock = self.client.post("/api/hospitals/beacon", json={
            "hospital_id": "H001",
            "hospital_name": "City Hospital",
            "lamport_timestamp": -1,
        })

        self.assertEqual(unknown.status_code, 404)
        self.assertEqual(invalid_clock.status_code, 400)

    def test_status_requires_both_rpc_response_and_recent_beacon(self):
        with patch("beacon_protocol.time.monotonic", return_value=90):
            self.registry.record("H001", "City Hospital")
        with (
            patch.object(coordinator, "get_hospital_status", return_value={
                "hospital_id": "H001",
                "hospital_name": "City Hospital",
                "status": "ONLINE",
                "beds": {"ICU": 2, "Emergency": 3, "General": 4},
                "lamport_timestamp": 14,
            }),
            patch("beacon_protocol.time.monotonic", return_value=100),
        ):
            statuses = coordinator.get_all_hospital_statuses()

        self.assertEqual(statuses[0]["status"], "OFFLINE")
        self.assertEqual(statuses[0]["rpc_status"], "ONLINE")
        self.assertEqual(statuses[0]["beacon_status"], "STALE")
        self.assertEqual(statuses[0]["beds"]["ICU"], 0)


if __name__ == "__main__":
    unittest.main()
