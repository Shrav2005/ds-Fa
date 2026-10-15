import socket
import unittest
from unittest.mock import patch

import coordinator
import grpc
from clock_sync import (
    ClockAdjustment,
    calculate_berkeley_adjustments,
    estimate_clock_offset,
)
from grpc_service import HospitalRpcClient, start_grpc_server
from rpc_service import build_hospital_service


class ClockSyncTests(unittest.TestCase):
    def test_estimates_offset_and_round_trip_delay_from_four_timestamps(self):
        sample = estimate_clock_offset(
            coordinator_sent_ms=1000,
            hospital_received_ms=1012,
            hospital_sent_ms=1014,
            coordinator_received_ms=1006,
        )

        self.assertEqual(sample["estimated_offset_ms"], 10)
        self.assertEqual(sample["round_trip_ms"], 4)

    def test_rejects_invalid_or_reversed_node_timestamps(self):
        with self.assertRaises(ValueError):
            estimate_clock_offset(1000, 1012, 1011, 1006)
        with self.assertRaises(ValueError):
            estimate_clock_offset(1000, 1012, 1014, True)

    def test_calculates_berkeley_average_and_per_node_adjustments(self):
        result = calculate_berkeley_adjustments({
            "COORDINATOR": 0,
            "H001": 10,
            "H002": -4,
        })

        self.assertEqual(result["average_offset_ms"], 2)
        self.assertEqual(result["adjustments_ms"], {
            "COORDINATOR": 2,
            "H001": -8,
            "H002": 6,
        })

    def test_rejects_invalid_berkeley_offset_maps(self):
        for offsets in ({}, {"H001": True}, {"H001": float("inf")}, []):
            with self.subTest(offsets=offsets):
                with self.assertRaises(ValueError):
                    calculate_berkeley_adjustments(offsets)


class ClockAdjustmentTests(unittest.TestCase):
    def test_clock_adjustment_changes_simulated_time_not_host_time(self):
        clock = ClockAdjustment()
        with patch("clock_sync.time.time_ns", return_value=1_000_000_000):
            self.assertEqual(clock.now_ms(), 1000)
            clock.set_ms(12.5)
            self.assertEqual(clock.now_ms(), 1012.5)
        self.assertEqual(clock.get_ms(), 12.5)

    def test_hospital_rpc_accepts_berkeley_adjustment_and_reports_it(self):
        state = {
            "hospital_id": "H001",
            "hospital_name": "City Hospital",
            "status": "ONLINE",
            "beds": {"ICU": 2, "Emergency": 3, "General": 4},
        }
        operations = build_hospital_service(state, "localhost", 5001)["functions"]

        result = operations["adjust_clock"](clock_adjustment_ms=8.5)
        status = operations["get_hospital_status"]()

        self.assertTrue(result["success"])
        self.assertEqual(result["clock_adjustment_ms"], 8.5)
        self.assertEqual(status["clock_adjustment_ms"], 8.5)

    def test_hospital_grpc_service_exposes_berkeley_clock_adjustment(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        state = {
            "hospital_id": "H001",
            "hospital_name": "City Hospital",
            "status": "ONLINE",
            "beds": {"ICU": 2, "Emergency": 3, "General": 4},
        }
        functions = build_hospital_service(state, "127.0.0.1", port)["functions"]
        server = start_grpc_server("127.0.0.1", port, functions)
        client = HospitalRpcClient("127.0.0.1", port)
        try:
            result = client.call("AdjustClock", clock_adjustment_ms=7.25)
            status = client.call("GetHospitalStatus")
            with self.assertRaises(grpc.RpcError) as error:
                client.call("AdjustClock", clock_adjustment_ms=True)
        finally:
            client.close()
            server.stop(0).wait()

        self.assertTrue(result["success"])
        self.assertEqual(result["clock_adjustment_ms"], 7.25)
        self.assertEqual(status["clock_adjustment_ms"], 7.25)
        self.assertEqual(error.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)


class BerkeleyClockSyncApiTests(unittest.TestCase):
    def setUp(self):
        self.hospitals = [
            {
                "hospital_id": "H001",
                "host": "localhost",
                "rpc_port": 5001,
            },
            {
                "hospital_id": "H002",
                "host": "localhost",
                "rpc_port": 5002,
            },
        ]
        self.statuses = [
            {
                "hospital_id": "H001",
                "status": "ONLINE",
                "clock_sync": {"estimated_offset_ms": 10},
                "clock_adjustment_ms": 0,
            },
            {
                "hospital_id": "H002",
                "status": "ONLINE",
                "clock_sync": {"estimated_offset_ms": -4},
                "clock_adjustment_ms": 0,
            },
        ]
        self.adjustment = ClockAdjustment()
        self.patches = [
            patch.object(coordinator, "get_hospital_config", return_value=self.hospitals),
            patch.object(coordinator, "get_all_hospital_statuses", return_value=self.statuses),
            patch.object(coordinator, "COORDINATOR_CLOCK_ADJUSTMENT", self.adjustment),
            patch.object(coordinator, "add_log"),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = coordinator.app.test_client()

    def test_sync_applies_mean_adjustment_to_coordinator_and_hospitals(self):
        rpc_results = {
            "H001": {"success": True, "clock_adjustment_ms": -8},
            "H002": {"success": True, "clock_adjustment_ms": 6},
        }

        with patch.object(
            coordinator,
            "call_hospital_rpc",
            side_effect=lambda hospital, method, **payload: rpc_results[hospital["hospital_id"]],
        ) as call_rpc:
            response = self.client.post("/api/clocks/berkeley-sync", json={})

        result = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(result["success"])
        self.assertEqual(result["average_offset_ms"], 2)
        self.assertEqual(result["coordinator_adjustment_ms"], 2)
        self.assertEqual(self.adjustment.get_ms(), 2)
        self.assertEqual(
            [call.kwargs["clock_adjustment_ms"] for call in call_rpc.call_args_list],
            [-8, 6],
        )

    def test_sync_reports_partial_hospital_failures(self):
        with patch.object(
            coordinator,
            "call_hospital_rpc",
            side_effect=[
                {"success": True, "clock_adjustment_ms": -8},
                RuntimeError("Hospital RPC unavailable."),
            ],
        ):
            response = self.client.post("/api/clocks/berkeley-sync", json={})

        result = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(result["success"])
        self.assertEqual(result["failures"][0]["hospital_id"], "H002")
        self.assertEqual(self.adjustment.get_ms(), 2)

    def test_sync_requires_at_least_one_online_hospital_with_an_offset(self):
        with (
            patch.object(coordinator, "get_all_hospital_statuses", return_value=[{
                "hospital_id": "H001",
                "status": "OFFLINE",
                "clock_sync": None,
            }]),
            patch.object(coordinator, "call_hospital_rpc") as call_rpc,
        ):
            response = self.client.post("/api/clocks/berkeley-sync", json={})

        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.get_json()["success"])
        call_rpc.assert_not_called()


if __name__ == "__main__":
    unittest.main()
