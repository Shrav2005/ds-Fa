import unittest
from unittest.mock import Mock, patch

import coordinator
from grpc_service import _from_struct, _to_struct
from lamport_clock import LamportClock
from vector_clock import VectorClock


class VectorClockTests(unittest.TestCase):
    def test_local_events_increment_only_the_local_component(self):
        clock = VectorClock("H001")

        self.assertEqual(clock.tick(), {"H001": 1})
        self.assertEqual(clock.tick(), {"H001": 2})

    def test_receive_merges_remote_components_then_increments_local(self):
        clock = VectorClock("H001")

        self.assertEqual(
            clock.receive({"COORDINATOR": 4, "H002": 3}),
            {"H001": 1, "COORDINATOR": 4, "H002": 3},
        )
        self.assertEqual(
            clock.receive({"COORDINATOR": 2, "H002": 5}),
            {"H001": 2, "COORDINATOR": 4, "H002": 5},
        )

    def test_rpc_numeric_vector_components_are_normalized_from_protobuf_floats(self):
        decoded = _from_struct(_to_struct({
            "vector_timestamp": {"COORDINATOR": 4, "H002": 2},
        }))
        clock = VectorClock("H001")

        result = clock.receive(decoded["vector_timestamp"])

        self.assertEqual(
            result,
            {"H001": 1, "COORDINATOR": 4, "H002": 2},
        )

    def test_restart_uses_atomic_persisted_vector_advances(self):
        stored = {}

        def advance(node_id, observed):
            merged = dict(stored.get(node_id, {}))
            for component, count in observed.items():
                merged[component] = max(merged.get(component, 0), count)
            merged[node_id] = merged.get(node_id, 0) + 1
            stored[node_id] = merged
            return merged

        first_process = VectorClock("H001", advance)
        self.assertEqual(first_process.tick(), {"H001": 1})
        self.assertEqual(
            first_process.receive({"COORDINATOR": 7}),
            {"H001": 2, "COORDINATOR": 7},
        )
        restarted_process = VectorClock("H001", advance)
        self.assertEqual(
            restarted_process.tick(),
            {"H001": 3, "COORDINATOR": 7},
        )

    def test_invalid_vector_timestamp_is_rejected(self):
        clock = VectorClock("H001")
        for vector in (
            None,
            {"H002": -1},
            {"H002": 1.5},
            {"H002": True},
            {"bad.node": 1},
            {"$bad": 1},
        ):
            with self.subTest(vector=vector):
                with self.assertRaises(ValueError):
                    clock.receive(vector)

    def test_compare_identifies_causal_order_and_concurrent_events(self):
        first = {"COORDINATOR": 1, "H001": 2}
        later = {"COORDINATOR": 2, "H001": 2}
        concurrent = {"COORDINATOR": 1, "H001": 1, "H002": 1}

        self.assertEqual(VectorClock.compare(first, later), "before")
        self.assertEqual(VectorClock.compare(later, first), "after")
        self.assertEqual(VectorClock.compare(first, dict(first)), "equal")
        self.assertEqual(VectorClock.compare(first, concurrent), "concurrent")


class CoordinatorVectorClockTests(unittest.TestCase):
    def setUp(self):
        self.logged_vector = None
        self.store = Mock()
        self.store.append_log.side_effect = self.capture_log
        self.hospital = {
            "hospital_id": "H001",
            "hospital_name": "City Hospital",
            "host": "localhost",
            "rpc_port": 5001,
        }
        self.client = Mock()
        self.client.call.return_value = {
            "success": True,
            "lamport_timestamp": 8,
            "vector_clock": {"COORDINATOR": 6, "H001": 2},
        }
        self.patches = [
            patch.object(coordinator, "MONGO_STORE", self.store),
            patch.object(coordinator, "COORDINATOR_CLOCK", LamportClock("COORDINATOR")),
            patch.object(coordinator, "COORDINATOR_VECTOR_CLOCK", VectorClock("COORDINATOR")),
            patch.object(coordinator, "HospitalRpcClient", return_value=self.client),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    def capture_log(self, _source, _message, _lamport_timestamp, vector_clock):
        self.logged_vector = vector_clock

    def test_reservation_rpc_sends_and_merges_vector_timestamp(self):
        result = coordinator.reserve_bed_at_hospital(
            self.hospital,
            "ICU",
            "P-100",
        )

        self.assertTrue(result["success"])
        self.client.call.assert_called_once_with(
            "ReserveBed",
            bed_type="ICU",
            patient_id="P-100",
            lamport_timestamp=1,
            vector_timestamp={"COORDINATOR": 1},
        )
        self.assertEqual(
            self.logged_vector,
            {"COORDINATOR": 8, "H001": 2},
        )

    def test_release_rpc_sends_and_merges_vector_timestamp(self):
        result = coordinator.release_bed_at_hospital(self.hospital, "ICU")

        self.assertTrue(result["success"])
        self.client.call.assert_called_once_with(
            "ReleaseBed",
            bed_type="ICU",
            lamport_timestamp=1,
            vector_timestamp={"COORDINATOR": 1},
        )
        self.assertEqual(
            self.logged_vector,
            {"COORDINATOR": 8, "H001": 2},
        )

    def test_availability_rpc_sends_and_merges_vector_timestamp(self):
        self.client.call.return_value = {
            "beds": {"ICU": 2, "Emergency": 3, "General": 4},
            "lamport_timestamp": 8,
            "vector_clock": {"COORDINATOR": 6, "H001": 2},
        }

        availability = coordinator.get_hospital_availability(self.hospital)

        self.assertEqual(availability["ICU"], 2)
        self.client.call.assert_called_once_with(
            "GetBedAvailability",
            lamport_timestamp=1,
            vector_timestamp={"COORDINATOR": 1},
        )
        self.assertEqual(
            self.logged_vector,
            {"COORDINATOR": 8, "H001": 2},
        )


if __name__ == "__main__":
    unittest.main()
