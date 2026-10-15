import unittest

from lamport_clock import LamportClock
from rpc_service import build_hospital_service


class LamportClockTests(unittest.TestCase):
    def test_local_events_increment_the_clock(self):
        clock = LamportClock("H001")

        self.assertEqual(clock.tick(), 1)
        self.assertEqual(clock.tick(), 2)

    def test_receiving_a_message_advances_beyond_its_timestamp(self):
        clock = LamportClock("H001")

        self.assertEqual(clock.receive(9), 10)
        self.assertEqual(clock.tick(), 11)
        self.assertEqual(clock.receive(3), 12)

    def test_persisted_clock_is_atomic_and_survives_new_clock_instances(self):
        stored = {}

        def advance(node_id, observed):
            stored[node_id] = max(stored.get(node_id, 0), observed) + 1
            return stored[node_id]

        first_process = LamportClock("H001", advance)
        self.assertEqual(first_process.tick(), 1)
        self.assertEqual(first_process.receive(8), 9)

        restarted_process = LamportClock("H001", advance)
        self.assertEqual(restarted_process.tick(), 10)
        self.assertEqual(stored["H001"], 10)

    def test_invalid_remote_timestamps_are_rejected(self):
        clock = LamportClock("H001")
        for timestamp in (-1, 1.5, True, "3"):
            with self.subTest(timestamp=timestamp):
                with self.assertRaises(ValueError):
                    clock.receive(timestamp)

    def test_hospital_reservation_merges_rpc_clock_and_stamps_event(self):
        class Store:
            def __init__(self):
                self.clock = 0
                self.reservation_clock = None
                self.reservation_vector = None

            def next_lamport_timestamp(self, *args):
                observed = args[1]
                self.clock = max(self.clock, observed) + 1
                return self.clock

            def next_vector_clock(self, node_id, observed):
                next_vector = dict(observed)
                next_vector[node_id] = next_vector.get(node_id, 0) + 1
                return next_vector

            def reserve_bed(self, *args):
                lamport_timestamp = args[-2]
                vector_clock = args[-1]
                self.reservation_clock = lamport_timestamp
                self.reservation_vector = vector_clock
                return 2

        state = {
            "hospital_id": "H001",
            "hospital_name": "City Hospital",
            "status": "ONLINE",
            "beds": {"ICU": 3, "Emergency": 2, "General": 1},
        }
        store = Store()
        operations = build_hospital_service(state, "localhost", 5001, store)["functions"]

        result = operations["reserve_bed"](
            "ICU",
            "P-100",
            lamport_timestamp=7,
            vector_timestamp={"COORDINATOR": 7},
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["lamport_timestamp"], 8)
        self.assertEqual(result["node_id"], "H001")
        self.assertEqual(store.reservation_clock, 8)
        self.assertEqual(result["vector_clock"], {"H001": 1, "COORDINATOR": 7})
        self.assertEqual(store.reservation_vector, result["vector_clock"])

    def test_hospital_status_merges_coordinator_clock_and_reports_local_clock(self):
        class Store:
            def __init__(self):
                self.clock = 0

            def next_lamport_timestamp(self, node_id, observed):
                self.clock = max(self.clock, observed) + 1
                return self.clock

            def next_vector_clock(self, node_id, observed):
                vector = dict(observed)
                vector[node_id] = vector.get(node_id, 0) + 1
                return vector

            def get_hospital(self, hospital_id):
                return {"beds": {"ICU": 2, "Emergency": 3, "General": 4}}

        state = {
            "hospital_id": "H001",
            "hospital_name": "City Hospital",
            "status": "ONLINE",
            "beds": {"ICU": 0, "Emergency": 0, "General": 0},
        }
        operations = build_hospital_service(
            state,
            "localhost",
            5001,
            Store(),
        )["functions"]

        result = operations["get_hospital_status"](
            lamport_timestamp=11,
            vector_timestamp={"COORDINATOR": 4},
        )

        self.assertEqual(result["beds"]["ICU"], 2)
        self.assertEqual(result["lamport_timestamp"], 12)
        self.assertEqual(
            result["vector_clock"],
            {"H001": 1, "COORDINATOR": 4},
        )
        self.assertIn("hospital_received_ms", result["clock_sample"])
        self.assertIn("hospital_sent_ms", result["clock_sample"])


if __name__ == "__main__":
    unittest.main()
