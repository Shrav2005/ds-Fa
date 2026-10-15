import threading
import time
import unittest
from unittest.mock import patch

import coordinator
from mutex import CentralizedMutex


class CentralizedMutexTests(unittest.TestCase):
    def run_queue(self, policy, waiters):
        mutex = CentralizedMutex(policy)
        holder_entered = threading.Event()
        release_holder = threading.Event()
        order = []

        def hold_lock():
            with mutex.acquire("holder", "Normal", 1):
                holder_entered.set()
                release_holder.wait(3)

        holder = threading.Thread(target=hold_lock)
        holder.start()
        self.assertTrue(holder_entered.wait(2))

        workers = []
        for request_id, priority, timestamp in waiters:
            def request_lock(rid=request_id, level=priority, stamp=timestamp):
                with mutex.acquire(rid, level, stamp):
                    order.append(rid)

            worker = threading.Thread(target=request_lock)
            workers.append(worker)
            worker.start()

        deadline = time.monotonic() + 2
        while mutex.status()["waiting"] < len(waiters) and time.monotonic() < deadline:
            time.sleep(0.005)
        waiting_count = mutex.status()["waiting"]
        release_holder.set()
        holder.join(2)
        for worker in workers:
            worker.join(2)

        self.assertEqual(waiting_count, len(waiters))
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        return order

    def test_fair_policy_uses_lamport_order_even_if_requests_arrive_later(self):
        order = self.run_queue("FAIR", [
            ("later-event", "Normal", 30),
            ("earlier-event", "Normal", 20),
        ])

        self.assertEqual(order, ["earlier-event", "later-event"])

    def test_priority_policy_allows_critical_to_jump_a_normal_waiter(self):
        order = self.run_queue("PRIORITY", [
            ("normal", "Normal", 10),
            ("critical", "Critical", 20),
        ])

        self.assertEqual(order, ["critical", "normal"])

    def test_invalid_policy_and_reservation_metadata_are_rejected(self):
        mutex = CentralizedMutex()

        with self.assertRaises(ValueError):
            mutex.set_policy("RANDOM")
        with self.assertRaises(ValueError):
            mutex.acquire("request", "Urgent", 1).__enter__()


class MutexPolicyApiTests(unittest.TestCase):
    def setUp(self):
        class Store:
            @staticmethod
            def append_log(*_args, **_kwargs):
                pass

        self.mutex = CentralizedMutex()
        self.patches = [
            patch.object(coordinator, "RESERVATION_MUTEX", self.mutex),
            patch.object(coordinator, "MONGO_STORE", Store()),
        ]
        for patcher in self.patches:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = coordinator.app.test_client()

    def test_policy_endpoint_switches_ordering_and_rejects_unknown_values(self):
        fair = self.client.get("/api/mutex/policy")
        priority = self.client.post("/api/mutex/policy", json={"policy": "PRIORITY"})
        invalid = self.client.post("/api/mutex/policy", json={"policy": "UNSAFE"})

        self.assertEqual(fair.get_json()["policy"], "FAIR")
        self.assertEqual(priority.get_json()["policy"], "PRIORITY")
        self.assertIn("move ahead", priority.get_json()["description"])
        self.assertEqual(invalid.status_code, 400)


if __name__ == "__main__":
    unittest.main()
