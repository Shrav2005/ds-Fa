import unittest
from unittest.mock import patch

import coordinator
from consultation_registry import ConsultationRegistry
from lamport_clock import LamportClock
from vector_clock import VectorClock


class FakeMongoStore:
    def __init__(self):
        self.requests = {}

    def create_emergency_request(self, request_data):
        self.requests[request_data["request_id"]] = dict(request_data)

    def append_log(self, source, message, lamport_timestamp=0, vector_clock=None):
        pass

    def claim_emergency_request(self, request_id):
        record = self.requests.get(request_id)
        if not record or record["status"] != "AWAITING_CONSULTATION":
            return None
        record["status"] = "RESERVING"
        return dict(record)

    def get_emergency_request(self, request_id):
        return self.requests.get(request_id)

    def hospital_patient_history(self, hospital_id):
        return [
            {
                "request_id": record["request_id"],
                "patient_id": record["patient_id"],
                "patient_name": record["patient_name"],
                "bed_type": record["bed_type"],
                "priority": record["priority"],
                "status": record["status"],
                "created_at": None,
            }
            for record in self.requests.values()
            if record.get("selected_hospital", {}).get("hospital_id") == hospital_id
            and record.get("status") in {"COMPLETED", "DISCHARGING", "DISCHARGED"}
        ]

    def claim_patient_discharge(self, request_id, hospital_id):
        record = self.requests.get(request_id)
        if (
            not record
            or record.get("selected_hospital", {}).get("hospital_id") != hospital_id
            or record.get("status") != "COMPLETED"
        ):
            return None
        record["status"] = "DISCHARGING"
        return dict(record)

    def finish_patient_discharge(self, request_id, status):
        self.requests[request_id]["status"] = status

    def complete_emergency_request(self, request_id, result, status="COMPLETED"):
        self.requests[request_id]["status"] = status
        self.requests[request_id]["result"] = result

    def cancel_emergency_request(self, request_id):
        record = self.requests.get(request_id)
        if not record or record["status"] != "AWAITING_CONSULTATION":
            return None
        record["status"] = "CANCELLED"
        return dict(record)


class OnlineBeaconRegistry:
    def get_status(self, hospital_id):
        return {
            "beacon_status": "ONLINE",
            "last_beacon_at": "2026-10-06T00:00:00+00:00",
            "heartbeat_age_seconds": 0,
        }


class EmergencyConsultationFlowTests(unittest.TestCase):
    def setUp(self):
        self.store = FakeMongoStore()
        self.registry = ConsultationRegistry()
        self.hospital = {
            "hospital_id": "H001",
            "hospital_name": "Example Hospital",
            "host": "localhost",
            "rpc_port": 5001,
            "http_port": 8001,
        }
        self.patches = [
            patch.object(coordinator, "MONGO_STORE", self.store),
            patch.object(coordinator, "COORDINATOR_CLOCK", LamportClock("TEST_COORDINATOR")),
            patch.object(coordinator, "COORDINATOR_VECTOR_CLOCK", VectorClock("TEST_COORDINATOR")),
            patch.object(coordinator, "HOSPITAL_BEACONS", OnlineBeaconRegistry()),
            patch.object(coordinator, "CONSULTATION_REGISTRY", self.registry),
            patch.object(coordinator, "get_hospital_config", return_value=[self.hospital]),
            patch.object(coordinator, "ping_hospital", return_value=True),
            patch.object(
                coordinator,
                "get_hospital_availability",
                return_value={"ICU": 2, "Emergency": 3, "General": 4},
            ),
            patch.object(
                coordinator,
                "reserve_bed_at_hospital",
                return_value={"success": True, "remaining_beds": 1},
            ),
            patch.object(coordinator, "notify_hospital_messages", return_value=True),
            patch.object(
                coordinator,
                "release_bed_at_hospital",
                return_value={"success": True},
            ),
        ]
        self.mocks = [patcher.start() for patcher in self.patches]
        self.addCleanup(self.stop_patches)
        self.client = coordinator.app.test_client()
    def stop_patches(self):
        for patcher in reversed(self.patches):
            patcher.stop()

    def plan_request(self):
        return self.client.post("/api/emergency/request", json={
            "patient_id": "P-100",
            "patient_name": "Test Patient",
            "bed_type": "ICU",
            "priority": "Critical",
        })

    def complete_consultation(self, request_data):
        return self.client.post(
            "/api/emergency/consultation-complete",
            json={
                "request_id": request_data["request_id"],
                "hospital_id": "H001",
            },
        )

    def test_request_waits_for_consultation_before_reserving(self):
        plan = self.plan_request()
        request_data = plan.get_json()
        reserve = self.mocks[8]

        self.assertEqual(plan.status_code, 200)
        self.assertEqual(request_data["status"], "CONSULTATION_REQUIRED")
        self.assertEqual(request_data["selected_hospital"]["hospital_id"], "H001")
        reserve.assert_not_called()

        early_confirmation = self.client.post(
            "/api/emergency/confirm",
            json={"request_id": request_data["request_id"]},
        )
        self.assertEqual(early_confirmation.status_code, 409)
        reserve.assert_not_called()

        completion = self.complete_consultation(request_data)
        self.assertEqual(completion.status_code, 200)
        confirmation = self.client.post(
            "/api/emergency/confirm",
            json={"request_id": request_data["request_id"]},
        )
        self.assertEqual(confirmation.status_code, 200)
        self.assertTrue(confirmation.get_json()["success"])
        reserve.assert_called_once()

        repeated_confirmation = self.client.post(
            "/api/emergency/confirm",
            json={"request_id": request_data["request_id"]},
        )
        self.assertEqual(repeated_confirmation.get_json(), confirmation.get_json())
        reserve.assert_called_once()

    def test_pending_request_can_be_cancelled_without_reserving(self):
        plan = self.plan_request()
        request_data = plan.get_json()

        cancellation = self.client.post(
            "/api/emergency/cancel",
            json={"request_id": request_data["request_id"]},
        )

        self.assertEqual(cancellation.status_code, 200)
        self.assertEqual(self.store.requests[request_data["request_id"]]["status"], "CANCELLED")
        self.mocks[8].assert_not_called()

    def test_consultation_receipt_rejects_wrong_hospital(self):
        request_data = self.plan_request().get_json()
        response = self.client.post(
            "/api/emergency/consultation-complete",
            json={
                "request_id": request_data["request_id"],
                "hospital_id": "H002",
            },
        )

        self.assertEqual(response.status_code, 403)
        self.assertFalse(self.registry.has_completed(request_data["request_id"]))

    def test_old_http_call_signaling_routes_are_removed(self):
        response = self.client.post("/api/calls/presence", json={})
        self.assertEqual(response.status_code, 404)

    def create_completed_request(self):
        request_data = self.plan_request().get_json()
        self.complete_consultation(request_data)
        self.client.post(
            "/api/emergency/confirm",
            json={"request_id": request_data["request_id"]},
        )
        return self.store.requests[request_data["request_id"]]

    def test_hospital_history_returns_only_that_hospitals_confirmed_patients(self):
        patient = self.create_completed_request()
        other_hospital_patient = dict(patient)
        other_hospital_patient.update({
            "request_id": "another-hospital-request",
            "selected_hospital": {
                "hospital_id": "H002",
                "hospital_name": "Another Hospital",
            },
        })
        self.store.requests[other_hospital_patient["request_id"]] = other_hospital_patient

        response = self.client.get("/api/hospitals/H001/patients")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item["request_id"] for item in response.get_json()["patients"]],
            [patient["request_id"]],
        )
        self.assertEqual(self.client.get("/api/hospitals/H999/patients").status_code, 404)

    def test_discharge_releases_bed_and_keeps_patient_record(self):
        patient = self.create_completed_request()

        response = self.client.post("/api/bed/deallocate", json={
            "hospital_id": "H001",
            "request_id": patient["request_id"],
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.store.requests[patient["request_id"]]["status"], "DISCHARGED")
        history = self.client.get("/api/hospitals/H001/patients").get_json()["patients"]
        self.assertEqual(history[0]["status"], "DISCHARGED")
        self.mocks[10].assert_called_once_with(self.hospital, "ICU")
        repeated = self.client.post("/api/bed/deallocate", json={
            "hospital_id": "H001",
            "request_id": patient["request_id"],
        })
        self.assertTrue(repeated.get_json()["success"])
        self.mocks[10].assert_called_once()

    def test_failed_discharge_keeps_patient_placement_active(self):
        patient = self.create_completed_request()
        self.mocks[10].return_value = {"success": False, "message": "Hospital is offline."}

        response = self.client.post("/api/bed/deallocate", json={
            "hospital_id": "H001",
            "request_id": patient["request_id"],
        })

        self.assertEqual(response.status_code, 502)
        self.assertEqual(self.store.requests[patient["request_id"]]["status"], "COMPLETED")


if __name__ == "__main__":
    unittest.main()
