import unittest
from unittest.mock import Mock

from grpc_service import RemoteHospitalObject


class RemoteHospitalObjectTests(unittest.TestCase):
    def setUp(self):
        self.hospital = {
            "hospital_id": "H002",
            "host": "hospital-b",
            "rpc_port": 5002,
        }
        self.client = Mock()
        self.client.call.return_value = {"success": True}
        self.remote = RemoteHospitalObject(self.hospital, self.client)

    def test_invocation_targets_the_bound_remote_hospital_operation(self):
        result = self.remote.invoke(
            "ReserveBed",
            bed_type="ICU",
            patient_id="P-100",
        )

        self.assertEqual(self.remote.hospital_id, "H002")
        self.assertEqual(result, {"success": True})
        self.client.call.assert_called_once_with(
            "ReserveBed",
            bed_type="ICU",
            patient_id="P-100",
        )

    def test_rejects_operations_outside_the_hospital_object_interface(self):
        with self.assertRaises(ValueError):
            self.remote.invoke("DeleteAllData")

        self.client.call.assert_not_called()

    def test_context_manager_closes_the_transport(self):
        with self.remote as remote:
            self.assertIs(remote, self.remote)

        self.client.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
