import unittest

from consultation_registry import ConsultationRegistry


class ConsultationRegistryTests(unittest.TestCase):
    def setUp(self):
        self.registry = ConsultationRegistry()

    def test_receipt_is_scoped_to_request_and_hospital(self):
        self.registry.mark_completed("request-1", "H001")

        self.assertTrue(self.registry.has_completed("request-1", "H001"))
        self.assertFalse(self.registry.has_completed("request-1", "H002"))
        self.assertFalse(self.registry.has_completed("request-2", "H001"))

    def test_request_cannot_be_completed_for_a_different_hospital(self):
        self.registry.mark_completed("request-1", "H001")

        with self.assertRaises(ValueError):
            self.registry.mark_completed("request-1", "H002")

    def test_empty_receipt_fields_are_rejected(self):
        with self.assertRaises(ValueError):
            self.registry.mark_completed("", "H001")
        with self.assertRaises(ValueError):
            self.registry.mark_completed("request-1", "")


if __name__ == "__main__":
    unittest.main()
