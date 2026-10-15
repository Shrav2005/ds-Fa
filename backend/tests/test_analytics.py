import json
import subprocess
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import coordinator
from analytics.streaming_common import aggregate_records
from mongo_service import MongoStore


BACKEND = Path(__file__).resolve().parents[1]
MAPPER = BACKEND / "analytics" / "mapper.py"
REDUCER = BACKEND / "analytics" / "reducer.py"


class StreamingAnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.records = [
            {
                "record_type": "request",
                "bed_type": "ICU",
                "hospital_name": "City Hospital",
                "created_at": "2026-10-07T09:15:00+00:00",
            },
            {
                "record_type": "request",
                "bed_type": "ICU",
                "hospital_name": "Metro Hospital",
                "created_at": "2026-10-07T09:40:00+00:00",
            },
            {
                "record_type": "reservation",
                "bed_type": "ICU",
                "hospital_name": "City Hospital",
            },
        ]

    def test_local_summary_aggregates_bed_type_hospital_and_peak_hour(self):
        summary = aggregate_records(self.records)

        self.assertEqual(summary["total_requests"], 2)
        self.assertEqual(summary["requests_by_bed_type"], {"ICU": 2})
        self.assertEqual(
            summary["requests_by_hospital"],
            {"City Hospital": 1, "Metro Hospital": 1},
        )
        self.assertEqual(summary["requests_by_hour"], {"09:00": 2})
        self.assertEqual(summary["reservations_by_hospital"], {"City Hospital": 1})
        self.assertEqual(summary["total_reservations"], 1)

    def test_hadoop_streaming_mapper_and_reducer_execute_as_a_pipeline(self):
        mapped = subprocess.run(
            [sys.executable, str(MAPPER)],
            input="".join(json.dumps(record) + "\n" for record in self.records),
            text=True,
            capture_output=True,
            check=True,
            cwd=BACKEND,
        )
        reduced = subprocess.run(
            [sys.executable, str(REDUCER)],
            input="\n".join(sorted(mapped.stdout.splitlines())) + "\n",
            text=True,
            capture_output=True,
            check=True,
            cwd=BACKEND,
        )

        self.assertIn("requests_by_bed_type|ICU\t2", reduced.stdout.splitlines())
        self.assertIn("requests_by_hour|09:00\t2", reduced.stdout.splitlines())
        self.assertIn("reservations_by_hospital|City Hospital\t1", reduced.stdout.splitlines())

    def test_mapper_rejects_unrecognized_record_types(self):
        with self.assertRaises(ValueError):
            aggregate_records([{"record_type": "patient"}])


class AnalyticsApiTests(unittest.TestCase):
    def setUp(self):
        self.records = [{
            "record_type": "request",
            "bed_type": "General",
            "hospital_name": "City Hospital",
            "created_at": "2026-10-07T09:15:00+00:00",
        }]
        self.patcher = patch.object(
            coordinator,
            "MONGO_STORE",
            type("Store", (), {"analytics_records": lambda _self: self.records})(),
        )
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.client = coordinator.app.test_client()

    def test_summary_endpoint_returns_mapreduce_dimensions(self):
        response = self.client.get("/api/analytics/summary")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["summary"]["requests_by_bed_type"], {"General": 1})
        self.assertEqual(response.get_json()["record_count"], 1)

    def test_export_endpoint_returns_ndjson_without_patient_identifiers(self):
        response = self.client.get("/api/analytics/export")
        record = json.loads(response.get_data(as_text=True))

        self.assertEqual(response.mimetype, "application/x-ndjson")
        self.assertEqual(record, self.records[0])
        self.assertNotIn("patient_id", record)
        self.assertNotIn("patient_name", record)


class MongoAnalyticsRecordsTests(unittest.TestCase):
    def test_naive_mongo_timestamps_are_exported_as_utc(self):
        store = object.__new__(MongoStore)
        timestamp = datetime(2026, 10, 11, 15, 49, 0)
        store.emergency_requests = Mock()
        store.emergency_requests.find.return_value = [{
            "bed_type": "ICU",
            "selected_hospital": {"hospital_name": "City Hospital"},
            "created_at": timestamp,
        }]
        store.reservations = Mock()
        store.reservations.find.return_value = [{
            "hospital_name": "City Hospital",
            "bed_type": "ICU",
            "created_at": timestamp,
        }]

        records = store.analytics_records()

        self.assertEqual(
            [record["created_at"] for record in records],
            [
                "2026-10-11T15:49:00+00:00",
                "2026-10-11T15:49:00+00:00",
            ],
        )
        self.assertEqual(aggregate_records(records)["requests_by_hour"], {"15:00": 1})

    def test_aware_mongo_timestamps_preserve_their_instant(self):
        store = object.__new__(MongoStore)
        timestamp = datetime(2026, 10, 11, 15, 49, tzinfo=timezone.utc)
        store.emergency_requests = Mock()
        store.emergency_requests.find.return_value = [{
            "bed_type": "ICU",
            "selected_hospital": {},
            "created_at": timestamp,
        }]
        store.reservations = Mock()
        store.reservations.find.return_value = []

        record = store.analytics_records()[0]

        self.assertEqual(record["created_at"], "2026-10-11T15:49:00+00:00")


if __name__ == "__main__":
    unittest.main()
