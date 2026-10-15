import json
import unittest
from unittest.mock import Mock, patch

import coordinator
from analytics.hdfs_storage import HdfsStorageError, archive_analytics_records
from analytics.serverless_function import app as function_app


class HdfsArchiveTests(unittest.TestCase):
    def test_archive_creates_directory_and_uploads_deidentified_json_lines(self):
        records = [{"record_type": "request", "bed_type": "ICU"}]
        mkdir_response = Mock()
        mkdir_response.raise_for_status.return_value = None
        mkdir_response.json.return_value = {"boolean": True}
        redirect_response = Mock(status_code=307)
        redirect_response.headers = {
            "Location": "http://datanode-1:9864/webhdfs/v1/hospital-analytics/exports/report.ndjson",
        }
        create_response = Mock()
        create_response.status_code = 201
        create_response.raise_for_status.return_value = None

        with patch.dict("os.environ", {
            "HDFS_WEBHDFS_URL": "http://namenode:9870",
            "HDFS_WEBHDFS_DATA_NODE_HOST_MAP": json.dumps({
                "datanode-1": "http://127.0.0.1:9865",
            }),
        }):
            with patch("analytics.hdfs_storage.requests.put", side_effect=[
                mkdir_response,
                redirect_response,
                create_response,
            ]) as put:
                result = archive_analytics_records(records)

        self.assertEqual(result["hdfs_path"].rsplit("/", 1)[0], "/hospital-analytics/exports")
        self.assertTrue(result["file_name"].endswith(".ndjson"))
        self.assertEqual(put.call_count, 3)
        self.assertEqual(put.call_args_list[1].kwargs["data"], (json.dumps(records[0]) + "\n").encode())
        self.assertEqual(put.call_args_list[1].kwargs["params"]["op"], "CREATE")
        self.assertFalse(put.call_args_list[1].kwargs["allow_redirects"])
        self.assertEqual(
            put.call_args_list[2].args[0],
            "http://127.0.0.1:9865/webhdfs/v1/hospital-analytics/exports/report.ndjson",
        )

    def test_archive_rewrites_compose_datanode_redirects_for_a_local_coordinator(self):
        mkdir_response = Mock()
        mkdir_response.raise_for_status.return_value = None
        mkdir_response.json.return_value = {"boolean": True}
        redirect_response = Mock(status_code=307)
        redirect_response.headers = {
            "Location": "http://datanode-2:9864/webhdfs/v1/hospital-analytics/exports/report.ndjson",
        }
        create_response = Mock(status_code=201)
        create_response.raise_for_status.return_value = None

        with patch.dict("os.environ", {"HDFS_WEBHDFS_URL": "http://localhost:9870"}, clear=True):
            with patch("analytics.hdfs_storage.requests.put", side_effect=[
                mkdir_response,
                redirect_response,
                create_response,
            ]) as put:
                archive_analytics_records([])

        self.assertEqual(
            put.call_args_list[2].args[0],
            "http://127.0.0.1:9866/webhdfs/v1/hospital-analytics/exports/report.ndjson",
        )

    def test_archive_reports_missing_hdfs_configuration(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(HdfsStorageError, "not configured"):
                archive_analytics_records([])

    def test_archive_uses_the_configured_webhdfs_simple_auth_identity(self):
        mkdir_response = Mock()
        mkdir_response.raise_for_status.return_value = None
        mkdir_response.json.return_value = {"boolean": True}
        redirect_response = Mock(status_code=307)
        redirect_response.headers = {
            "Location": "http://datanode-1:9864/webhdfs/v1/hospital-analytics/exports/report.ndjson",
        }
        create_response = Mock(status_code=201)
        create_response.raise_for_status.return_value = None

        with patch.dict("os.environ", {
            "HDFS_WEBHDFS_URL": "http://namenode:9870",
            "HDFS_WEBHDFS_USER": "root",
        }):
            with patch("analytics.hdfs_storage.requests.put", side_effect=[
                mkdir_response,
                redirect_response,
                create_response,
            ]) as put:
                archive_analytics_records([])

        self.assertEqual(put.call_args_list[0].kwargs["params"]["user.name"], "root")
        self.assertEqual(put.call_args_list[1].kwargs["params"]["user.name"], "root")


class ServerlessAnalyticsFunctionTests(unittest.TestCase):
    def setUp(self):
        self.client = function_app.test_client()

    def test_function_aggregates_input_records_on_demand(self):
        response = self.client.post("/", json={"records": [{
            "record_type": "request",
            "bed_type": "ICU",
            "hospital_name": "City Hospital",
            "created_at": "2026-10-07T09:15:00+00:00",
        }]})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["summary"]["total_requests"], 1)
        self.assertEqual(response.json["summary"]["requests_by_bed_type"], {"ICU": 1})

    def test_function_rejects_malformed_input(self):
        response = self.client.post("/", json={"records": "not-an-array"})

        self.assertEqual(response.status_code, 400)


class AnalyticsFunctionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.records = [{
            "record_type": "request",
            "bed_type": "General",
            "hospital_name": "City Hospital",
            "created_at": "2026-10-07T09:15:00+00:00",
        }]
        self.store_patch = patch.object(
            coordinator,
            "MONGO_STORE",
            type("Store", (), {"analytics_records": lambda _self: self.records})(),
        )
        self.store_patch.start()
        self.addCleanup(self.store_patch.stop)
        self.client = coordinator.app.test_client()

    def test_configured_function_is_used_for_the_report(self):
        with patch.dict("os.environ", {"ANALYTICS_FUNCTION_URL": "http://analytics-function/"}):
            with patch.object(coordinator.requests, "post") as post:
                post.return_value.json.return_value = {"summary": {"total_requests": 1}}
                post.return_value.raise_for_status.return_value = None

                response = self.client.get("/api/analytics/summary")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json["engine"], "Knative serverless function")
        self.assertEqual(response.json["summary"]["total_requests"], 1)
        post.assert_called_once()

    def test_function_failure_is_not_silently_replaced_with_local_results(self):
        with patch.dict("os.environ", {"ANALYTICS_FUNCTION_URL": "http://analytics-function/"}):
            with patch.object(
                coordinator.requests,
                "post",
                side_effect=coordinator.requests.ConnectionError,
            ):
                response = self.client.get("/api/analytics/summary")

        self.assertEqual(response.status_code, 502)
        self.assertFalse(response.json["success"])

    def test_hdfs_api_surfaces_missing_configuration(self):
        with patch.dict("os.environ", {}, clear=True):
            with patch("coordinator.archive_analytics_records", side_effect=HdfsStorageError(
                "HDFS is not configured. Start the analytics profile first."
            )):
                response = self.client.post("/api/analytics/hdfs/export")

        self.assertEqual(response.status_code, 503)
        self.assertIn("HDFS is not configured", response.json["message"])

    def test_hdfs_api_returns_the_archived_path(self):
        with patch("coordinator.archive_analytics_records", return_value={
            "hdfs_path": "/hospital-analytics/exports/report.ndjson",
            "file_name": "report.ndjson",
        }):
            response = self.client.post("/api/analytics/hdfs/export")

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json["record_count"], 1)
        self.assertEqual(
            response.json["hdfs_path"],
            "/hospital-analytics/exports/report.ndjson",
        )


if __name__ == "__main__":
    unittest.main()
