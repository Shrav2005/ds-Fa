"""Write de-identified analytics inputs to an HDFS cluster via WebHDFS."""

import json
import os
import uuid
from datetime import datetime, timezone
from urllib.parse import quote, urlsplit, urlunsplit

import requests


class HdfsStorageError(RuntimeError):
    """Raised when WebHDFS is not configured or rejects an archive request."""


def _datanode_host_map(base_url):
    configured = os.getenv("HDFS_WEBHDFS_DATA_NODE_HOST_MAP")
    if configured:
        try:
            host_map = json.loads(configured)
        except json.JSONDecodeError as exc:
            raise HdfsStorageError("HDFS_WEBHDFS_DATA_NODE_HOST_MAP must be a JSON object.") from exc
        if not isinstance(host_map, dict) or any(
            not isinstance(host, str) or not isinstance(target, str)
            or not urlsplit(target).scheme or not urlsplit(target).netloc
            for host, target in host_map.items()
        ):
            raise HdfsStorageError(
                "HDFS_WEBHDFS_DATA_NODE_HOST_MAP must map DataNode hostnames to absolute URLs."
            )
        return host_map

    if urlsplit(base_url).hostname in {"localhost", "127.0.0.1"}:
        return {
            "datanode-1": "http://127.0.0.1:9865",
            "datanode-2": "http://127.0.0.1:9866",
        }
    return {}


def _rewrite_datanode_redirect(location, host_map):
    redirect = urlsplit(location)
    target = host_map.get(redirect.hostname)
    if not target:
        return location
    destination = urlsplit(target)
    return urlunsplit((
        destination.scheme,
        destination.netloc,
        redirect.path,
        redirect.query,
        redirect.fragment,
    ))


def archive_analytics_records(records):
    base_url = os.getenv("HDFS_WEBHDFS_URL")
    if not base_url:
        raise HdfsStorageError("HDFS is not configured. Start the analytics profile first.")

    archive_dir = os.getenv(
        "HDFS_ANALYTICS_DIRECTORY",
        "/hospital-analytics/exports",
    ).rstrip("/")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"analytics-{timestamp}-{uuid.uuid4().hex[:12]}.ndjson"
    hdfs_path = f"{archive_dir}/{filename}"
    encoded_directory = quote(archive_dir, safe="/")
    encoded_path = quote(hdfs_path, safe="/")
    webhdfs_url = f"{base_url.rstrip('/')}/webhdfs/v1"
    payload = "".join(f"{json.dumps(record)}\n" for record in records).encode("utf-8")
    timeout = (5, 30)
    user_name = os.getenv("HDFS_WEBHDFS_USER")
    query_user = {"user.name": user_name} if user_name else {}
    datanode_host_map = _datanode_host_map(base_url)

    try:
        mkdirs_response = requests.put(
            webhdfs_url + encoded_directory,
            params={"op": "MKDIRS", **query_user},
            timeout=timeout,
        )
        mkdirs_response.raise_for_status()
        mkdirs_result = mkdirs_response.json()
        if not isinstance(mkdirs_result, dict) or mkdirs_result.get("boolean") is not True:
            raise HdfsStorageError("WebHDFS could not create the analytics archive directory.")

        create_response = requests.put(
            webhdfs_url + encoded_path,
            params={
                "op": "CREATE",
                "overwrite": "false",
                "replication": os.getenv("HDFS_REPLICATION", "2"),
                **query_user,
            },
            data=payload,
            headers={"Content-Type": "application/octet-stream"},
            allow_redirects=False,
            timeout=timeout,
        )
        if create_response.status_code in {301, 302, 303, 307, 308}:
            redirect_url = create_response.headers.get("Location")
            if not redirect_url:
                raise HdfsStorageError("WebHDFS did not return a DataNode upload location.")
            create_response = requests.put(
                _rewrite_datanode_redirect(redirect_url, datanode_host_map),
                data=payload,
                headers={"Content-Type": "application/octet-stream"},
                timeout=timeout,
            )
        create_response.raise_for_status()
    except requests.RequestException as exc:
        raise HdfsStorageError(f"WebHDFS could not archive the report: {exc}") from exc

    return {
        "hdfs_path": hdfs_path,
        "file_name": filename,
    }
