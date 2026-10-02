# Match the existing unittest-based route test style.
# ruff: noqa: D107, INP001, PT009, RUF012, S105

from __future__ import annotations

import unittest
from io import BytesIO
from unittest.mock import patch

import botocore.exceptions

from s3_web_browser import create_app


class TestConfig:
    """Flask config for upload route tests."""

    TESTING = True
    SECRET_KEY = "test"
    AWS_KWARGS = {}
    AWS_BUCKET = None
    PAGE_ITEMS = 300


class FakeS3Client:
    """Record managed uploads and optionally fail selected object keys."""

    def __init__(self, failures: dict[str, str] | None = None) -> None:
        self.failures = failures or {}
        self.uploads: list[tuple[str, str, bytes, dict[str, str] | None]] = []

    def upload_fileobj(
        self,
        file_object: object,
        bucket_name: str,
        object_key: str,
        ExtraArgs: dict[str, str] | None = None,  # noqa: N803
    ) -> None:
        error_code = self.failures.get(object_key)
        if error_code:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": error_code, "Message": error_code}},
                "PutObject",
            )
        self.uploads.append((bucket_name, object_key, file_object.read(), ExtraArgs))


class UploadFilesTest(unittest.TestCase):
    """File upload route behavior."""

    def make_client(self, s3_client: FakeS3Client) -> object:
        app = create_app(TestConfig)
        client_patcher = patch("s3_web_browser.routes.boto3.client", return_value=s3_client)
        self.addCleanup(client_patcher.stop)
        client_patcher.start()
        return app.test_client()

    def test_uploads_multiple_files_to_current_folder(self) -> None:
        s3_client = FakeS3Client()
        client = self.make_client(s3_client)

        response = client.post(
            "/buckets/example/upload",
            data={
                "current_path": "reports/2026",
                "files": [
                    (BytesIO(b"summary"), "summary.txt", "text/plain"),
                    (BytesIO(b"csv"), "data.csv", "text/csv"),
                ],
            },
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/buckets/example/reports/2026/")
        self.assertEqual(
            s3_client.uploads,
            [
                ("example", "reports/2026/summary.txt", b"summary", {"ContentType": "text/plain"}),
                ("example", "reports/2026/data.csv", b"csv", {"ContentType": "text/csv"}),
            ],
        )

    def test_uses_basename_for_browser_supplied_paths(self) -> None:
        s3_client = FakeS3Client()
        client = self.make_client(s3_client)

        client.post(
            "/buckets/example/upload",
            data={"current_path": "safe/", "files": (BytesIO(b"data"), "../outside.txt")},
            content_type="multipart/form-data",
        )

        self.assertEqual(s3_client.uploads[0][1], "safe/outside.txt")

    def test_upload_requests_configured_server_side_encryption(self) -> None:
        s3_client = FakeS3Client()
        client = self.make_client(s3_client)
        client.application.config["AWS_SERVER_SIDE_ENCRYPTION"] = "AES256"

        client.post(
            "/buckets/example/upload",
            data={"files": (BytesIO(b"data"), "file.txt", "text/plain")},
            content_type="multipart/form-data",
        )

        self.assertEqual(
            s3_client.uploads[0][3],
            {"ContentType": "text/plain", "ServerSideEncryption": "AES256"},
        )

    def test_rejects_empty_file_selection(self) -> None:
        s3_client = FakeS3Client()
        client = self.make_client(s3_client)

        response = client.post(
            "/buckets/example/upload",
            data={"current_path": "reports/", "files": (BytesIO(), "")},
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(s3_client.uploads, [])
        with client.session_transaction() as session:
            self.assertIn(("error", "Select at least one valid file to upload."), session["_flashes"])

    def test_reports_partial_failure_and_keeps_successful_upload(self) -> None:
        s3_client = FakeS3Client({"reports/blocked.txt": "AccessDenied"})
        client = self.make_client(s3_client)

        client.post(
            "/buckets/example/upload",
            data={
                "current_path": "reports/",
                "files": [
                    (BytesIO(b"ok"), "ok.txt"),
                    (BytesIO(b"blocked"), "blocked.txt"),
                ],
            },
            content_type="multipart/form-data",
            follow_redirects=False,
        )

        self.assertEqual([upload[1] for upload in s3_client.uploads], ["reports/ok.txt"])
        with client.session_transaction() as session:
            messages = session["_flashes"]
        self.assertIn(("success", "Uploaded 1 file(s) to reports/."), messages)
        self.assertIn(("error", "You do not have permission to upload one or more selected files."), messages)

    def test_reports_missing_bucket(self) -> None:
        s3_client = FakeS3Client({"file.txt": "NoSuchBucket"})
        client = self.make_client(s3_client)

        client.post(
            "/buckets/example/upload",
            data={"files": (BytesIO(b"data"), "file.txt")},
            content_type="multipart/form-data",
        )

        with client.session_transaction() as session:
            self.assertIn(("error", "The specified bucket does not exist."), session["_flashes"])


if __name__ == "__main__":
    unittest.main()
