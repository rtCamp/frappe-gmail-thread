import base64
import json
from unittest.mock import MagicMock, patch

import frappe
from frappe.tests import IntegrationTestCase, change_settings

from frappe_gmail_thread.api.pubsub import callback
from frappe_gmail_thread.tests import TEST_USER, make_test_user

PUBSUB_MODULE = "frappe_gmail_thread.api.pubsub"


def _build_request(message_dict):
    """Build a Pub/Sub request body with the inner message dict base64-encoded inside the standard {message: {data: ...}} envelope."""
    inner = json.dumps(message_dict).encode()
    encoded = base64.b64encode(inner).decode()
    return json.dumps({"message": {"data": encoded}})


class TestPubsubCallback(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)

    def test_returns_ok_without_enqueue_when_google_settings_disabled(self):
        """callback returns 'OK' without enqueuing when Google Settings.enable=0."""
        body = _build_request({"emailAddress": TEST_USER, "historyId": 100})
        request = MagicMock()
        request.get_data.return_value = body
        with change_settings(
            "Google Settings",
            enable=0,
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with (
                patch(f"{PUBSUB_MODULE}.frappe.request", request),
                patch(f"{PUBSUB_MODULE}.frappe.enqueue") as mock_enqueue,
            ):
                result = callback()
        self.assertEqual(result, "OK")
        mock_enqueue.assert_not_called()

    def test_returns_ok_without_enqueue_when_realtime_disabled(self):
        """callback returns 'OK' without enqueuing when custom_gmail_sync_in_realtime=0."""
        body = _build_request({"emailAddress": TEST_USER, "historyId": 100})
        request = MagicMock()
        request.get_data.return_value = body
        with change_settings(
            "Google Settings",
            enable=1,
            custom_gmail_sync_in_realtime=0,
            custom_gmail_pubsub_topic="topic",
        ):
            with (
                patch(f"{PUBSUB_MODULE}.frappe.request", request),
                patch(f"{PUBSUB_MODULE}.frappe.enqueue") as mock_enqueue,
            ):
                result = callback()
        self.assertEqual(result, "OK")
        mock_enqueue.assert_not_called()

    def test_returns_ok_without_enqueue_when_no_topic_configured(self):
        """callback returns 'OK' without enqueuing when custom_gmail_pubsub_topic is empty."""
        body = _build_request({"emailAddress": TEST_USER, "historyId": 100})
        request = MagicMock()
        request.get_data.return_value = body
        with change_settings(
            "Google Settings",
            enable=1,
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="",
        ):
            with (
                patch(f"{PUBSUB_MODULE}.frappe.request", request),
                patch(f"{PUBSUB_MODULE}.frappe.enqueue") as mock_enqueue,
            ):
                result = callback()
        self.assertEqual(result, "OK")
        mock_enqueue.assert_not_called()

    def test_enqueues_sync_job_for_matching_system_user(self):
        """callback decodes the base64 payload, looks up the System User by email, and enqueues a gmail_thread_sync_<user> job."""
        body = _build_request({"emailAddress": TEST_USER, "historyId": 500})
        request = MagicMock()
        request.get_data.return_value = body
        frappe.db.set_value("User", TEST_USER, "user_type", "System User")
        with change_settings(
            "Google Settings",
            enable=1,
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with (
                patch(f"{PUBSUB_MODULE}.frappe.request", request),
                patch(f"{PUBSUB_MODULE}.is_job_enqueued", return_value=False),
                patch(f"{PUBSUB_MODULE}.frappe.enqueue") as mock_enqueue,
            ):
                result = callback()
        self.assertEqual(result, "OK")
        mock_enqueue.assert_called_once()
        kwargs = mock_enqueue.call_args.kwargs
        self.assertEqual(kwargs["user"], TEST_USER)
        self.assertEqual(kwargs["job_name"], f"gmail_thread_sync_{TEST_USER}")

    def test_does_not_reenqueue_when_job_already_running(self):
        """callback skips enqueue when is_job_enqueued returns True for the user's sync job."""
        body = _build_request({"emailAddress": TEST_USER, "historyId": 500})
        request = MagicMock()
        request.get_data.return_value = body
        frappe.db.set_value("User", TEST_USER, "user_type", "System User")
        with change_settings(
            "Google Settings",
            enable=1,
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with (
                patch(f"{PUBSUB_MODULE}.frappe.request", request),
                patch(f"{PUBSUB_MODULE}.is_job_enqueued", return_value=True),
                patch(f"{PUBSUB_MODULE}.frappe.enqueue") as mock_enqueue,
            ):
                callback()
        mock_enqueue.assert_not_called()

    def test_logs_error_and_returns_ok_on_malformed_json(self):
        """A malformed JSON payload inside the base64 blob is captured via frappe.log_error and the endpoint still returns 'OK'."""
        bad_inner = base64.b64encode(b"not-json{{{").decode()
        body = json.dumps({"message": {"data": bad_inner}})
        request = MagicMock()
        request.get_data.return_value = body
        with change_settings(
            "Google Settings",
            enable=1,
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with (
                patch(f"{PUBSUB_MODULE}.frappe.request", request),
                patch(f"{PUBSUB_MODULE}.frappe.log_error") as mock_log,
                patch(f"{PUBSUB_MODULE}.frappe.enqueue") as mock_enqueue,
            ):
                result = callback()
        self.assertEqual(result, "OK")
        mock_log.assert_called_once()
        mock_enqueue.assert_not_called()
