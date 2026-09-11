from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase, change_settings

from frappe_gmail_thread.tasks.daily import enable_pubsub_everyday
from frappe_gmail_thread.tests import (
    TEST_USER,
    as_user,
    make_test_gmail_account,
    make_test_user,
)

DAILY_TASK_MODULE = "frappe_gmail_thread.tasks.daily"


class TestEnablePubsubEveryday(IntegrationTestCase):
    def test_returns_silently_when_google_settings_disabled(self):
        """enable_pubsub_everyday returns silently when Google Settings.enable=0."""
        with change_settings(
            "Google Settings",
            enable=0,
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with patch(f"{DAILY_TASK_MODULE}.enable_pubsub") as mock_enable:
                enable_pubsub_everyday()
        mock_enable.assert_not_called()

    def test_returns_silently_when_realtime_disabled(self):
        """enable_pubsub_everyday returns silently when custom_gmail_sync_in_realtime=0."""
        with change_settings(
            "Google Settings",
            enable=1,
            custom_gmail_sync_in_realtime=0,
            custom_gmail_pubsub_topic="topic",
        ):
            with patch(f"{DAILY_TASK_MODULE}.enable_pubsub") as mock_enable:
                enable_pubsub_everyday()
        mock_enable.assert_not_called()

    def test_returns_silently_when_topic_not_configured(self):
        """enable_pubsub_everyday returns silently when custom_gmail_pubsub_topic is empty."""
        with change_settings(
            "Google Settings",
            enable=1,
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="",
        ):
            with patch(f"{DAILY_TASK_MODULE}.enable_pubsub") as mock_enable:
                enable_pubsub_everyday()
        mock_enable.assert_not_called()

    def test_calls_enable_pubsub_for_every_enabled_account(self):
        """enable_pubsub_everyday calls enable_pubsub for every gmail_enabled=1 Gmail Account."""
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            account = make_test_gmail_account(linked_user=TEST_USER)
        frappe.db.set_value("Gmail Account", account.name, "gmail_enabled", 1)
        with change_settings(
            "Google Settings",
            enable=1,
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with patch(f"{DAILY_TASK_MODULE}.enable_pubsub") as mock_enable:
                enable_pubsub_everyday()
        mock_enable.assert_called_once()

    def test_logs_per_account_failure_and_continues(self):
        """enable_pubsub_everyday logs per-account failures with title='PubSub Error' and continues the loop."""
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            account = make_test_gmail_account(linked_user=TEST_USER)
        frappe.db.set_value("Gmail Account", account.name, "gmail_enabled", 1)
        with change_settings(
            "Google Settings",
            enable=1,
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with (
                patch(
                    f"{DAILY_TASK_MODULE}.enable_pubsub",
                    side_effect=RuntimeError("boom"),
                ),
                patch(f"{DAILY_TASK_MODULE}.frappe.log_error") as mock_log,
            ):
                enable_pubsub_everyday()
        mock_log.assert_called_once()
        kwargs = mock_log.call_args.kwargs
        self.assertEqual(kwargs["title"], "PubSub Error")
