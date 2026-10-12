from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase, change_settings

from frappe_gmail_thread.api.oauth import GmailAuthorizationExpiredError
from frappe_gmail_thread.tasks.daily import enable_pubsub_everyday
from frappe_gmail_thread.tests import (
    TEST_USER,
    TEST_USER_2,
    as_user,
    make_test_gmail_account,
    make_test_user,
    set_password_field,
)

DAILY_TASK_MODULE = "frappe_gmail_thread.tasks.daily"
REALTIME_ON = {
    "enable": 1,
    "custom_gmail_sync_in_realtime": 1,
    "custom_gmail_pubsub_topic": "topic",
}


def _account(user, *, refresh_token="rt"):
    """Return the name of an enabled Gmail Account for `user` with the given refresh token."""
    make_test_user(user)
    with as_user(user):
        account = make_test_gmail_account(linked_user=user)
    frappe.db.set_value("Gmail Account", account.name, "gmail_enabled", 1)
    set_password_field("Gmail Account", account.name, "refresh_token", refresh_token)
    return account.name


def _called_for(mock):
    return [c.args[0].name for c in mock.call_args_list]


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

    def test_renews_watch_of_every_enabled_authorized_account(self):
        """enable_pubsub_everyday renews the watch of each enabled account that has a refresh token."""
        account = _account(TEST_USER)
        with change_settings("Google Settings", **REALTIME_ON):
            with patch(f"{DAILY_TASK_MODULE}.enable_pubsub") as mock_enable:
                enable_pubsub_everyday()
        self.assertEqual(_called_for(mock_enable).count(account), 1)

    def test_skips_account_without_refresh_token(self):
        """An enabled account that isn't authorized has no watch to renew and must not be logged as a failure."""
        account = _account(TEST_USER, refresh_token="")
        with change_settings("Google Settings", **REALTIME_ON):
            with (
                patch(f"{DAILY_TASK_MODULE}.enable_pubsub") as mock_enable,
                patch(f"{DAILY_TASK_MODULE}.frappe.log_error") as mock_log,
            ):
                enable_pubsub_everyday()
        self.assertNotIn(account, _called_for(mock_enable))
        mock_log.assert_not_called()

    def test_logs_per_account_failure_and_continues(self):
        """enable_pubsub_everyday logs per-account failures with title='PubSub Error' and continues the loop."""
        failing = _account(TEST_USER)
        other = _account(TEST_USER_2)

        def enable(account):
            if account.name == failing:
                raise RuntimeError("boom")

        with change_settings("Google Settings", **REALTIME_ON):
            with (
                patch(
                    f"{DAILY_TASK_MODULE}.enable_pubsub", side_effect=enable
                ) as mock_enable,
                patch(f"{DAILY_TASK_MODULE}.frappe.log_error") as mock_log,
            ):
                enable_pubsub_everyday()
        mock_log.assert_called_once()
        self.assertEqual(mock_log.call_args.kwargs["title"], "PubSub Error")
        self.assertIn(other, _called_for(mock_enable))

    def test_expired_authorization_is_handled_not_logged(self):
        """A revoked grant found while renewing a watch clears the token and alerts the owner instead of logging an error."""
        account = _account(TEST_USER)

        def enable(gmail_account):
            if gmail_account.name == account:
                frappe.throw("expired", GmailAuthorizationExpiredError)

        with change_settings("Google Settings", **REALTIME_ON):
            with (
                patch(f"{DAILY_TASK_MODULE}.enable_pubsub", side_effect=enable),
                patch(f"{DAILY_TASK_MODULE}.frappe.log_error") as mock_log,
                patch(
                    f"{DAILY_TASK_MODULE}.handle_expired_authorization"
                ) as mock_handle,
            ):
                enable_pubsub_everyday()
        mock_log.assert_not_called()
        mock_handle.assert_called_once()
        self.assertEqual(mock_handle.call_args.args[0].name, account)
