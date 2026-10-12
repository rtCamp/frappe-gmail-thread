from unittest.mock import MagicMock, patch

import frappe
from frappe.tests import IntegrationTestCase, change_settings

from frappe_gmail_thread.doc_events.google_settings import (
    on_update,
    update_gmail_watches,
)
from frappe_gmail_thread.tests import (
    TEST_USER,
    TEST_USER_2,
    TEST_USER_3,
    as_user,
    make_test_gmail_account,
    make_test_user,
    set_password_field,
)

GOOGLE_SETTINGS_DOC_EVENT_MODULE = "frappe_gmail_thread.doc_events.google_settings"
DISABLED_USER = "test_fgt_disabled_watch@example.com"
SECOND_USER = "test_fgt_second_watch@example.com"


def _account(user, *, gmail_enabled, refresh_token):
    """Return the name of the Gmail Account linked to `user`, forced into the given state."""
    make_test_user(user)
    with as_user(user):
        account = make_test_gmail_account(linked_user=user)
    frappe.db.set_value("Gmail Account", account.name, "gmail_enabled", gmail_enabled)
    set_password_field("Gmail Account", account.name, "refresh_token", refresh_token)
    return account.name


def _settings(*, realtime, changed=True):
    gs_doc = MagicMock()
    gs_doc.has_value_changed.return_value = changed
    gs_doc.custom_gmail_sync_in_realtime = realtime
    return gs_doc


def _called_for(mock):
    return {c.args[0].name for c in mock.call_args_list}


class TestOnUpdate(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.authorized = _account(TEST_USER, gmail_enabled=1, refresh_token="rt")
        cls.not_enabled = _account(TEST_USER_2, gmail_enabled=0, refresh_token="rt")
        cls.not_authorized = _account(TEST_USER_3, gmail_enabled=1, refresh_token="")
        cls.second = _account(SECOND_USER, gmail_enabled=1, refresh_token="rt")
        cls.disabled_user = _account(DISABLED_USER, gmail_enabled=1, refresh_token="rt")
        frappe.db.set_value("User", DISABLED_USER, "enabled", 0)

    def _run(self, realtime):
        with (
            change_settings("Google Settings", custom_gmail_sync_in_realtime=realtime),
            patch(f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.enable_pubsub") as mock_enable,
            patch(f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.disable_pubsub") as mock_disable,
        ):
            update_gmail_watches()
        return mock_enable, mock_disable

    def test_toggling_realtime_updates_watches_in_background(self):
        """Saving Google Settings with real-time sync toggled queues the watch update after the save commits."""
        with patch(
            f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.frappe.enqueue"
        ) as mock_enqueue:
            on_update(_settings(realtime=1))
        mock_enqueue.assert_called_once()
        self.assertEqual(
            mock_enqueue.call_args.args[0],
            "frappe_gmail_thread.doc_events.google_settings.update_gmail_watches",
        )
        self.assertTrue(mock_enqueue.call_args.kwargs["enqueue_after_commit"])

    def test_does_nothing_when_realtime_setting_unchanged(self):
        """on_update queues nothing when has_value_changed('custom_gmail_sync_in_realtime') returns False."""
        with patch(
            f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.frappe.enqueue"
        ) as mock_enqueue:
            on_update(_settings(realtime=1, changed=False))
        mock_enqueue.assert_not_called()

    def test_realtime_on_starts_watches_of_enabled_authorized_accounts(self):
        """With real-time sync on, a watch is started for each enabled, authorized account of an enabled user, and nothing else."""
        mock_enable, mock_disable = self._run(realtime=1)
        called = _called_for(mock_enable)
        self.assertIn(self.authorized, called)
        self.assertIn(self.second, called)
        self.assertNotIn(self.not_enabled, called)
        self.assertNotIn(self.not_authorized, called)
        self.assertNotIn(self.disabled_user, called)
        mock_disable.assert_not_called()

    def test_realtime_off_stops_watches_of_enabled_authorized_accounts(self):
        """With real-time sync off, the watch of each enabled, authorized account is stopped, a disabled user's included."""
        mock_enable, mock_disable = self._run(realtime=0)
        called = _called_for(mock_disable)
        self.assertIn(self.authorized, called)
        self.assertIn(self.disabled_user, called)
        self.assertNotIn(self.not_enabled, called)
        self.assertNotIn(self.not_authorized, called)
        mock_enable.assert_not_called()

    def test_failing_account_is_logged_and_others_still_processed(self):
        """A mailbox that can't be watched is logged without stopping the other accounts."""
        started = []

        def enable(account):
            if account.name == self.authorized:
                frappe.throw("Your Gmail authorization has expired.")
            started.append(account.name)

        with (
            change_settings("Google Settings", custom_gmail_sync_in_realtime=1),
            patch(
                f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.enable_pubsub", side_effect=enable
            ),
            patch(f"{GOOGLE_SETTINGS_DOC_EVENT_MODULE}.frappe.log_error") as mock_log,
        ):
            update_gmail_watches()
        self.assertIn(self.second, started)
        mock_log.assert_called()
