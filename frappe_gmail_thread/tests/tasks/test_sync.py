from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase

from frappe_gmail_thread.tasks.sync import sync_emails
from frappe_gmail_thread.tests import (
    TEST_USER,
    as_user,
    make_test_gmail_account,
    make_test_user,
    set_password_field,
)

SYNC_TASK_MODULE = "frappe_gmail_thread.tasks.sync"


class TestSyncEmails(IntegrationTestCase):
    def test_enqueues_sync_for_each_enabled_account_with_refresh_token(self):
        """sync_emails enqueues gmail_thread.sync once per enabled Gmail Account that has a refresh_token."""
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            account = make_test_gmail_account(linked_user=TEST_USER)
        frappe.db.set_value("Gmail Account", account.name, "gmail_enabled", 1)
        set_password_field("Gmail Account", account.name, "refresh_token", "rt")
        with (
            patch(f"{SYNC_TASK_MODULE}.is_job_enqueued", return_value=False),
            patch(f"{SYNC_TASK_MODULE}.frappe.enqueue") as mock_enqueue,
        ):
            sync_emails()
        mock_enqueue.assert_called_once()
        kwargs = mock_enqueue.call_args.kwargs
        self.assertEqual(kwargs["user"], TEST_USER)
        self.assertEqual(kwargs["job_name"], f"gmail_thread_sync_{TEST_USER}")

    def test_skips_account_without_refresh_token(self):
        """sync_emails skips accounts whose refresh_token is empty."""
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            account = make_test_gmail_account(linked_user=TEST_USER)
        frappe.db.set_value("Gmail Account", account.name, "gmail_enabled", 1)
        set_password_field("Gmail Account", account.name, "refresh_token", "")
        with (
            patch(f"{SYNC_TASK_MODULE}.is_job_enqueued", return_value=False),
            patch(f"{SYNC_TASK_MODULE}.frappe.enqueue") as mock_enqueue,
        ):
            sync_emails()
        mock_enqueue.assert_not_called()

    def test_does_not_reenqueue_running_job(self):
        """sync_emails does not enqueue when a sync job for the account's user is already running."""
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            account = make_test_gmail_account(linked_user=TEST_USER)
        frappe.db.set_value("Gmail Account", account.name, "gmail_enabled", 1)
        set_password_field("Gmail Account", account.name, "refresh_token", "rt")
        with (
            patch(f"{SYNC_TASK_MODULE}.is_job_enqueued", return_value=True),
            patch(f"{SYNC_TASK_MODULE}.frappe.enqueue") as mock_enqueue,
        ):
            sync_emails()
        mock_enqueue.assert_not_called()
