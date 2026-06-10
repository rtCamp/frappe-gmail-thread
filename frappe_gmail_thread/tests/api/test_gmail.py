from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase

from frappe_gmail_thread.api.gmail import is_gmail_configured
from frappe_gmail_thread.tests import (
    TEST_USER,
    as_user,
    make_test_gmail_account,
    make_test_user,
)

GMAIL_MODULE = "frappe_gmail_thread.api.gmail"


def _ensure_account():
    make_test_user(TEST_USER)
    with as_user(TEST_USER):
        return make_test_gmail_account(linked_user=TEST_USER)


class TestIsGmailConfigured(IntegrationTestCase):
    def test_returns_false_with_not_found_message_when_no_gmail_account(self):
        """is_gmail_configured returns {configured: False} with a 'Gmail Account not found' message when the session user has no account."""
        no_acct_user = "test_fgt_no_gmail_account@example.com"
        make_test_user(no_acct_user)
        with as_user(no_acct_user):
            result = is_gmail_configured()
        self.assertEqual(result["configured"], False)
        self.assertIn("Gmail Account not found", result["message"])

    def test_throws_permission_error_when_no_read_permission(self):
        """is_gmail_configured throws PermissionError when the session user lacks read permission on the account."""
        _ensure_account()
        with as_user(TEST_USER):
            with patch(f"{GMAIL_MODULE}.frappe.has_permission", return_value=False):
                with self.assertRaises(frappe.PermissionError):
                    is_gmail_configured()

    def test_returns_false_when_gmail_not_enabled(self):
        """is_gmail_configured returns {configured: False, 'Please configure Gmail'} when the account exists but gmail_enabled=0."""
        account = _ensure_account()
        frappe.db.set_value(
            "Gmail Account",
            account.name,
            {"gmail_enabled": 0, "refresh_token": "rt"},
        )
        with as_user(TEST_USER):
            with patch(f"{GMAIL_MODULE}.frappe.has_permission", return_value=True):
                result = is_gmail_configured()
        self.assertEqual(result["configured"], False)
        self.assertIn("Please configure Gmail", result["message"])

    def test_returns_true_when_enabled_with_refresh_token_and_matching_user(self):
        """is_gmail_configured returns {configured: True} only when gmail_enabled=1, refresh_token is set, and linked_user matches the session user."""
        account = _ensure_account()
        frappe.db.set_value(
            "Gmail Account",
            account.name,
            {"gmail_enabled": 1, "refresh_token": "rt"},
        )
        with as_user(TEST_USER):
            with patch(f"{GMAIL_MODULE}.frappe.has_permission", return_value=True):
                result = is_gmail_configured()
        self.assertEqual(result["configured"], True)
