import json
from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase, change_settings

from frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_account.gmail_account import (
    sync_labels_api,
)
from frappe_gmail_thread.tests import (
    TEST_USER,
    as_user,
    make_test_gmail_account,
    make_test_user,
)

GMAIL_ACCOUNT_MODULE = (
    "frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_account.gmail_account"
)


class _GmailAccountTestCase(IntegrationTestCase):
    """Base class that skips the auto-loaded test_records to avoid the ERPNext Fiscal Year fixture conflict on dev DBs."""

    doctype = "Gmail Account"

    @classmethod
    def setUpClass(cls):
        # Pre-populate so IntegrationTestCase sees the doctype as already loaded and skips make_test_records.
        if not hasattr(frappe.local, "test_objects"):
            frappe.local.test_objects = {}
        frappe.local.test_objects.setdefault(cls.doctype, [])
        super().setUpClass()


def _reset_account(
    *, gmail_enabled=0, refresh_token=None, last_historyid=0, labels=None
):
    """Get or create the test Gmail Account and force the supplied field values onto it (bypassing make_test_gmail_account's idempotent-fetch cache pollution)."""
    with as_user(TEST_USER):
        account = make_test_gmail_account(linked_user=TEST_USER)
    updates = {
        "gmail_enabled": gmail_enabled,
        "refresh_token": refresh_token or "",
        "last_historyid": last_historyid,
    }
    frappe.db.set_value("Gmail Account", account.name, updates)
    # Reset the labels child table via direct delete + reinsert (we don't go through save() to avoid hooks).
    frappe.db.delete("Gmail Label", {"parent": account.name})
    if labels:
        for idx, label in enumerate(labels, start=1):
            row = frappe.get_doc(
                {
                    "doctype": "Gmail Label",
                    "parent": account.name,
                    "parenttype": "Gmail Account",
                    "parentfield": "labels",
                    "idx": idx,
                    **label,
                }
            )
            row.flags.ignore_validate = True
            row.db_insert()
    return frappe.get_doc("Gmail Account", account.name)


class TestGmailAccountBeforeInsert(_GmailAccountTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)

    def test_before_insert_sets_linked_user_and_email_id_from_session_user(self):
        """before_insert auto-fills linked_user from frappe.session.user and email_id from that user's email."""
        unique_user = "test_fgt_before_insert@example.com"
        make_test_user(unique_user)
        with as_user(unique_user):
            account = frappe.get_doc({"doctype": "Gmail Account"})
            account.flags.ignore_validate = True
            account.insert(ignore_permissions=True)
        self.assertEqual(account.linked_user, unique_user)
        self.assertEqual(account.email_id, unique_user)


class TestGmailAccountValidate(_GmailAccountTestCase):
    def _new_account(self, **fields):
        account = frappe.new_doc("Gmail Account")
        for k, v in fields.items():
            setattr(account, k, v)
        return account

    def test_throws_when_gmail_enabled_without_google_settings_enabled(self):
        """validate raises when gmail_enabled=1 but Google Settings.enable is off."""
        account = self._new_account(gmail_enabled=1)
        with change_settings(
            "Google Settings", enable=0, client_id="x", client_secret="y"
        ):
            with self.assertRaises(frappe.ValidationError):
                account.validate()

    def test_throws_when_gmail_enabled_without_client_id(self):
        """validate raises when client_id is empty."""
        account = self._new_account(gmail_enabled=1)
        with change_settings(
            "Google Settings", enable=1, client_id="", client_secret="y"
        ):
            with self.assertRaises(frappe.ValidationError):
                account.validate()

    def test_throws_when_gmail_enabled_without_client_secret(self):
        """validate raises when client_secret is empty."""
        account = self._new_account(gmail_enabled=1)
        with change_settings(
            "Google Settings", enable=1, client_id="x", client_secret=""
        ):
            with self.assertRaises(frappe.ValidationError):
                account.validate()

    def test_does_not_throw_when_gmail_disabled(self):
        """validate is silent when gmail_enabled=0 regardless of Google Settings state."""
        account = self._new_account(gmail_enabled=0)
        with change_settings(
            "Google Settings", enable=0, client_id="", client_secret=""
        ):
            account.validate()  # no exception expected


class TestGmailAccountOnTrash(_GmailAccountTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)

    def test_calls_disable_pubsub_when_enabled_with_token_and_topic(self):
        """on_trash invokes disable_pubsub when all three preconditions are met."""
        account = _reset_account(gmail_enabled=1, refresh_token="rt")
        with change_settings("Google Settings", custom_gmail_pubsub_topic="topic"):
            with patch(f"{GMAIL_ACCOUNT_MODULE}.disable_pubsub") as mock_disable:
                account.on_trash()
        mock_disable.assert_called_once()

    def test_returns_silently_when_not_enabled(self):
        """on_trash does nothing when gmail_enabled=0."""
        account = _reset_account(gmail_enabled=0, refresh_token="rt")
        with change_settings("Google Settings", custom_gmail_pubsub_topic="topic"):
            with patch(f"{GMAIL_ACCOUNT_MODULE}.disable_pubsub") as mock_disable:
                account.on_trash()
        mock_disable.assert_not_called()

    def test_returns_silently_when_no_refresh_token(self):
        """on_trash does nothing when refresh_token is empty."""
        account = _reset_account(gmail_enabled=1, refresh_token=None)
        with change_settings("Google Settings", custom_gmail_pubsub_topic="topic"):
            with patch(f"{GMAIL_ACCOUNT_MODULE}.disable_pubsub") as mock_disable:
                account.on_trash()
        mock_disable.assert_not_called()

    def test_returns_silently_when_no_pubsub_topic(self):
        """on_trash does nothing when Google Settings has no pubsub topic configured."""
        account = _reset_account(gmail_enabled=1, refresh_token="rt")
        with change_settings("Google Settings", custom_gmail_pubsub_topic=""):
            with patch(f"{GMAIL_ACCOUNT_MODULE}.disable_pubsub") as mock_disable:
                account.on_trash()
        mock_disable.assert_not_called()


class TestGmailAccountBeforeSave(_GmailAccountTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)

    def test_revalidates_google_settings_when_gmail_enabled_toggled_on(self):
        """When gmail_enabled flips from 0 to 1, before_save re-checks Google Settings and raises if not configured."""
        account = _reset_account(gmail_enabled=0)
        account.gmail_enabled = 1
        with change_settings(
            "Google Settings", enable=0, client_id="", client_secret=""
        ):
            with self.assertRaises(frappe.ValidationError):
                account.save(ignore_permissions=True)

    def test_refresh_token_change_triggers_sync_labels_and_enable_pubsub(self):
        """Setting refresh_token on an enabled account triggers sync_labels + enable_pubsub when realtime + topic are configured."""
        account = _reset_account(gmail_enabled=1)
        account.refresh_token = "new-token"
        with change_settings(
            "Google Settings",
            enable=1,
            client_id="cid",
            client_secret="csec",
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with (
                patch(f"{GMAIL_ACCOUNT_MODULE}.sync_labels") as mock_sync_labels,
                patch(f"{GMAIL_ACCOUNT_MODULE}.enable_pubsub") as mock_enable,
                patch(f"{GMAIL_ACCOUNT_MODULE}.disable_pubsub") as mock_disable,
            ):
                account.save(ignore_permissions=True)
        mock_sync_labels.assert_called_once()
        mock_enable.assert_called()
        mock_disable.assert_not_called()

    def test_refresh_token_change_triggers_disable_pubsub_when_not_enabled(self):
        """Setting refresh_token on a disabled account triggers disable_pubsub instead of enable_pubsub."""
        account = _reset_account(gmail_enabled=0)
        account.refresh_token = "new-token"
        with change_settings(
            "Google Settings",
            enable=1,
            client_id="cid",
            client_secret="csec",
            custom_gmail_sync_in_realtime=1,
            custom_gmail_pubsub_topic="topic",
        ):
            with (
                patch(f"{GMAIL_ACCOUNT_MODULE}.sync_labels"),
                patch(f"{GMAIL_ACCOUNT_MODULE}.enable_pubsub") as mock_enable,
                patch(f"{GMAIL_ACCOUNT_MODULE}.disable_pubsub") as mock_disable,
            ):
                account.save(ignore_permissions=True)
        mock_disable.assert_called()
        mock_enable.assert_not_called()

    def test_labels_change_resets_last_historyid(self):
        """When labels child table changes, last_historyid is reset to 0."""
        account = _reset_account(gmail_enabled=0, last_historyid=12345)
        account.append(
            "labels", {"label_id": "INBOX", "label_name": "INBOX", "enabled": 0}
        )
        account.save(ignore_permissions=True)
        self.assertEqual(account.last_historyid, 0)


class TestSyncLabelsApi(_GmailAccountTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)

    def test_enqueues_sync_job(self):
        """sync_labels_api enqueues the gmail_thread.sync background job for the account's linked_user."""
        account = _reset_account()
        args = json.dumps({"doc_name": account.name})

        with (
            patch(f"{GMAIL_ACCOUNT_MODULE}.is_job_enqueued", return_value=False),
            patch("frappe.enqueue") as mock_enqueue,
        ):
            sync_labels_api(args)

        mock_enqueue.assert_called_once()
        kwargs = mock_enqueue.call_args.kwargs
        self.assertEqual(kwargs["user"], TEST_USER)
        self.assertEqual(kwargs["job_name"], f"gmail_thread_sync_{TEST_USER}")

    def test_does_not_reenqueue_if_already_enqueued(self):
        """sync_labels_api is idempotent: if a job for this user is already enqueued, no new enqueue happens."""
        account = _reset_account()
        args = json.dumps({"doc_name": account.name})

        with (
            patch(f"{GMAIL_ACCOUNT_MODULE}.is_job_enqueued", return_value=True),
            patch("frappe.enqueue") as mock_enqueue,
        ):
            sync_labels_api(args)

        mock_enqueue.assert_not_called()

    def test_resets_last_historyid_when_flag_set(self):
        """sync_labels_api with reset_historyid=True sets last_historyid to 0 before enqueuing."""
        account = _reset_account(last_historyid=42)
        args = json.dumps({"doc_name": account.name, "reset_historyid": True})

        with (
            patch(f"{GMAIL_ACCOUNT_MODULE}.is_job_enqueued", return_value=False),
            patch("frappe.enqueue"),
        ):
            sync_labels_api(args)

        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 0
        )
