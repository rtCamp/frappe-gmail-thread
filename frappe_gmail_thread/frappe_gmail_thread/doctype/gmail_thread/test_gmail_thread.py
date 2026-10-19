import datetime
import json
from contextlib import contextmanager
from email.utils import format_datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import frappe
import googleapiclient.errors
import httplib2
from frappe.tests import IntegrationTestCase, change_settings
from frappe.utils import get_datetime
from frappe.utils.password import get_decrypted_password
from google.auth.exceptions import RefreshError

from frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread import (
    GMAIL_API_RETRIES,
    MAX_FETCH_ATTEMPTS,
    get_max_threads_per_label,
    get_permission_query_conditions,
    get_retry_after,
    has_permission,
    is_not_found_error,
    is_rate_limit_error,
    list_label_threads,
    sync,
    sync_labels,
    update_involved_users,
)
from frappe_gmail_thread.tasks.sync import sync_emails
from frappe_gmail_thread.tests import (
    TEST_USER,
    TEST_USER_2,
    TEST_USER_3,
    as_user,
    make_test_gmail_account,
    make_test_gmail_thread,
    make_test_user,
    set_password_field,
)
from frappe_gmail_thread.utils.helpers import AlreadyExistsError
from frappe_gmail_thread.utils.helpers import (
    find_gmail_thread as real_find_gmail_thread,
)
from frappe_gmail_thread.utils.sync_pause import (
    count_fetch_failure,
    is_sync_paused,
    pause_sync,
)

GMAIL_THREAD_MODULE = (
    "frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread"
)
OAUTH_MODULE = "frappe_gmail_thread.api.oauth"
SYNC_TASK_MODULE = "frappe_gmail_thread.tasks.sync"


class _GmailThreadTestCase(IntegrationTestCase):
    """Base class that skips auto-loaded test_records to avoid the ERPNext Fiscal Year fixture conflict on dev DBs."""

    @classmethod
    def setUpClass(cls):
        if not hasattr(frappe.local, "test_objects"):
            frappe.local.test_objects = {}
        for doctype in ("Gmail Thread", "Gmail Account"):
            frappe.local.test_objects.setdefault(doctype, [])
        super().setUpClass()


def _ensure_account():
    """Return the cached Gmail Account for TEST_USER, creating user + account if missing."""
    make_test_user(TEST_USER)
    with as_user(TEST_USER):
        return make_test_gmail_account(linked_user=TEST_USER)


def _reset_account_for_sync(*, last_historyid=0, labels=(("INBOX", True),)):
    """Get the cached Gmail Account and force it into a state ready for sync tests."""
    account = _ensure_account()
    frappe.db.set_value(
        "Gmail Account",
        account.name,
        {"gmail_enabled": 1, "last_historyid": last_historyid},
    )
    set_password_field("Gmail Account", account.name, "refresh_token", "rt")
    frappe.db.delete("Gmail Label", {"parent": account.name})
    for idx, (label_id, enabled) in enumerate(labels, start=1):
        row = frappe.get_doc(
            {
                "doctype": "Gmail Label",
                "parent": account.name,
                "parenttype": "Gmail Account",
                "parentfield": "labels",
                "idx": idx,
                "label_id": label_id,
                "label_name": label_id,
                "enabled": 1 if enabled else 0,
            }
        )
        row.flags.ignore_validate = True
        row.db_insert()
    return frappe.get_doc("Gmail Account", account.name)


def _make_http_error(reason):
    """Build a googleapiclient HttpError with the given reason in error_details."""
    err = googleapiclient.errors.HttpError(
        resp=MagicMock(status=404, reason=reason),
        content=b'{"error": {"code": 404}}',
    )
    err.error_details = [{"reason": reason}]
    return err


def _configure_mock_child_tables(mock_doc, *fieldnames):
    """Make selected child tables on a document mock support ``append`` calls."""
    for fieldname in fieldnames:
        setattr(mock_doc, fieldname, [])

    def append(fieldname, value, **_kwargs):
        getattr(mock_doc, fieldname).append(value)

    mock_doc.append.side_effect = append


class TestGmailThreadStatus(_GmailThreadTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.account = _ensure_account()

    def test_status_flips_open_to_linked_when_reference_set(self):
        """before_save flips status from Open to Linked when reference_doctype + reference_name both newly become non-empty."""
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="status-open-to-linked",
            status="Open",
        )
        thread = frappe.get_doc("Gmail Thread", "status-open-to-linked")
        thread.reference_doctype = "User"
        thread.reference_name = TEST_USER
        thread.save(ignore_permissions=True)
        self.assertEqual(thread.status, "Linked")

    def test_status_flips_linked_to_open_when_reference_cleared(self):
        """before_save flips status back to Open when reference is cleared and current status is Linked."""
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="status-linked-to-open",
            reference_doctype="User",
            reference_name=TEST_USER,
            status="Linked",
        )
        thread = frappe.get_doc("Gmail Thread", "status-linked-to-open")
        thread.reference_doctype = None
        thread.reference_name = None
        thread.save(ignore_permissions=True)
        self.assertEqual(thread.status, "Open")

    def test_warns_via_msgprint_when_reference_already_linked_elsewhere(self):
        """When a second thread is linked to the same reference doc, before_save emits a msgprint warning."""
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="dup-ref-a",
            reference_doctype="User",
            reference_name=TEST_USER,
            status="Linked",
        )
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="dup-ref-b",
            status="Open",
        )
        thread_b = frappe.get_doc("Gmail Thread", "dup-ref-b")
        thread_b.reference_doctype = "User"
        thread_b.reference_name = TEST_USER
        with patch(f"{GMAIL_THREAD_MODULE}.frappe.msgprint") as mock_msgprint:
            thread_b.save(ignore_permissions=True)
        mock_msgprint.assert_called()


class TestGmailThreadAllSubjects(_GmailThreadTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.account = _ensure_account()

    def test_all_subjects_is_sorted_deduped_join(self):
        """all_subjects is the newline-joined, sorted, deduplicated list of subject_of_first_mail and every email's subject."""
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="subjects-basic",
            subject_of_first_mail="Foo",
            emails=[
                {"gmail_message_id": "mid-bar", "subject": "Bar"},
                {"gmail_message_id": "mid-foo", "subject": "Foo"},
                {"gmail_message_id": "mid-baz", "subject": "Baz"},
            ],
        )
        thread = frappe.get_doc("Gmail Thread", "subjects-basic")
        thread.save(ignore_permissions=True)
        self.assertEqual(thread.all_subjects, "Bar\nBaz\nFoo")

    def test_all_subjects_strips_whitespace_before_dedup(self):
        """Whitespace around subjects is stripped before deduplication."""
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="subjects-whitespace",
            subject_of_first_mail="  Foo  ",
            emails=[
                {"gmail_message_id": "mid-ws-1", "subject": "Foo"},
                {"gmail_message_id": "mid-ws-2", "subject": " Foo"},
            ],
        )
        thread = frappe.get_doc("Gmail Thread", "subjects-whitespace")
        thread.save(ignore_permissions=True)
        self.assertEqual(thread.all_subjects, "Foo")


class TestGmailThreadFileSharing(_GmailThreadTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.account = _ensure_account()
        make_test_user(TEST_USER_2)
        make_test_user(TEST_USER_3)

    def test_shares_attachments_with_involved_users_except_owner(self):
        """When involved_users changes, every File attached to the thread is shared via add_docshare with every involved user except the owner."""
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="share-attachments",
            owner=TEST_USER,
            involved_users=[TEST_USER],
        )
        frappe.get_doc(
            {
                "doctype": "File",
                "file_name": "test.txt",
                "content": "hello",
                "attached_to_doctype": "Gmail Thread",
                "attached_to_name": "share-attachments",
            }
        ).insert(ignore_permissions=True)
        thread = frappe.get_doc("Gmail Thread", "share-attachments")
        thread.append("involved_users", {"account": TEST_USER_2})
        thread.append("involved_users", {"account": TEST_USER_3})
        with patch("frappe.share.add_docshare") as mock_share:
            thread.save(ignore_permissions=True)
        shared_users = {call.args[2] for call in mock_share.call_args_list}
        self.assertEqual(shared_users, {TEST_USER_2, TEST_USER_3})


class TestSyncLabels(_GmailThreadTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.account = _ensure_account()

    def _gmail_with_labels(self, labels):
        gmail = MagicMock()
        gmail.users().labels().list.return_value.execute.return_value = {
            "labels": labels
        }
        return gmail

    def test_appends_new_labels_not_already_on_account(self):
        """sync_labels appends labels whose label_id isn't already on the account."""
        account = frappe.get_doc("Gmail Account", self.account.name)
        account.set("labels", [])
        account.append("labels", {"label_id": "INBOX", "label_name": "INBOX"})
        gmail = self._gmail_with_labels(
            [
                {"id": "INBOX", "name": "INBOX"},
                {"id": "Label_1", "name": "Custom"},
            ]
        )
        with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail):
            sync_labels(account, should_save=False)
        self.assertEqual([x.label_id for x in account.labels], ["INBOX", "Label_1"])

    def test_skips_draft_and_chat_labels(self):
        """sync_labels never adds Gmail's DRAFT or CHAT system labels."""
        account = frappe.get_doc("Gmail Account", self.account.name)
        account.set("labels", [])
        gmail = self._gmail_with_labels(
            [
                {"id": "DRAFT", "name": "DRAFT"},
                {"id": "CHAT", "name": "CHAT"},
                {"id": "INBOX", "name": "INBOX"},
            ]
        )
        with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail):
            sync_labels(account, should_save=False)
        self.assertEqual([x.label_id for x in account.labels], ["INBOX"])

    def test_drops_labels_deleted_in_gmail_without_resetting_sync(self):
        """Fetching labels removes the ones deleted in Gmail and keeps the sync position."""
        account = _reset_account_for_sync(
            last_historyid=100,
            labels=(("INBOX", True), ("Label_gone", False)),
        )
        gmail = self._gmail_with_labels([{"id": "INBOX", "name": "INBOX"}])
        with change_settings(
            "Google Settings", enable=1, client_id="cid", client_secret="csec"
        ):
            with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail):
                sync_labels(account, should_save=True)
        self.assertEqual(
            frappe.get_all(
                "Gmail Label", filters={"parent": account.name}, pluck="label_id"
            ),
            ["INBOX"],
        )
        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 100
        )

    def test_reauthorizing_keeps_sync_position_when_a_label_was_deleted(self):
        """A new authorization doesn't reset the sync because a label was deleted in Gmail."""
        account = _reset_account_for_sync(
            last_historyid=100,
            labels=(("INBOX", True), ("Label_gone", False)),
        )
        account.refresh_token = "new-token"
        gmail = self._gmail_with_labels([{"id": "INBOX", "name": "INBOX"}])
        with change_settings(
            "Google Settings",
            enable=1,
            client_id="cid",
            client_secret="csec",
            custom_gmail_sync_in_realtime=0,
        ):
            with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail):
                account.save(ignore_permissions=True)
        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 100
        )

    def test_does_not_save_when_should_save_false(self):
        """When should_save=False the account's save() is not invoked."""
        account = frappe.get_doc("Gmail Account", self.account.name)
        gmail = self._gmail_with_labels([])
        with (
            patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
            patch.object(account, "save") as mock_save,
        ):
            sync_labels(account, should_save=False)
        mock_save.assert_not_called()

    def test_saves_account_when_should_save_true(self):
        """When should_save=True the account's save() is invoked."""
        account = frappe.get_doc("Gmail Account", self.account.name)
        gmail = self._gmail_with_labels([])
        with (
            patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
            patch.object(account, "save") as mock_save,
        ):
            sync_labels(account, should_save=True)
        mock_save.assert_called_once_with()


class TestUpdateInvolvedUsers(_GmailThreadTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.account = _ensure_account()

    def test_appends_only_system_users_with_matching_email(self):
        """Emails matching System Users are appended; Website Users and unknown emails are skipped."""
        sys_user = "test_fgt_iu_sys@example.com"
        web_user = "test_fgt_iu_web@example.com"
        make_test_user(sys_user)
        make_test_user(web_user, user_type="Website User")
        # Force user_type post-insert to bypass Frappe's User.validate auto-downgrade-to-Website-User.
        frappe.db.set_value("User", sys_user, "user_type", "System User")
        frappe.db.set_value("User", web_user, "user_type", "Website User")
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="iu-system-only",
            involved_users=[TEST_USER],
        )
        update_involved_users(
            thread, {TEST_USER, sys_user, web_user, "nobody@example.com"}
        )
        users = [x.account for x in thread.involved_users]
        self.assertIn(sys_user, users)
        self.assertNotIn(web_user, users)
        self.assertNotIn("nobody@example.com", users)

    def test_does_not_duplicate_existing_involved_users(self):
        """Users already linked are not appended a second time."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="iu-no-dup",
            involved_users=[TEST_USER],
        )
        update_involved_users(thread, {TEST_USER})
        users = [x.account for x in thread.involved_users]
        self.assertEqual(users.count(TEST_USER), 1)


class TestGetPermissionQueryConditions(_GmailThreadTestCase):
    def test_returns_empty_string_for_administrator(self):
        """No SQL filter is applied for Administrator."""
        self.assertEqual(get_permission_query_conditions("Administrator"), "")

    def test_returns_sql_filter_for_non_admin(self):
        """SQL restricts results to threads where user is in Involved User or is owner."""
        sql = get_permission_query_conditions("user@example.com")
        self.assertIn("`tabGmail Thread`.name in (", sql)
        self.assertIn("tabInvolved User", sql)
        self.assertIn("`tabGmail Thread`.owner =", sql)
        self.assertIn("user@example.com", sql)

    def test_falls_back_to_session_user_when_user_is_none(self):
        """user=None falls back to frappe.session.user."""
        with as_user("Administrator"):
            self.assertEqual(get_permission_query_conditions(None), "")


class TestHasPermission(_GmailThreadTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.account = _ensure_account()
        make_test_user(TEST_USER_2)

    def test_returns_true_for_administrator_on_any_ptype(self):
        """Administrator has permission unconditionally regardless of ptype."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="perm-admin",
            involved_users=[],
        )
        self.assertTrue(has_permission(thread, "read", "Administrator"))
        self.assertTrue(has_permission(thread, "export", "Administrator"))

    def test_returns_true_for_involved_user_on_standard_ptypes(self):
        """Non-Administrator with an Involved User row gets True for read/write/delete/create."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="perm-involved",
            involved_users=[TEST_USER_2],
        )
        for ptype in ("read", "write", "delete", "create"):
            self.assertTrue(has_permission(thread, ptype, TEST_USER_2))

    def test_returns_false_for_non_involved_user(self):
        """Non-Administrator without an Involved User row gets False."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="perm-not-involved",
            involved_users=[],
        )
        self.assertFalse(has_permission(thread, "read", TEST_USER_2))

    def test_returns_false_for_unknown_ptype_even_when_involved(self):
        """Non-Administrator gets False for any ptype outside read/write/delete/create."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="perm-other-ptype",
            involved_users=[TEST_USER_2],
        )
        self.assertFalse(has_permission(thread, "export", TEST_USER_2))


class TestSyncDispatch(_GmailThreadTestCase):
    def test_uses_threads_list_when_last_historyid_zero(self):
        """The initial-sync path (threads().list) runs when last_historyid is 0."""
        _reset_account_for_sync(last_historyid=0)
        gmail = MagicMock()
        gmail.users().threads().list.return_value.execute.return_value = {"threads": []}
        with as_user("Administrator"):
            with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail):
                sync(user=TEST_USER)
        gmail.users().threads().list.assert_called()
        gmail.users().history().list.assert_not_called()

    def test_uses_history_list_when_last_historyid_nonzero(self):
        """The incremental-sync path (history().list) runs when last_historyid is non-zero."""
        _reset_account_for_sync(last_historyid=100)
        gmail = MagicMock()
        gmail.users().history().list.return_value.execute.return_value = {
            "history": [],
            "historyId": "200",
        }
        with as_user("Administrator"):
            with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail):
                sync(user=TEST_USER)
        gmail.users().history().list.assert_called()
        gmail.users().threads().list.assert_not_called()


class TestSyncPreconditions(_GmailThreadTestCase):
    def test_skips_account_disabled_after_job_was_queued(self):
        """sync returns without calling Gmail when the account has been disabled since the job was queued."""
        account = _reset_account_for_sync()
        frappe.db.set_value("Gmail Account", account.name, "gmail_enabled", 0)
        with as_user("Administrator"):
            with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object") as mock_get:
                sync(user=TEST_USER)
        mock_get.assert_not_called()

    def test_skips_account_without_authorization(self):
        """sync returns without calling Gmail when the account has no refresh token (never authorized, or expired)."""
        account = _reset_account_for_sync()
        set_password_field("Gmail Account", account.name, "refresh_token", "")
        with as_user("Administrator"):
            with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object") as mock_get:
                sync(user=TEST_USER)
        mock_get.assert_not_called()

    def test_skips_user_whose_account_was_deleted(self):
        """sync returns quietly for a user with no Gmail Account (deleted after the job was queued)."""
        no_account_user = "test_fgt_no_gmail_account@example.com"
        make_test_user(no_account_user)
        with as_user("Administrator"):
            with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object") as mock_get:
                sync(user=no_account_user)
        mock_get.assert_not_called()

    def test_returns_silently_when_no_enabled_labels(self):
        """sync returns silently when no labels are enabled (no API calls)."""
        _reset_account_for_sync(labels=(("INBOX", False),))
        gmail = MagicMock()
        with as_user("Administrator"):
            with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail):
                sync(user=TEST_USER)
        gmail.users().threads().list.assert_not_called()
        gmail.users().history().list.assert_not_called()


def _revoked_gmail():
    """A Gmail client whose token refresh fails the way Google reports a revoked or expired grant."""
    gmail = MagicMock()
    gmail.users().getProfile().execute.side_effect = RefreshError(
        "invalid_grant: Token has been expired or revoked.",
        {
            "error": "invalid_grant",
            "error_description": "Token has been expired or revoked.",
        },
    )
    return gmail


class TestSyncExpiredAuthorization(_GmailThreadTestCase):
    def _sync_with_revoked_grant(self):
        with (
            patch(f"{OAUTH_MODULE}.get_access_token", return_value=None),
            patch(f"{OAUTH_MODULE}.build", return_value=_revoked_gmail()),
            patch("frappe.log_error") as mock_log,
        ):
            with as_user("Administrator"):
                sync(user=TEST_USER)
        return mock_log

    def _alerts(self, account_name):
        return frappe.get_all(
            "Notification Log",
            filters={
                "for_user": TEST_USER,
                "type": "Alert",
                "document_type": "Gmail Account",
                "document_name": account_name,
            },
            pluck="name",
        )

    def test_revoked_grant_clears_token_and_alerts_owner_without_error_log(self):
        """When Google rejects the refresh token, sync forgets it, alerts the account owner and writes no Error Log."""
        account = _reset_account_for_sync(last_historyid=100)
        alerts_before = len(self._alerts(account.name))

        mock_log = self._sync_with_revoked_grant()

        mock_log.assert_not_called()
        self.assertFalse(
            frappe.db.get_value("Gmail Account", account.name, "refresh_token")
        )
        self.assertIsNone(
            get_decrypted_password(
                "Gmail Account", account.name, "refresh_token", raise_exception=False
            )
        )
        self.assertEqual(len(self._alerts(account.name)), alerts_before + 1)
        # the sync position is kept so a re-authorization resumes where it stopped
        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 100
        )

    def test_misconfigured_oauth_client_keeps_tokens(self):
        """A site-wide OAuth problem (invalid_client) isn't a per-user expiry: the token is kept and the error surfaces."""
        account = _reset_account_for_sync()
        gmail = MagicMock()
        gmail.users().getProfile().execute.side_effect = RefreshError(
            "invalid_client: The OAuth client was not found.",
            {"error": "invalid_client"},
        )
        with (
            patch(f"{OAUTH_MODULE}.get_access_token", return_value=None),
            patch(f"{OAUTH_MODULE}.build", return_value=gmail),
        ):
            with as_user("Administrator"):
                with self.assertRaises(RefreshError):
                    sync(user=TEST_USER)
        self.assertTrue(
            frappe.db.get_value("Gmail Account", account.name, "refresh_token")
        )

    def test_scheduler_stops_syncing_the_account_after_expiry(self):
        """Once handled, the expired account is left out of the 30-minute sync until it is re-authorized."""
        _reset_account_for_sync()
        self._sync_with_revoked_grant()
        with (
            patch(f"{SYNC_TASK_MODULE}.is_job_enqueued", return_value=False),
            patch(f"{SYNC_TASK_MODULE}.frappe.enqueue") as mock_enqueue,
        ):
            sync_emails()
        enqueued_for = {c.kwargs["user"] for c in mock_enqueue.call_args_list}
        self.assertNotIn(TEST_USER, enqueued_for)


class TestIsNotFoundError(_GmailThreadTestCase):
    def _error(self, status, content):
        return googleapiclient.errors.HttpError(
            resp=MagicMock(status=status, reason=""), content=content
        )

    def test_recognises_gmail_not_found_payload(self):
        """The 404 body Gmail returns for a missing label / expired history id is recognised."""
        content = (
            b'{"error": {"code": 404, "message": "Requested entity was not found.",'
            b' "errors": [{"message": "Requested entity was not found.",'
            b' "domain": "global", "reason": "notFound"}], "status": "NOT_FOUND"}}'
        )
        self.assertTrue(is_not_found_error(self._error(404, content)))

    def test_other_errors_are_not_not_found(self):
        """A rate-limit error, or a body that isn't JSON, isn't treated as not-found."""
        rate_limited = (
            b'{"error": {"code": 429, "message": "Too many requests",'
            b' "errors": [{"reason": "rateLimitExceeded"}]}}'
        )
        self.assertFalse(is_not_found_error(self._error(429, rate_limited)))
        self.assertFalse(is_not_found_error(self._error(502, b"Bad Gateway")))


class TestSyncConcurrentMailboxes(_GmailThreadTestCase):
    """Syncs of different mailboxes update the same thread at the same time."""

    def setUp(self):
        self.account = _reset_account_for_sync(last_historyid=100)
        frappe.db.set_value("User", TEST_USER, "user_type", "System User")
        self.thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id=f"race-{frappe.generate_hash(length=10)}",
            emails=[_email_row("first", "2026-10-12 09:00:00")],
        )
        # the other mailbox's connection must see the thread
        frappe.db.commit()  # nosemgrep

    def _gmail(self, *messages):
        gmail = MagicMock()
        gmail.users().history().list.return_value.execute.return_value = {
            "history": [{"messages": [{"id": m, "threadId": t} for m, t in messages]}],
            "historyId": "200",
        }
        gmail.users().messages().get.return_value.execute.return_value = {
            "labelIds": ["INBOX"]
        }
        return gmail

    @contextmanager
    def _other_mailbox(self):
        """Run the block as a parallel sync would: on a separate DB connection."""
        user = frappe.session.user
        # primary_connection() restores the sync's connection afterwards, which
        # secondary_connection() alone doesn't on its first use
        with self.primary_connection(), self.secondary_connection():
            yield
        frappe.set_user(user)

    def _other_mailbox_appends(self, row):
        """Append `row` to the thread from another connection and commit."""
        with self._other_mailbox():
            other = frappe.get_doc("Gmail Thread", self.thread.name)
            other.append("emails", row)
            other.save(ignore_permissions=True)
            frappe.db.commit()  # nosemgrep

    def _thread_is_locked(self, name):
        with self._other_mailbox():
            try:
                frappe.db.sql(
                    "select name from `tabGmail Thread` where name = %s for update nowait",
                    name,
                )
                return False
            except frappe.QueryTimeoutError:
                return True
            finally:
                frappe.db.rollback()

    def _sync(self, gmail, incoming, find_gmail_thread=None, process_attachments=None):
        with (
            change_settings(
                "Google Settings", enable=1, client_id="cid", client_secret="csec"
            ),
            patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
            patch(f"{GMAIL_THREAD_MODULE}.create_new_email", side_effect=incoming),
            patch(
                f"{GMAIL_THREAD_MODULE}.find_gmail_thread",
                side_effect=find_gmail_thread or real_find_gmail_thread,
            ),
            patch(
                f"{GMAIL_THREAD_MODULE}.process_attachments",
                side_effect=process_attachments,
            ),
            patch(f"{GMAIL_THREAD_MODULE}.replace_inline_images"),
            patch(f"{GMAIL_THREAD_MODULE}.frappe.log_error") as mock_log,
        ):
            with as_user("Administrator"):
                sync(user=TEST_USER)
        return mock_log

    def _rfc_ids(self):
        return frappe.get_all(
            "Single Email CT",
            filters={"parent": self.thread.name},
            pluck="email_message_id",
        )

    def test_email_committed_meanwhile_by_another_mailbox_is_kept(self):
        """A sync that read the thread before another mailbox's sync appended an email still saves, keeping both emails."""

        def find_then_other_mailbox_appends(thread_id, message_ids=None):
            found = real_find_gmail_thread(thread_id, message_ids)
            self._other_mailbox_appends(_email_row("theirs", "2026-10-12 10:43:15"))
            return found

        mock_log = self._sync(
            self._gmail(("m-ours", self.thread.name)),
            [_incoming("ours", "2026-10-12 10:43:16")],
            find_gmail_thread=find_then_other_mailbox_appends,
        )
        mock_log.assert_not_called()
        self.assertCountEqual(
            self._rfc_ids(), ["first@x.com", "theirs@x.com", "ours@x.com"]
        )

    def test_same_email_committed_meanwhile_is_not_duplicated(self):
        """When another mailbox's sync imports the same email first, this sync adds its user instead of a second copy."""

        def find_then_other_mailbox_imports_it(thread_id, message_ids=None):
            found = real_find_gmail_thread(thread_id, message_ids)
            self._other_mailbox_appends(_email_row("shared", "2026-10-12 10:43:15"))
            return found

        mock_log = self._sync(
            self._gmail(("m-shared", self.thread.name)),
            [_incoming("shared", "2026-10-12 10:43:15")],
            find_gmail_thread=find_then_other_mailbox_imports_it,
        )
        mock_log.assert_not_called()
        self.assertEqual(self._rfc_ids().count("shared@x.com"), 1)
        involved = frappe.get_all(
            "Involved User", filters={"parent": self.thread.name}, pluck="account"
        )
        self.assertIn(TEST_USER, involved)

    def test_thread_is_locked_while_incremental_sync_updates_it(self):
        """No other sync can write the thread between the incremental sync reading and saving it."""
        locked = []
        self._sync(
            self._gmail(("m-lock", self.thread.name)),
            [_incoming("lock", "2026-10-12 10:43:16")],
            process_attachments=lambda email, thread, obj: locked.append(
                self._thread_is_locked(thread.name)
            ),
        )
        self.assertEqual(locked, [True])

    def test_thread_is_locked_while_initial_sync_updates_it(self):
        """No other sync can write the thread between the initial sync reading and saving it."""
        frappe.db.set_value("Gmail Account", self.account.name, "last_historyid", 0)
        frappe.db.commit()  # nosemgrep
        gmail = MagicMock()
        gmail.users().threads().list.return_value.execute.return_value = {
            "threads": [{"id": self.thread.name}]
        }
        gmail.users().threads().get.return_value.execute.return_value = {
            "messages": [{"id": "m-initial-lock", "historyId": "10"}]
        }
        gmail.users().messages().get.return_value.execute.return_value = {
            "labelIds": ["INBOX"]
        }
        locked = []
        self._sync(
            gmail,
            [_incoming("initial-lock", "2026-10-12 10:43:16")],
            process_attachments=lambda email, thread, obj: locked.append(
                self._thread_is_locked(thread.name)
            ),
        )
        self.assertEqual(locked, [True])

    def test_thread_lock_is_released_before_the_next_email(self):
        """A sync holds one thread at a time, so two syncs never wait on each other's threads (deadlock)."""
        second = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id=f"race-{frappe.generate_hash(length=10)}",
            emails=[_email_row("second-first", "2026-10-12 09:00:00")],
        )
        frappe.db.commit()  # nosemgrep
        first_still_locked = []

        def probe(email, thread, obj):
            if thread.name == second.name:
                first_still_locked.append(self._thread_is_locked(self.thread.name))

        self._sync(
            self._gmail(("m-a", self.thread.name), ("m-b", second.name)),
            [
                _incoming("a", "2026-10-12 10:43:16"),
                _incoming("b", "2026-10-12 10:43:17"),
            ],
            process_attachments=probe,
        )
        self.assertEqual(first_still_locked, [False])

    def test_lock_timeout_retries_the_job_instead_of_logging(self):
        """If a thread stays locked past the wait timeout, the job is retried rather than logged and skipped."""
        with (
            patch(
                f"{GMAIL_THREAD_MODULE}.lock_gmail_thread",
                side_effect=frappe.QueryTimeoutError("lock wait timeout"),
            ),
            patch(f"{GMAIL_THREAD_MODULE}.frappe.log_error") as mock_log,
            self.assertRaises(frappe.RetryBackgroundJobError),
        ):
            self._sync(
                self._gmail(("m-timeout", self.thread.name)),
                [_incoming("timeout", "2026-10-12 10:43:16")],
            )
        mock_log.assert_not_called()

    def test_retry_starts_from_the_sync_position_of_the_run(self):
        """A label synced before the lock timeout doesn't move the position the retry starts from."""
        account = _reset_account_for_sync(
            last_historyid=100, labels=(("INBOX", True), ("Label_2", True))
        )
        frappe.db.commit()  # nosemgrep
        gmail = MagicMock()

        def history_list(**kwargs):
            m = MagicMock()
            messages = (
                [{"id": "m-label-2", "threadId": self.thread.name}]
                if kwargs["labelId"] == "Label_2"
                else []
            )
            m.execute.return_value = {
                "history": [{"messages": messages}],
                "historyId": "500",
            }
            return m

        gmail.users().history().list.side_effect = history_list
        gmail.users().messages().get.return_value.execute.return_value = {
            "labelIds": ["Label_2"]
        }
        with (
            patch(
                f"{GMAIL_THREAD_MODULE}.lock_gmail_thread",
                side_effect=frappe.QueryTimeoutError("lock wait timeout"),
            ),
            self.assertRaises(frappe.RetryBackgroundJobError),
        ):
            self._sync(gmail, [_incoming("label-2", "2026-10-12 10:43:16")])
        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 100
        )


def _email_row(key, date_and_time):
    return {
        "gmail_message_id": f"gm-{key}-{frappe.generate_hash(length=6)}",
        "email_message_id": f"{key}@x.com",
        "subject": "Race",
        "sender": "someone@example.com",
        "date_and_time": date_and_time,
    }


def _incoming(key, date_and_time):
    """The (email, email_object) pair create_new_email returns for a new message."""
    email = frappe.new_doc("Single Email CT")
    email.update(_email_row(key, date_and_time))
    email_object = SimpleNamespace(
        message_id=f"{key}@x.com",
        from_email="someone@example.com",
        to=[TEST_USER],
        cc=[],
        bcc=[],
        mail={"References": None},
    )
    return email, email_object


class TestInitialSyncTransactions(_GmailThreadTestCase):
    """The initial sync imports each thread in one transaction."""

    def _sync_thread(self, dates, fail_on=None):
        """Initial-sync one thread whose messages carry `dates`; return (thread name, commit count)."""
        _reset_account_for_sync(last_historyid=0)
        key = frappe.generate_hash(length=8)
        thread_id = f"tx-{key}"
        gmail = MagicMock()
        gmail.users().threads().list.return_value.execute.return_value = {
            "threads": [{"id": thread_id}]
        }
        gmail.users().threads().get.return_value.execute.return_value = {
            "messages": [
                {"id": f"tx-{key}-{i}", "historyId": str(10 + i)}
                for i in range(len(dates))
            ]
        }
        gmail.users().messages().get.return_value.execute.return_value = {
            "labelIds": ["INBOX"]
        }

        def incoming(raw_email, gmail_account):
            i = incoming.calls
            incoming.calls += 1
            if i == fail_on:
                raise RuntimeError("unparseable message")
            email = frappe._dict(
                doctype="Single Email CT",
                gmail_message_id=f"tx-{key}-{i}",
                email_message_id=f"tx-{key}-{i}@x.com",
                subject="Import",
                date_and_time=dates[i],
            )
            email_object = SimpleNamespace(
                message_id=email.email_message_id,
                from_email=TEST_USER,
                to=[],
                cc=[],
                bcc=[],
                mail={"References": None},
            )
            return email, email_object

        incoming.calls = 0
        with as_user("Administrator"):
            with (
                change_settings(
                    "Google Settings", enable=1, client_id="cid", client_secret="csec"
                ),
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
                patch(f"{GMAIL_THREAD_MODULE}.create_new_email", side_effect=incoming),
                patch(f"{GMAIL_THREAD_MODULE}.find_gmail_thread", return_value=None),
                patch(f"{GMAIL_THREAD_MODULE}.process_attachments"),
                patch(f"{GMAIL_THREAD_MODULE}.replace_inline_images"),
                patch(f"{GMAIL_THREAD_MODULE}.frappe.log_error"),
                patch.object(frappe.db, "commit") as mock_commit,
            ):
                sync(user=TEST_USER)
        return thread_id, mock_commit.call_count

    def test_commits_once_per_thread_not_per_email(self):
        """A thread is committed once, whatever its number of emails, then the label's sync position once."""
        _, one_email = self._sync_thread(["2026-01-01 10:00:00"])
        _, three_emails = self._sync_thread(
            ["2026-01-01 10:00:00", "2026-01-01 11:00:00", "2026-01-01 12:00:00"]
        )
        self.assertEqual(one_email, 2)
        self.assertEqual(three_emails, 2)

    def test_thread_dates_and_owner_come_from_its_emails(self):
        """modified is the latest email's date, creation the earliest's, owner the account's user, whatever the order."""
        thread_id, _ = self._sync_thread(
            ["2026-01-01 10:00:00", "2026-01-01 09:00:00", "2026-01-01 11:00:00"]
        )
        thread = frappe.db.get_value(
            "Gmail Thread", thread_id, ["modified", "creation", "owner"], as_dict=True
        )
        self.assertEqual(str(thread.modified), "2026-01-01 11:00:00")
        self.assertEqual(str(thread.creation), "2026-01-01 09:00:00")
        self.assertEqual(thread.owner, TEST_USER)

    def test_emails_before_a_failing_message_keep_correct_dates(self):
        """When a later message of the thread fails, the emails already added still carry their own dates, not the import time."""
        thread_id, _ = self._sync_thread(
            ["2026-01-01 10:00:00", "2026-01-01 11:00:00"], fail_on=1
        )
        thread = frappe.db.get_value(
            "Gmail Thread", thread_id, ["modified", "creation"], as_dict=True
        )
        self.assertEqual(str(thread.modified), "2026-01-01 10:00:00")
        self.assertEqual(str(thread.creation), "2026-01-01 10:00:00")


def _throttled(status=429, reason="rateLimitExceeded", retry_after=None):
    """An HttpError as Gmail returns it when it throttles a mailbox."""
    headers = {"status": status}
    if retry_after is not None:
        headers["retry-after"] = retry_after
    content = json.dumps(
        {
            "error": {
                "code": status,
                "message": "Rate Limit Exceeded",
                "errors": [
                    {"domain": "usageLimits", "reason": reason, "message": "Limit"}
                ],
            }
        }
    ).encode()
    return googleapiclient.errors.HttpError(
        resp=httplib2.Response(headers), content=content
    )


class TestSyncRateLimits(_GmailThreadTestCase):
    def setUp(self):
        frappe.cache.delete_value(f"gmail_thread_sync_paused:{TEST_USER}")
        frappe.cache.delete_keys("gmail_thread_fetch_failures:")
        self.addCleanup(
            frappe.cache.delete_value, f"gmail_thread_sync_paused:{TEST_USER}"
        )
        self.addCleanup(frappe.cache.delete_keys, "gmail_thread_fetch_failures:")

    def _incremental_gmail(self, labels_history):
        """history().list returns or raises per label, as given in `labels_history`."""
        gmail = MagicMock()

        def history_list(**kwargs):
            m = MagicMock()
            outcome = labels_history[kwargs["labelId"]]
            if isinstance(outcome, Exception):
                m.execute.side_effect = outcome
            else:
                m.execute.return_value = outcome
            return m

        gmail.users().history().list.side_effect = history_list
        return gmail

    def _sync(self, gmail, **patches):
        with as_user("Administrator"):
            with (
                change_settings(
                    "Google Settings", enable=1, client_id="cid", client_secret="csec"
                ),
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
                patch(f"{GMAIL_THREAD_MODULE}.frappe.log_error") as mock_log,
            ):
                sync(user=TEST_USER)
        return mock_log

    def test_gmail_calls_are_retried_with_backoff(self):
        """Every Gmail call of the sync asks the client to retry 429s, 5xx and rate-limit 403s."""
        _reset_account_for_sync(last_historyid=0)
        gmail = MagicMock()
        gmail.users().threads().list.return_value.execute.return_value = {
            "threads": [{"id": "t-retry"}]
        }
        gmail.users().threads().get.return_value.execute.return_value = {
            "messages": [{"id": "m-retry", "historyId": "10"}]
        }
        gmail.users().messages().get.return_value.execute.return_value = {
            "labelIds": ["DRAFT"]
        }
        self._sync(gmail)

        _reset_account_for_sync(last_historyid=100)
        incremental = MagicMock()
        incremental.users().history().list.return_value.execute.return_value = {
            "history": [{"messages": [{"id": "m-retry-2", "threadId": "t-retry"}]}],
            "historyId": "200",
        }
        incremental.users().messages().get.return_value.execute.return_value = {
            "labelIds": ["DRAFT"]
        }
        self._sync(incremental)

        for call in (
            gmail.users().threads().list.return_value.execute,
            gmail.users().threads().get.return_value.execute,
            gmail.users().messages().get.return_value.execute,
            incremental.users().history().list.return_value.execute,
            incremental.users().messages().get.return_value.execute,
        ):
            call.assert_called_with(num_retries=GMAIL_API_RETRIES)

    def test_rate_limit_stops_the_run_and_keeps_the_sync_position(self):
        """A throttled label ends the run without an Error Log; labels synced before it don't move the position past it."""
        account = _reset_account_for_sync(
            last_historyid=100,
            labels=(("INBOX", True), ("STARRED", True), ("IMPORTANT", True)),
        )
        gmail = self._incremental_gmail(
            {
                "INBOX": {"history": [], "historyId": "500"},
                "STARRED": _throttled(),
                "IMPORTANT": {"history": [], "historyId": "500"},
            }
        )
        mock_log = self._sync(gmail)
        mock_log.assert_not_called()
        self.assertEqual(gmail.users().history().list.call_count, 2)
        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 100
        )
        self.assertTrue(is_sync_paused(TEST_USER))

    def test_throttled_message_is_fetched_again_next_run(self):
        """A message Gmail refuses to return because of a rate limit isn't skipped for good."""
        account = _reset_account_for_sync(last_historyid=100)
        gmail = self._incremental_gmail(
            {
                "INBOX": {
                    "history": [{"messages": [{"id": "m-429", "threadId": "t-429"}]}],
                    "historyId": "500",
                }
            }
        )
        gmail.users().messages().get.return_value.execute.side_effect = _throttled(
            status=403, reason="userRateLimitExceeded"
        )
        mock_log = self._sync(gmail)
        mock_log.assert_not_called()
        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 100
        )
        self.assertTrue(is_sync_paused(TEST_USER))

    def test_other_errors_are_still_logged_per_label(self):
        """Errors that aren't rate limits keep the existing behaviour: logged, and the next label is synced."""
        _reset_account_for_sync(
            last_historyid=100, labels=(("INBOX", True), ("STARRED", True))
        )
        gmail = self._incremental_gmail(
            {
                "INBOX": _throttled(status=403, reason="forbidden"),
                "STARRED": {"history": [], "historyId": "500"},
            }
        )
        mock_log = self._sync(gmail)
        mock_log.assert_called_once()
        self.assertEqual(gmail.users().history().list.call_count, 2)
        self.assertFalse(is_sync_paused(TEST_USER))

    def _sync_message_error(self, error):
        """Incrementally sync one message whose fetch raises `error`; returns (account, log mock)."""
        account = _reset_account_for_sync(last_historyid=100)
        gmail = self._incremental_gmail(
            {
                "INBOX": {
                    "history": [{"messages": [{"id": "m-err", "threadId": "t-err"}]}],
                    "historyId": "500",
                }
            }
        )
        gmail.users().messages().get.return_value.execute.side_effect = error
        with patch(f"{GMAIL_THREAD_MODULE}.create_new_email") as mock_create:
            mock_log = self._sync(gmail)
        mock_create.assert_not_called()
        return account, mock_log

    def _last_historyid(self, account):
        return frappe.db.get_value("Gmail Account", account.name, "last_historyid")

    def test_server_error_left_after_retries_keeps_the_message_for_next_run(self):
        """A message Gmail still fails to return after the retries (5xx) is fetched again on a later run."""
        account, mock_log = self._sync_message_error(
            _throttled(status=503, reason="backendError")
        )
        mock_log.assert_not_called()
        self.assertEqual(self._last_historyid(account), 100)
        self.assertTrue(is_sync_paused(TEST_USER))

    def test_server_error_in_initial_import_keeps_the_message_for_next_run(self):
        """The same holds while importing a label's threads for the first time."""
        account = _reset_account_for_sync(last_historyid=0)
        gmail = MagicMock()
        gmail.users().threads().list.return_value.execute.return_value = {
            "threads": [{"id": "t-503"}]
        }
        gmail.users().threads().get.return_value.execute.return_value = {
            "messages": [{"id": "m-503", "historyId": "10"}]
        }
        gmail.users().messages().get.return_value.execute.side_effect = _throttled(
            status=503, reason="backendError"
        )
        mock_log = self._sync(gmail)
        mock_log.assert_not_called()
        self.assertEqual(self._last_historyid(account), 0)
        self.assertTrue(is_sync_paused(TEST_USER))

    def test_deleted_message_is_skipped(self):
        """A message deleted since it was listed (404) is skipped and the sync moves on."""
        account, mock_log = self._sync_message_error(
            _throttled(status=404, reason="notFound")
        )
        mock_log.assert_not_called()
        self.assertEqual(self._last_historyid(account), 500)
        self.assertFalse(is_sync_paused(TEST_USER))

    def test_message_gmail_refuses_is_logged_and_skipped(self):
        """A message Gmail refuses (an error a retry won't fix) is logged and skipped, so the label still finishes."""
        account, mock_log = self._sync_message_error(
            _throttled(status=400, reason="invalidArgument")
        )
        mock_log.assert_called_once()
        self.assertFalse(is_sync_paused(TEST_USER))
        self.assertEqual(self._last_historyid(account), 500)

    def test_message_failing_on_every_attempt_is_skipped_after_the_limit(self):
        """A message Gmail fails to return on every run is logged and skipped after the last attempt, so the mailbox doesn't stall."""
        with patch(
            f"{GMAIL_THREAD_MODULE}.count_fetch_failure",
            return_value=MAX_FETCH_ATTEMPTS,
        ):
            account, mock_log = self._sync_message_error(
                _throttled(status=503, reason="backendError")
            )
        mock_log.assert_called_once()
        self.assertFalse(is_sync_paused(TEST_USER))
        self.assertEqual(self._last_historyid(account), 500)

    def test_failed_fetches_are_counted_per_item(self):
        """Each failed fetch of a thread or message adds to that item's own count."""
        self.assertEqual(count_fetch_failure(TEST_USER, "message m-a"), 1)
        self.assertEqual(count_fetch_failure(TEST_USER, "message m-a"), 2)
        self.assertEqual(count_fetch_failure(TEST_USER, "message m-b"), 1)

    def _initial_gmail_with_thread_error(self, error):
        gmail = MagicMock()
        gmail.users().threads().list.return_value.execute.return_value = {
            "threads": [{"id": "t-unavailable"}]
        }
        gmail.users().threads().get.return_value.execute.side_effect = error
        return gmail

    def test_thread_deleted_since_listed_is_skipped(self):
        """A thread deleted after the label was listed (404) is skipped quietly in the initial import."""
        _reset_account_for_sync(last_historyid=0)
        mock_log = self._sync(
            self._initial_gmail_with_thread_error(
                _throttled(status=404, reason="notFound")
            )
        )
        mock_log.assert_not_called()
        self.assertFalse(is_sync_paused(TEST_USER))

    def test_thread_server_error_pauses_the_initial_import(self):
        """A thread Gmail fails to return (5xx) stops the initial import to try again later."""
        account = _reset_account_for_sync(last_historyid=0)
        mock_log = self._sync(
            self._initial_gmail_with_thread_error(
                _throttled(status=503, reason="backendError")
            )
        )
        mock_log.assert_not_called()
        self.assertTrue(is_sync_paused(TEST_USER))
        self.assertEqual(self._last_historyid(account), 0)

    def test_initial_import_that_brings_nothing_hands_over_to_incremental_sync(self):
        """An initial import starts from the mailbox's current history id, so the next run is incremental even if nothing was imported."""
        account = _reset_account_for_sync(last_historyid=0)
        gmail = MagicMock()
        gmail.users().getProfile.return_value.execute.return_value = {
            "historyId": "777"
        }
        gmail.users().threads().list.return_value.execute.return_value = {}
        self._sync(gmail)
        self.assertEqual(self._last_historyid(account), 777)

    def test_throttled_profile_check_pauses_the_mailbox(self):
        """A rate limit or server error from Gmail before any label is synced pauses the mailbox instead of failing the job."""
        account = _reset_account_for_sync(last_historyid=100)
        with as_user("Administrator"):
            with (
                change_settings(
                    "Google Settings", enable=1, client_id="cid", client_secret="csec"
                ),
                patch(
                    f"{GMAIL_THREAD_MODULE}.get_gmail_object",
                    side_effect=_throttled(status=503, reason="backendError"),
                ),
                patch(f"{GMAIL_THREAD_MODULE}.frappe.log_error") as mock_log,
            ):
                sync(user=TEST_USER)
        mock_log.assert_not_called()
        self.assertTrue(is_sync_paused(TEST_USER))
        self.assertEqual(self._last_historyid(account), 100)

    def test_rate_limit_reasons_are_read_from_errors_and_details(self):
        """A 403 counts as a rate limit by the reasons in either list of Google's error body."""

        def forbidden(error_body):
            return googleapiclient.errors.HttpError(
                resp=httplib2.Response({"status": 403}),
                content=json.dumps({"error": {"code": 403, **error_body}}).encode(),
            )

        error_info = {
            "@type": "type.googleapis.com/google.rpc.ErrorInfo",
            "reason": "RATE_LIMIT_EXCEEDED",
        }
        self.assertTrue(
            is_rate_limit_error(
                forbidden(
                    {
                        "errors": [{"reason": "userRateLimitExceeded"}],
                        "details": [error_info],
                    }
                )
            )
        )
        self.assertTrue(is_rate_limit_error(forbidden({"details": [error_info]})))
        self.assertFalse(
            is_rate_limit_error(forbidden({"errors": [{"reason": "forbidden"}]}))
        )

    def test_incremental_sync_reads_every_history_page(self):
        """Every page of a label's history is read, and the position comes from the last page."""
        account = _reset_account_for_sync(last_historyid=100)
        gmail = MagicMock()
        pages = {
            None: {
                "history": [{"messages": [{"id": "m-page-1", "threadId": "t-p"}]}],
                "nextPageToken": "page-2",
                "historyId": "500",
            },
            "page-2": {
                "history": [{"messages": [{"id": "m-page-2", "threadId": "t-p"}]}],
                "historyId": "500",
            },
        }

        def history_list(**kwargs):
            m = MagicMock()
            m.execute.return_value = pages[kwargs["pageToken"]]
            return m

        gmail.users().history().list.side_effect = history_list
        gmail.users().messages().get.return_value.execute.return_value = {
            "labelIds": ["DRAFT"]
        }
        self._sync(gmail)
        fetched = [
            c.kwargs["id"]
            for c in gmail.users().messages().get.call_args_list
            if c.kwargs
        ]
        self.assertEqual(fetched, ["m-page-1", "m-page-2"])
        self.assertEqual(self._last_historyid(account), 500)

    def test_paused_mailbox_is_not_synced(self):
        """While a mailbox is paused, its sync job returns without calling Gmail."""
        _reset_account_for_sync(last_historyid=100)
        pause_sync(TEST_USER, 60)
        with as_user("Administrator"):
            with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object") as mock_get:
                sync(user=TEST_USER)
        mock_get.assert_not_called()

    def test_pause_length_follows_retry_after(self):
        """The pause lasts as long as Google's Retry-After asks, within one minute to one day, else 15 minutes."""
        self.assertEqual(get_retry_after(_throttled(retry_after="300")), 300)
        self.assertEqual(get_retry_after(_throttled(retry_after="1")), 60)
        self.assertEqual(get_retry_after(_throttled(retry_after="999999")), 86400)
        self.assertEqual(get_retry_after(_throttled()), 15 * 60)
        in_ten_minutes = format_datetime(
            datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(minutes=10),
            usegmt=True,
        )
        self.assertAlmostEqual(
            get_retry_after(_throttled(retry_after=in_ten_minutes)), 600, delta=5
        )


class TestInitialSyncThreadCap(_GmailThreadTestCase):
    def _gmail_with_pages(self, pages):
        gmail = MagicMock()
        responses = iter(pages)
        gmail.users().threads().list.return_value.execute.side_effect = (
            lambda **kwargs: next(responses)
        )
        gmail.users().threads().get.return_value.execute.return_value = {"messages": []}
        return gmail

    def _page(self, start, count, next_token=None):
        page = {"threads": [{"id": f"cap-{i}"} for i in range(start, start + count)]}
        if next_token:
            page["nextPageToken"] = next_token
        return page

    def test_pages_through_the_label_up_to_the_cap(self):
        """The initial sync reads further pages of a label until it has the configured number of threads."""
        _reset_account_for_sync(last_historyid=0)
        gmail = self._gmail_with_pages(
            [self._page(0, 2, "p2"), self._page(2, 2, "p3"), self._page(4, 2, "p4")]
        )
        with as_user("Administrator"):
            with (
                change_settings(
                    "Google Settings", enable=1, client_id="cid", client_secret="csec"
                ),
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
                patch(
                    f"{GMAIL_THREAD_MODULE}.get_max_threads_per_label", return_value=5
                ),
            ):
                sync(user=TEST_USER)
        list_calls = gmail.users().threads().list.call_args_list
        self.assertEqual(
            [(c.kwargs["maxResults"], c.kwargs["pageToken"]) for c in list_calls],
            [(5, None), (3, "p2"), (1, "p3")],
        )
        fetched = [c.kwargs["id"] for c in gmail.users().threads().get.call_args_list]
        # the newest five, imported oldest first
        self.assertEqual(fetched, ["cap-4", "cap-3", "cap-2", "cap-1", "cap-0"])

    def test_stops_when_the_label_has_no_more_pages(self):
        """A label with fewer threads than the cap is read in one request."""
        threads = list_label_threads(
            self._gmail_with_pages([self._page(0, 3)]), "INBOX", 100
        )
        self.assertEqual(len(threads), 3)

    def test_cap_defaults_to_one_hundred(self):
        """Without a configured cap, the first sync imports a label's newest 100 threads, as before."""
        with change_settings("Google Settings", custom_gmail_max_threads_per_label=0):
            self.assertEqual(get_max_threads_per_label(), 100)
        with change_settings("Google Settings", custom_gmail_max_threads_per_label=250):
            self.assertEqual(get_max_threads_per_label(), 250)


class TestSyncErrorHandling(_GmailThreadTestCase):
    def test_resets_last_historyid_on_history_api_not_found(self):
        """A notFound HttpError from history().list resets last_historyid to 0."""
        account = _reset_account_for_sync(last_historyid=100)
        gmail = MagicMock()
        gmail.users().history().list.return_value.execute.side_effect = (
            _make_http_error("notFound")
        )
        with as_user("Administrator"):
            with change_settings(
                "Google Settings", enable=1, client_id="cid", client_secret="csec"
            ):
                with patch(
                    f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail
                ):
                    sync(user=TEST_USER)
        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 0
        )

    def _gmail_with_history(self, deleted_labels, expired_labels=()):
        """Gmail mock: history().list 404s for deleted/expired labels; labels().get 404s only for deleted ones."""
        gmail = MagicMock()

        def history_list(**kwargs):
            m = MagicMock()
            if kwargs["labelId"] in (*deleted_labels, *expired_labels):
                m.execute.side_effect = _make_http_error("notFound")
            else:
                m.execute.return_value = {"history": [], "historyId": "500"}
            return m

        def labels_get(userId, id):
            m = MagicMock()
            if id in deleted_labels:
                m.execute.side_effect = _make_http_error("notFound")
            else:
                m.execute.return_value = {"id": id}
            return m

        gmail.users().history().list.side_effect = history_list
        gmail.users().labels().get.side_effect = labels_get
        return gmail

    def _label_ids(self, account_name):
        return set(
            frappe.get_all(
                "Gmail Label", filters={"parent": account_name}, pluck="label_id"
            )
        )

    def test_label_deleted_in_gmail_is_dropped_without_resetting_sync(self):
        """A history 404 for a label Gmail no longer has removes that label and keeps syncing the others incrementally."""
        account = _reset_account_for_sync(
            last_historyid=100,
            labels=(("Label_deleted", True), ("INBOX", True)),
        )
        gmail = self._gmail_with_history(deleted_labels=("Label_deleted",))
        with as_user("Administrator"):
            with (
                change_settings(
                    "Google Settings", enable=1, client_id="cid", client_secret="csec"
                ),
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
                patch(f"{GMAIL_THREAD_MODULE}.frappe.log_error") as mock_log,
            ):
                sync(user=TEST_USER)
        mock_log.assert_not_called()
        self.assertEqual(self._label_ids(account.name), {"INBOX"})
        # INBOX was still synced and the sync position moved forward, not back to 0
        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 500
        )

    def test_dropped_label_is_committed_before_the_next_label_syncs(self):
        """The dropped label row is committed right away, so the account's labels aren't locked while other labels sync."""
        _reset_account_for_sync(
            last_historyid=100,
            labels=(("Label_deleted", True), ("INBOX", True)),
        )
        gmail = self._gmail_with_history(deleted_labels=("Label_deleted",))
        events = []
        history_list = gmail.users().history().list.side_effect

        def recording_history_list(**kwargs):
            events.append(f"history {kwargs['labelId']}")
            return history_list(**kwargs)

        gmail.users().history().list.side_effect = recording_history_list
        with as_user("Administrator"):
            with (
                change_settings(
                    "Google Settings", enable=1, client_id="cid", client_secret="csec"
                ),
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
                patch.object(
                    frappe.db, "commit", side_effect=lambda: events.append("commit")
                ),
            ):
                sync(user=TEST_USER)
        self.assertEqual(
            events[:3], ["history Label_deleted", "commit", "history INBOX"]
        )

    def test_expired_history_still_resets_sync(self):
        """A history 404 for a label that still exists means the history id expired: reset for an initial sync."""
        account = _reset_account_for_sync(last_historyid=100)
        gmail = self._gmail_with_history(deleted_labels=(), expired_labels=("INBOX",))
        with as_user("Administrator"):
            with (
                change_settings(
                    "Google Settings", enable=1, client_id="cid", client_secret="csec"
                ),
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
            ):
                sync(user=TEST_USER)
        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 0
        )
        self.assertEqual(self._label_ids(account.name), {"INBOX"})

    def test_reset_after_a_dropped_label_does_not_restore_it(self):
        """When one label was deleted in Gmail and another's history expired, the reset keeps the deleted label out."""
        account = _reset_account_for_sync(
            last_historyid=100,
            labels=(("Label_deleted", True), ("INBOX", True)),
        )
        gmail = self._gmail_with_history(
            deleted_labels=("Label_deleted",), expired_labels=("INBOX",)
        )
        with as_user("Administrator"):
            with (
                change_settings(
                    "Google Settings", enable=1, client_id="cid", client_secret="csec"
                ),
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
            ):
                sync(user=TEST_USER)
        self.assertEqual(self._label_ids(account.name), {"INBOX"})
        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 0
        )

    def test_updates_last_historyid_to_max_observed(self):
        """After incremental sync, last_historyid is set to the max historyId observed (the new historyId returned by Gmail)."""
        account = _reset_account_for_sync(last_historyid=100)
        gmail = MagicMock()
        gmail.users().history().list.return_value.execute.return_value = {
            "history": [],
            "historyId": "500",
        }
        with as_user("Administrator"):
            with change_settings(
                "Google Settings", enable=1, client_id="cid", client_secret="csec"
            ):
                with patch(
                    f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail
                ):
                    sync(user=TEST_USER)
        self.assertEqual(
            frappe.db.get_value("Gmail Account", account.name, "last_historyid"), 500
        )

    def test_logs_error_and_continues_to_next_label_on_exception(self):
        """An exception in one label's processing is logged via frappe.log_error and the next label is still processed."""
        _reset_account_for_sync(
            last_historyid=100, labels=(("INBOX", True), ("STARRED", True))
        )
        gmail = MagicMock()
        call_count = {"n": 0}

        def history_list_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                m = MagicMock()
                m.execute.side_effect = RuntimeError("boom")
                return m
            m = MagicMock()
            m.execute.return_value = {"history": [], "historyId": "200"}
            return m

        gmail.users().history().list.side_effect = history_list_side_effect
        with as_user("Administrator"):
            with (
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
                patch(f"{GMAIL_THREAD_MODULE}.frappe.log_error") as mock_log,
            ):
                sync(user=TEST_USER)
        mock_log.assert_called()
        self.assertEqual(call_count["n"], 2)


class TestSyncMessageHandling(_GmailThreadTestCase):
    def _build_initial_gmail(self, *, thread_id, message_id, label_ids):
        """Build a gmail mock returning one initial-sync thread with one message having the given labelIds."""
        gmail = MagicMock()
        gmail.users().threads().list.return_value.execute.return_value = {
            "threads": [{"id": thread_id}]
        }
        gmail.users().threads().get.return_value.execute.return_value = {
            "messages": [{"id": message_id, "historyId": "10"}]
        }
        gmail.users().messages().get.return_value.execute.return_value = {
            "labelIds": label_ids
        }
        return gmail

    def test_skips_draft_labelled_messages(self):
        """A message whose labelIds include DRAFT is skipped (create_new_email is not called)."""
        _reset_account_for_sync(last_historyid=0)
        gmail = self._build_initial_gmail(
            thread_id="t-draft", message_id="m-draft", label_ids=["INBOX", "DRAFT"]
        )
        with as_user("Administrator"):
            with (
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
                patch(f"{GMAIL_THREAD_MODULE}.create_new_email") as mock_create,
            ):
                sync(user=TEST_USER)
        mock_create.assert_not_called()

    def test_continues_on_already_exists_error(self):
        """When create_new_email raises AlreadyExistsError, sync skips that message and continues without raising."""
        _reset_account_for_sync(last_historyid=0)
        gmail = self._build_initial_gmail(
            thread_id="t-exists", message_id="m-exists", label_ids=["INBOX"]
        )
        with as_user("Administrator"):
            with (
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
                patch(
                    f"{GMAIL_THREAD_MODULE}.create_new_email",
                    side_effect=AlreadyExistsError(),
                ),
                patch(f"{GMAIL_THREAD_MODULE}.find_gmail_thread", return_value=None),
            ):
                sync(user=TEST_USER)

    def test_initial_sync_adds_user_to_thread_imported_by_another_mailbox(self):
        """An email another mailbox imported under its own Gmail thread id adds this mailbox's user to that thread."""
        account = _reset_account_for_sync(last_historyid=0)
        frappe.db.set_value("User", TEST_USER, "user_type", "System User")
        make_test_user(TEST_USER_2)
        key = frappe.generate_hash(length=10)
        rfc_message_id = f"shared-{key}@x.com"
        # imported by TEST_USER_2's mailbox, under that mailbox's thread id; the thread's
        # Gmail Account isn't looked at, and a second one would outlive this class (sync commits)
        other_thread = make_test_gmail_thread(
            gmail_account=account.name,
            gmail_thread_id=f"t-other-{key}",
            involved_users=[TEST_USER_2],
            emails=[
                {
                    "gmail_message_id": f"m-other-{key}",
                    "email_message_id": rfc_message_id,
                    "subject": "Shared",
                }
            ],
        )
        modified = get_datetime(other_thread.modified)
        gmail = self._build_initial_gmail(
            thread_id=f"t-this-{key}", message_id=f"m-this-{key}", label_ids=["INBOX"]
        )
        # sent to a group address: this mailbox's user isn't in the headers
        gmail.users().threads().get.return_value.execute.return_value = {
            "messages": [
                {
                    "id": f"m-this-{key}",
                    "historyId": "10",
                    "payload": {
                        "headers": [
                            {"name": "Message-ID", "value": f"<{rfc_message_id}>"},
                            {"name": "From", "value": "someone@example.com"},
                            {"name": "To", "value": "team@example.com"},
                        ]
                    },
                }
            ]
        }
        with as_user("Administrator"):
            with (
                change_settings(
                    "Google Settings", enable=1, client_id="cid", client_secret="csec"
                ),
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
                patch(f"{GMAIL_THREAD_MODULE}.create_new_email") as mock_create,
            ):
                sync(user=TEST_USER)
        mock_create.assert_not_called()
        involved = frappe.get_all(
            "Involved User", filters={"parent": other_thread.name}, pluck="account"
        )
        self.assertIn(TEST_USER, involved)
        self.assertEqual(
            frappe.db.count("Single Email CT", {"email_message_id": rfc_message_id}), 1
        )
        self.assertEqual(
            frappe.db.get_value("Gmail Thread", other_thread.name, "modified"), modified
        )
        # this mailbox's later emails in its thread find the same Gmail Thread
        self.assertEqual(
            real_find_gmail_thread(f"t-this-{key}").name, other_thread.name
        )

    def test_aggregates_involved_users_from_sender_recipients_and_account_owner(self):
        """update_involved_users is called with the set of sender + to + cc + bcc + the gmail_account's linked_user."""
        _reset_account_for_sync(last_historyid=100)
        gmail = MagicMock()
        gmail.users().history().list.return_value.execute.return_value = {
            "history": [{"messages": [{"id": "m-iu", "threadId": "t-iu"}]}],
            "historyId": "200",
        }
        gmail.users().messages().get.return_value.execute.return_value = {
            "labelIds": ["INBOX"]
        }
        mock_thread = MagicMock()
        mock_thread.name = "t-iu"
        mock_thread.reference_doctype = None
        mock_thread.reference_name = None
        mock_thread.subject_of_first_mail = "Hi"
        mock_thread.creation = None
        _configure_mock_child_tables(mock_thread, "emails", "references")
        mock_email = MagicMock(subject="Hi", date_and_time="2026-01-01 10:00:00")
        mock_email_object = SimpleNamespace(
            message_id="mid-iu",
            from_email="sender@example.com",
            to=["to1@example.com"],
            cc=["cc1@example.com"],
            bcc=["bcc1@example.com"],
            mail={"References": None},
        )
        with as_user("Administrator"):
            with (
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
                patch(
                    f"{GMAIL_THREAD_MODULE}.create_new_email",
                    return_value=(mock_email, mock_email_object),
                ),
                patch(
                    f"{GMAIL_THREAD_MODULE}.find_gmail_thread", return_value=mock_thread
                ),
                patch(
                    f"{GMAIL_THREAD_MODULE}.lock_gmail_thread", return_value=mock_thread
                ),
                patch(f"{GMAIL_THREAD_MODULE}.update_involved_users") as mock_uiu,
                patch(f"{GMAIL_THREAD_MODULE}.process_attachments"),
                patch(f"{GMAIL_THREAD_MODULE}.replace_inline_images"),
            ):
                sync(user=TEST_USER)
        args, _ = mock_uiu.call_args
        self.assertEqual(
            args[1],
            {
                "sender@example.com",
                "to1@example.com",
                "cc1@example.com",
                "bcc1@example.com",
                TEST_USER,
            },
        )

    def test_publishes_realtime_for_linked_threads_on_incremental_sync(self):
        """Incremental sync emits publish_realtime('gthread_new_email', doctype, docname) for every thread that has a reference."""
        _reset_account_for_sync(last_historyid=100)
        gmail = MagicMock()
        gmail.users().history().list.return_value.execute.return_value = {
            "history": [{"messages": [{"id": "m-link", "threadId": "t-link"}]}],
            "historyId": "200",
        }
        gmail.users().messages().get.return_value.execute.return_value = {
            "labelIds": ["INBOX"]
        }
        mock_thread = MagicMock()
        mock_thread.name = "t-link"
        mock_thread.reference_doctype = "User"
        mock_thread.reference_name = TEST_USER
        mock_thread.subject_of_first_mail = "Hello"
        mock_thread.creation = None
        _configure_mock_child_tables(mock_thread, "emails", "references")
        mock_email = MagicMock(subject="Hello", date_and_time="2026-01-01 10:00:00")
        mock_email_object = SimpleNamespace(
            message_id="mid-link",
            from_email=TEST_USER,
            to=[],
            cc=[],
            bcc=[],
            mail={"References": None},
        )
        with as_user("Administrator"):
            with change_settings(
                "Google Settings", enable=1, client_id="cid", client_secret="csec"
            ):
                with (
                    patch(
                        f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail
                    ),
                    patch(
                        f"{GMAIL_THREAD_MODULE}.create_new_email",
                        return_value=(mock_email, mock_email_object),
                    ),
                    patch(
                        f"{GMAIL_THREAD_MODULE}.find_gmail_thread",
                        return_value=mock_thread,
                    ),
                    patch(
                        f"{GMAIL_THREAD_MODULE}.lock_gmail_thread",
                        return_value=mock_thread,
                    ),
                    patch(f"{GMAIL_THREAD_MODULE}.update_involved_users"),
                    patch(f"{GMAIL_THREAD_MODULE}.process_attachments"),
                    patch(f"{GMAIL_THREAD_MODULE}.replace_inline_images"),
                    patch("frappe.publish_realtime") as mock_publish,
                ):
                    sync(user=TEST_USER)
        mock_publish.assert_any_call(
            "gthread_new_email", doctype="User", docname=TEST_USER
        )

    def test_creates_new_thread_with_subject_creation_and_owner(self):
        """Initial sync: when find_gmail_thread returns None, a new Gmail Thread is created with subject_of_first_mail, creation, modified, and owner all set from the email."""
        account = _reset_account_for_sync(last_historyid=0)
        gmail = MagicMock()
        gmail.users().threads().list.return_value.execute.return_value = {
            "threads": [{"id": "t-create-new"}]
        }
        gmail.users().threads().get.return_value.execute.return_value = {
            "messages": [{"id": "m-create-new", "historyId": "10"}]
        }
        gmail.users().messages().get.return_value.execute.return_value = {
            "labelIds": ["INBOX"]
        }
        email_dict = frappe._dict(
            {
                "doctype": "Single Email CT",
                "gmail_message_id": "mid-create-new",
                "subject": "First subject",
                "date_and_time": "2026-01-01 10:00:00",
            }
        )
        mock_email_object = SimpleNamespace(
            message_id="mid-create-new",
            from_email=TEST_USER,
            to=[],
            cc=[],
            bcc=[],
            mail={"References": None},
        )

        def patched_update_involved_users(thread, users):
            for u in users:
                if frappe.db.exists("User", u):
                    thread.append("involved_users", {"account": u})

        with as_user("Administrator"):
            with (
                patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail),
                patch(
                    f"{GMAIL_THREAD_MODULE}.create_new_email",
                    return_value=(email_dict, mock_email_object),
                ),
                patch(f"{GMAIL_THREAD_MODULE}.find_gmail_thread", return_value=None),
                patch(
                    f"{GMAIL_THREAD_MODULE}.update_involved_users",
                    side_effect=patched_update_involved_users,
                ),
                patch(f"{GMAIL_THREAD_MODULE}.process_attachments"),
                patch(f"{GMAIL_THREAD_MODULE}.replace_inline_images"),
            ):
                sync(user=TEST_USER)
        self.assertTrue(frappe.db.exists("Gmail Thread", "t-create-new"))
        thread = frappe.get_doc("Gmail Thread", "t-create-new")
        self.assertEqual(thread.subject_of_first_mail, "First subject")
        self.assertEqual(thread.owner, account.linked_user)
        self.assertEqual(str(thread.creation), "2026-01-01 10:00:00")
