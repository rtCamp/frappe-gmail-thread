from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import frappe
import googleapiclient.errors
from frappe.tests import IntegrationTestCase, change_settings

from frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread import (
    get_permission_query_conditions,
    has_permission,
    sync,
    sync_labels,
    update_involved_users,
)
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

GMAIL_THREAD_MODULE = (
    "frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread"
)


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
        mock_save.assert_called_once_with(ignore_permissions=True)


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
    def test_throws_when_gmail_not_enabled(self):
        """sync throws when gmail_enabled is 0."""
        account = _reset_account_for_sync()
        frappe.db.set_value("Gmail Account", account.name, "gmail_enabled", 0)
        with as_user("Administrator"):
            with self.assertRaises(frappe.ValidationError):
                sync(user=TEST_USER)

    def test_throws_when_no_refresh_token(self):
        """sync throws when refresh_token is empty."""
        account = _reset_account_for_sync()
        set_password_field("Gmail Account", account.name, "refresh_token", "")
        with as_user("Administrator"):
            with self.assertRaises(frappe.ValidationError):
                sync(user=TEST_USER)

    def test_returns_silently_when_no_enabled_labels(self):
        """sync returns silently when no labels are enabled (no API calls)."""
        _reset_account_for_sync(labels=(("INBOX", False),))
        gmail = MagicMock()
        with as_user("Administrator"):
            with patch(f"{GMAIL_THREAD_MODULE}.get_gmail_object", return_value=gmail):
                sync(user=TEST_USER)
        gmail.users().threads().list.assert_not_called()
        gmail.users().history().list.assert_not_called()


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
