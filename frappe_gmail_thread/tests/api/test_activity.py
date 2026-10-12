import json
from contextlib import contextmanager
from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase

from frappe_gmail_thread.api.activity import (
    get_attachments_data,
    get_linked_gmail_threads,
    relink_gmail_thread,
    unlink_gmail_thread,
)
from frappe_gmail_thread.tests import (
    TEST_USER,
    as_user,
    make_test_gmail_account,
    make_test_gmail_thread,
    make_test_user,
)


class TestGetAttachmentsData(IntegrationTestCase):
    def test_returns_attachments_with_fresh_file_url_from_db(self):
        """get_attachments_data fetches each attachment's file_url fresh from the File doctype (not the stale value cached on the email row)."""
        file_doc = frappe.get_doc(
            {
                "doctype": "File",
                "file_name": "fresh.txt",
                "content": b"hi",
                "is_private": 1,
            }
        ).insert(ignore_permissions=True)
        email = frappe._dict(
            {
                "attachments_data": json.dumps(
                    [{"file_doc_name": file_doc.name, "file_url": "/old/stale.txt"}]
                )
            }
        )
        result = get_attachments_data(email)
        self.assertEqual(result[0]["file_url"], file_doc.file_url)


class TestGetLinkedGmailThreads(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            cls.account = make_test_gmail_account(linked_user=TEST_USER)

    def test_returns_one_timeline_entry_per_email_for_matching_threads(self):
        """get_linked_gmail_threads returns one timeline-entry dict per email for every Gmail Thread referencing the target doc."""
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="timeline-thread-1",
            reference_doctype="User",
            reference_name=TEST_USER,
            emails=[
                {
                    "gmail_message_id": "tl-mid-1",
                    "email_message_id": "<tl-1@x.com>",
                    "subject": "First",
                    "sender": TEST_USER,
                    "attachments_data": "[]",
                },
                {
                    "gmail_message_id": "tl-mid-2",
                    "email_message_id": "<tl-2@x.com>",
                    "subject": "Second",
                    "sender": "stranger@x.com",
                    "attachments_data": "[]",
                },
            ],
        )
        result = get_linked_gmail_threads("User", TEST_USER)
        self.assertEqual(len(result), 2)
        subjects = [r["template_data"]["doc"]["subject"] for r in result]
        self.assertEqual(set(subjects), {"First", "Second"})
        for entry in result:
            self.assertEqual(entry["doctype"], "Gmail Thread")
            self.assertEqual(entry["icon"], "mail")
            self.assertEqual(entry["template"], "timeline_message_box")

    def _make_file(self, name):
        return frappe.get_doc(
            {
                "doctype": "File",
                "file_name": name,
                "content": name.encode(),
                "is_private": 1,
            }
        ).insert(ignore_permissions=True)

    def _make_linked_thread(self, thread_id, reference_name, n_emails=2):
        emails = []
        for i in range(n_emails):
            file_doc = self._make_file(f"{thread_id}-{i}.txt")
            emails.append(
                {
                    "gmail_message_id": f"{thread_id}-mid-{i}",
                    "email_message_id": f"<{thread_id}-{i}@x.com>",
                    "subject": f"{thread_id} {i}",
                    "sender": TEST_USER,
                    "attachments_data": json.dumps(
                        [{"file_doc_name": file_doc.name, "file_name": "a.txt"}]
                    ),
                }
            )
        return make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id=thread_id,
            reference_doctype="User",
            reference_name=reference_name,
            emails=emails,
        )

    @contextmanager
    def _count_queries(self):
        queries = []
        orig_sql = frappe.db.__class__.sql

        def counting_sql(*args, **kwargs):
            queries.append(1)
            return orig_sql(*args, **kwargs)

        frappe.db.__class__.sql = counting_sql
        try:
            yield queries
        finally:
            frappe.db.__class__.sql = orig_sql

    def test_resolves_attachments_to_current_file_urls(self):
        """Attachments resolve to their File's current URL; entries whose File is gone, or that have no URL, are dropped."""
        target = "test_fgt_timeline_attachments@example.com"
        make_test_user(target)
        live = self._make_file("timeline-live.txt")
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="timeline-attachments",
            reference_doctype="User",
            reference_name=target,
            emails=[
                {
                    "gmail_message_id": "tl-att-1",
                    "email_message_id": "<tl-att-1@x.com>",
                    "subject": "With attachments",
                    "sender": TEST_USER,
                    "attachments_data": json.dumps(
                        [
                            {"file_doc_name": live.name, "file_url": "/old/stale.txt"},
                            {"file_doc_name": "no-such-file"},
                            {"file_url": "/files/external.png"},
                            {"file_name": "no-url.txt"},
                            "not-a-dict",
                        ]
                    ),
                }
            ],
        )
        result = get_linked_gmail_threads("User", target)
        urls = [a["file_url"] for a in result[0]["template_data"]["doc"]["attachments"]]
        self.assertEqual(urls, [live.file_url, "/files/external.png"])

    def test_query_count_does_not_grow_with_threads_and_emails(self):
        """The timeline costs the same number of queries for one linked thread as for several threads with more emails and attachments."""
        target = "test_fgt_timeline_queries@example.com"
        make_test_user(target)
        self._make_linked_thread("timeline-q-1", target, n_emails=1)
        get_linked_gmail_threads("User", target)  # warm caches
        with self._count_queries() as queries:
            single = get_linked_gmail_threads("User", target)
        self.assertEqual(len(single), 1)

        for i in range(2, 5):
            self._make_linked_thread(f"timeline-q-{i}", target, n_emails=3)
        get_linked_gmail_threads("User", target)
        with self.assertQueryCount(len(queries)):
            many = get_linked_gmail_threads("User", target)
        self.assertEqual(len(many), 10)
        self.assertEqual(
            sum(len(e["template_data"]["doc"]["attachments"]) for e in many), 10
        )

    def test_user_sees_threads_of_a_document_they_can_read(self):
        """A user who can read the linked document sees its threads, without owning them or being involved in them."""
        reader = "test_fgt_timeline_reader@example.com"
        make_test_user(reader)
        # a user without roles is saved as a Website User; desk users get the Desk User role
        frappe.db.set_value("User", reader, "user_type", "System User")
        frappe.clear_cache(user=reader)
        readable = frappe.get_doc(
            {"doctype": "ToDo", "description": "Timeline", "allocated_to": reader}
        ).insert(ignore_permissions=True)
        unreadable = frappe.get_doc(
            {"doctype": "ToDo", "description": "Timeline, someone else's"}
        ).insert(ignore_permissions=True)
        for thread_id, todo, subject in (
            ("timeline-reader-1", readable, "On the readable ToDo"),
            ("timeline-reader-2", unreadable, "On someone else's ToDo"),
        ):
            make_test_gmail_thread(
                gmail_account=self.account.name,
                gmail_thread_id=thread_id,
                reference_doctype="ToDo",
                reference_name=todo.name,
                emails=[
                    {
                        "gmail_message_id": f"{thread_id}-mid",
                        "email_message_id": f"{thread_id}@x.com",
                        "subject": subject,
                        "sender": TEST_USER,
                        "attachments_data": "[]",
                    }
                ],
            )

        with as_user(reader):
            result = get_linked_gmail_threads("ToDo", readable.name)
            with self.assertRaises(frappe.PermissionError):
                get_linked_gmail_threads("ToDo", unreadable.name)

        self.assertEqual(
            [r["template_data"]["doc"]["subject"] for r in result],
            ["On the readable ToDo"],
        )

    def test_returns_empty_list_when_no_thread_references_the_doc(self):
        """get_linked_gmail_threads returns [] when no Gmail Thread points at the supplied (doctype, docname)."""
        result = get_linked_gmail_threads("User", "no-such-user@example.com")
        self.assertEqual(result, [])


class TestRelinkGmailThread(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            cls.account = make_test_gmail_account(linked_user=TEST_USER)

    def test_updates_reference_fields_and_saves(self):
        """relink_gmail_thread updates reference_doctype + reference_name and saves so the lifecycle hooks transition status."""
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="relink-thread",
            status="Open",
        )
        with patch(
            "frappe_gmail_thread.api.activity.frappe.has_permission",
            return_value=True,
        ):
            relink_gmail_thread("relink-thread", "User", TEST_USER)
        thread = frappe.get_doc("Gmail Thread", "relink-thread")
        self.assertEqual(thread.reference_doctype, "User")
        self.assertEqual(thread.reference_name, TEST_USER)


class TestUnlinkGmailThread(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            cls.account = make_test_gmail_account(linked_user=TEST_USER)

    def test_clears_reference_fields_and_saves(self):
        """unlink_gmail_thread clears reference_doctype + reference_name and saves so the lifecycle hooks transition status back to Open."""
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="unlink-thread",
            reference_doctype="User",
            reference_name=TEST_USER,
            status="Linked",
        )
        with patch(
            "frappe_gmail_thread.api.activity.frappe.has_permission",
            return_value=True,
        ):
            unlink_gmail_thread("unlink-thread")
        thread = frappe.get_doc("Gmail Thread", "unlink-thread")
        self.assertIsNone(thread.reference_doctype)
        self.assertIsNone(thread.reference_name)
