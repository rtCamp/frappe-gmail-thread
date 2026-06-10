import json
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
