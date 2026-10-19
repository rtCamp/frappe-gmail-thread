import base64
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from unittest.mock import MagicMock, patch

import frappe
from frappe.tests import IntegrationTestCase
from frappe.utils import get_datetime

from frappe_gmail_thread.tests import (
    TEST_USER,
    TEST_USER_2,
    as_user,
    make_test_gmail_account,
    make_test_gmail_thread,
    make_test_user,
)
from frappe_gmail_thread.utils.helpers import (
    AlreadyExistsError,
    GmailInboundMail,
    create_new_email,
    find_gmail_thread,
    html_to_text,
    lock_gmail_thread,
    process_attachments,
    replace_inline_images,
)

HELPERS_MODULE = "frappe_gmail_thread.utils.helpers"


def _build_raw_email(
    *,
    sender="sender@example.com",
    recipient="recipient@example.com",
    cc=None,
    bcc=None,
    subject="Test Subject",
    message_id="<test-1@example.com>",
    text_body="Hello",
    html_body=None,
):
    """Build a minimal MIME email string suitable for GmailInboundMail."""
    if html_body:
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))
    else:
        msg = MIMEText(text_body, "plain")
    msg["From"] = sender
    msg["To"] = recipient
    if cc:
        msg["Cc"] = cc
    if bcc:
        msg["Bcc"] = bcc
    msg["Subject"] = subject
    msg["Message-ID"] = message_id
    msg["Date"] = "Mon, 1 Jan 2026 10:00:00 +0000"
    return msg.as_string()


def _build_gmail_inbound_mail(**kwargs):
    """Return a GmailInboundMail built from a minimal valid email; useful as a vehicle for testing its methods on custom inputs."""
    return GmailInboundMail(content=_build_raw_email(**kwargs))


def _build_email_dict(*, msg_id="m1", **kwargs):
    """Build the dict shape create_new_email expects: {id, raw} with raw urlsafe-base64-encoded."""
    raw = _build_raw_email(**kwargs)
    return {
        "id": msg_id,
        "raw": base64.urlsafe_b64encode(raw.encode()).decode("ASCII"),
    }


class TestHtmlToText(IntegrationTestCase):
    def test_extracts_plain_text_with_space_separator(self):
        """html_to_text returns BeautifulSoup-extracted text with ' ' joining adjacent elements."""
        result = html_to_text("<p>Hello</p><p>World</p>")
        self.assertEqual(result, "Hello World")

    def test_strips_outer_whitespace(self):
        """html_to_text strips leading and trailing whitespace from the extracted text."""
        result = html_to_text("  <p>Hello</p>  ")
        self.assertEqual(result, "Hello")


class TestPopDownQuotedRepliesText(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mail = _build_gmail_inbound_mail()

    def test_strips_on_wrote_block_from_text(self):
        """pop_down_quoted_replies on text content strips everything from '\\nOn ... wrote:' onwards."""
        text = "Reply body\nOn Mon, Jan 1, John <j@x.com> wrote:\n> Old content"
        result = self.mail.pop_down_quoted_replies(text, "text")
        self.assertEqual(result, "Reply body")

    def test_leaves_text_unchanged_when_no_quote_marker(self):
        """pop_down_quoted_replies returns the input unchanged when there is no quote marker."""
        text = "Just a reply body."
        result = self.mail.pop_down_quoted_replies(text, "text")
        self.assertEqual(result, text)


class TestPopDownQuotedRepliesHtml(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mail = _build_gmail_inbound_mail()

    def test_rewraps_apple_blockquote_cite_as_gmail_quote(self):
        """An Apple Mail blockquote[type='cite'] is rewrapped as <div class='gmail_quote' style='...'>."""
        html = '<p>Reply</p><blockquote type="cite">Old reply</blockquote>'
        result = self.mail.pop_down_quoted_replies(html, "html")
        self.assertIn('class="gmail_quote"', result)
        self.assertIn("border-left:1px solid", result)
        self.assertNotIn('type="cite"', result)
        self.assertIn("Old reply", result)

    def test_rewraps_outlook_div_rply_fwd_msg(self):
        """An Outlook div#divRplyFwdMsg is rewrapped as a gmail_quote div."""
        html = "<p>Reply</p><div id='divRplyFwdMsg'>Old content</div>"
        result = self.mail.pop_down_quoted_replies(html, "html")
        self.assertIn('class="gmail_quote"', result)
        self.assertIn("Old content", result)

    def test_rewraps_yahoo_quoted_div(self):
        """A Yahoo div.yahoo_quoted is rewrapped as a gmail_quote div."""
        html = '<p>Reply</p><div class="yahoo_quoted">Old content</div>'
        result = self.mail.pop_down_quoted_replies(html, "html")
        self.assertIn('class="gmail_quote"', result)
        self.assertIn("Old content", result)


class TestSetToAndCc(IntegrationTestCase):
    def test_populates_to_cc_bcc_from_headers(self):
        """GmailInboundMail.set_to_and_cc populates .to/.cc/.bcc by parsing the corresponding headers via extract_email_id."""
        mail = _build_gmail_inbound_mail(
            recipient="a@x.com, b@y.com",
            cc="c@x.com",
            bcc="d@x.com",
        )
        self.assertEqual(mail.to, ["a@x.com", "b@y.com"])
        self.assertEqual(mail.cc, ["c@x.com"])
        self.assertEqual(mail.bcc, ["d@x.com"])

    def test_returns_empty_list_when_header_missing(self):
        """When Cc/Bcc headers are absent, the corresponding fields are empty lists."""
        mail = _build_gmail_inbound_mail()
        self.assertEqual(mail.cc, [])
        self.assertEqual(mail.bcc, [])


class TestFindGmailThread(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            cls.account = make_test_gmail_account(linked_user=TEST_USER)

    def test_returns_thread_when_gmail_thread_id_matches(self):
        """find_gmail_thread(thread_id) returns the Gmail Thread with that gmail_thread_id."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="find-by-thread-id",
        )
        result = find_gmail_thread("find-by-thread-id")
        self.assertEqual(result.name, thread.name)

    def test_walks_message_ids_when_thread_id_missing(self):
        """When no thread matches the thread_id, find_gmail_thread walks the message_ids and returns the parent thread of any matching Single Email CT."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="find-by-message-1",
            emails=[{"gmail_message_id": "mid-001"}],
        )
        # email_message_id is read_only so the parent-doc-constructor pathway drops it;
        # set it directly via db.set_value after insert.
        frappe.db.set_value(
            "Single Email CT", "mid-001", "email_message_id", "<msg-001@x.com>"
        )
        result = find_gmail_thread("no-such-thread", ["<msg-001@x.com>"])
        self.assertEqual(result.name, thread.name)

    def test_returns_none_when_no_match(self):
        """find_gmail_thread returns None when neither the thread_id nor any message_id matches."""
        result = find_gmail_thread("nonexistent", ["no-such@msg.com"])
        self.assertIsNone(result)


class TestCreateNewEmail(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            cls.account = make_test_gmail_account(linked_user=TEST_USER)

    def test_raises_already_exists_when_email_message_id_in_db(self):
        """create_new_email raises AlreadyExistsError when a Single Email CT row already has the same email_message_id."""
        make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="dup-msg-thread",
            emails=[{"gmail_message_id": "mid-dup"}],
        )
        frappe.db.set_value(
            "Single Email CT", "mid-dup", "email_message_id", "dup-msg-id@x.com"
        )
        email = _build_email_dict(msg_id="m-dup", message_id="<dup-msg-id@x.com>")
        with self.assertRaises(AlreadyExistsError):
            create_new_email(email, self.account)

    def test_sets_sent_or_received_to_sent_for_known_system_user_sender(self):
        """create_new_email sets sent_or_received='Sent' when the sender's email matches a non-Website User in Frappe."""
        sender = "test_fgt_sender_sysuser@example.com"
        make_test_user(sender)
        frappe.db.set_value("User", sender, "user_type", "System User")
        email = _build_email_dict(
            msg_id="m-sent", message_id="<sent-1@x.com>", sender=sender
        )
        new_email, _ = create_new_email(email, self.account)
        self.assertEqual(new_email.sent_or_received, "Sent")

    def test_sets_sent_or_received_to_received_for_unknown_sender(self):
        """create_new_email sets sent_or_received='Received' when the sender's email does not match a non-Website User."""
        email = _build_email_dict(
            msg_id="m-recv",
            message_id="<recv-1@x.com>",
            sender="stranger@nobody.com",
        )
        new_email, _ = create_new_email(email, self.account)
        self.assertEqual(new_email.sent_or_received, "Received")

    def test_appends_linked_user_to_existing_thread_involved_users(self):
        """When AlreadyExistsError fires, create_new_email first appends the gmail_account's linked_user to the existing thread's involved_users if absent."""
        make_test_user(TEST_USER_2)
        with as_user(TEST_USER_2):
            other_account = make_test_gmail_account(linked_user=TEST_USER_2)
        thread = make_test_gmail_thread(
            gmail_account=other_account.name,
            gmail_thread_id="append-iu-thread",
            involved_users=[TEST_USER_2],
            emails=[{"gmail_message_id": "mid-append"}],
        )
        frappe.db.set_value(
            "Single Email CT", "mid-append", "email_message_id", "append-1@x.com"
        )
        email = _build_email_dict(msg_id="m-append", message_id="<append-1@x.com>")
        try:
            create_new_email(email, self.account)
        except AlreadyExistsError:
            pass
        refreshed = frappe.get_doc("Gmail Thread", thread.name)
        accounts = [u.account for u in refreshed.involved_users]
        self.assertIn(TEST_USER, accounts)

    def test_adding_linked_user_keeps_thread_modified(self):
        """Finding an already imported email adds the linked user without moving the thread's modified time."""
        make_test_user(TEST_USER_2)
        with as_user(TEST_USER_2):
            other_account = make_test_gmail_account(linked_user=TEST_USER_2)
        thread = make_test_gmail_thread(
            gmail_account=other_account.name,
            gmail_thread_id="keep-modified-thread",
            involved_users=[TEST_USER_2],
            emails=[{"gmail_message_id": "mid-keep-modified"}],
        )
        frappe.db.set_value(
            "Single Email CT",
            "mid-keep-modified",
            "email_message_id",
            "keep-modified@x.com",
        )
        last_email_at = get_datetime("2026-01-01 10:00:00")
        frappe.db.set_value(
            "Gmail Thread",
            thread.name,
            "modified",
            last_email_at,
            update_modified=False,
        )
        email = _build_email_dict(
            msg_id="m-keep-modified", message_id="<keep-modified@x.com>"
        )
        email["threadId"] = "keep-modified-this-mailbox"
        with self.assertRaises(AlreadyExistsError):
            create_new_email(email, self.account)
        involved = frappe.get_all(
            "Involved User", filters={"parent": thread.name}, pluck="account"
        )
        self.assertIn(TEST_USER, involved)
        self.assertEqual(
            frappe.db.get_value("Gmail Thread", thread.name, "modified"), last_email_at
        )
        # this mailbox's later emails in its thread find the same Gmail Thread
        self.assertEqual(
            find_gmail_thread("keep-modified-this-mailbox").name, thread.name
        )

    def test_records_mailbox_thread_id_when_user_already_involved(self):
        """The mailbox's Gmail thread id is recorded on the existing thread even if its user is already listed."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="known-user-thread",
            involved_users=[TEST_USER],
            emails=[{"gmail_message_id": "mid-known-user"}],
        )
        frappe.db.set_value(
            "Single Email CT", "mid-known-user", "email_message_id", "known-user@x.com"
        )
        email = _build_email_dict(
            msg_id="m-known-user", message_id="<known-user@x.com>"
        )
        email["threadId"] = "known-user-other-thread-id"
        with self.assertRaises(AlreadyExistsError):
            create_new_email(email, self.account)
        self.assertEqual(
            find_gmail_thread("known-user-other-thread-id").name, thread.name
        )


class TestLockGmailThread(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            cls.account = make_test_gmail_account(linked_user=TEST_USER)

    def _lock(self, pending_writes):
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id=f"lock-{frappe.generate_hash(length=8)}",
        )
        with (
            patch.object(frappe.db, "transaction_writes", pending_writes),
            patch.object(frappe.db, "commit") as mock_commit,
        ):
            locked = lock_gmail_thread(thread.name)
        self.assertEqual(locked.name, thread.name)
        return mock_commit

    def test_locks_from_a_new_transaction_when_nothing_is_pending(self):
        """With nothing to keep, the lock is taken in a new transaction, so no older snapshot can conflict with it."""
        self._lock(pending_writes=0).assert_called_once()

    def test_keeps_pending_writes_in_the_transaction(self):
        """Pending writes aren't committed early by taking the lock."""
        self._lock(pending_writes=3).assert_not_called()


class TestProcessAttachments(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            cls.account = make_test_gmail_account(linked_user=TEST_USER)

    def test_creates_private_file_per_attachment_and_records_data(self):
        """process_attachments creates a private File record for each attachment and stores file_name + file_doc_name in new_email.attachments_data."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="attach-thread-1",
        )
        new_email = frappe.new_doc("Single Email CT")
        new_email.gmail_message_id = "mid-attach-1"
        email_object = MagicMock()
        email_object.attachments = [
            {"fname": "doc.txt", "fcontent": b"content-1"},
            {"fname": "image.png", "fcontent": b"content-2"},
        ]
        email_object.cid_map = {}
        process_attachments(new_email, thread, email_object)
        data = json.loads(new_email.attachments_data)
        self.assertEqual(len(data), 2)
        for row in data:
            self.assertTrue(row["is_private"])
            self.assertTrue(frappe.db.exists("File", row["file_doc_name"]))

    def test_renames_filenames_longer_than_140_chars_to_uuid(self):
        """File names >= 140 chars are renamed to <uuid>.<ext> before saving."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="attach-thread-long",
        )
        new_email = frappe.new_doc("Single Email CT")
        new_email.gmail_message_id = "mid-attach-long"
        long_name = "a" * 141 + ".bin"
        email_object = MagicMock()
        email_object.attachments = [
            {"fname": long_name, "fcontent": b"content"},
        ]
        email_object.cid_map = {}
        process_attachments(new_email, thread, email_object)
        data = json.loads(new_email.attachments_data)
        saved_name = data[0]["file_name"]
        self.assertNotEqual(saved_name, long_name)
        self.assertTrue(saved_name.endswith(".bin"))
        self.assertLess(len(saved_name), 140)


class TestProcessAttachmentsAsMailboxUser(IntegrationTestCase):
    """The sync runs as the mailbox's user, not as Administrator."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)
        frappe.db.set_value("User", TEST_USER, "user_type", "System User")
        frappe.clear_cache(user=TEST_USER)
        with as_user(TEST_USER):
            cls.account = make_test_gmail_account(linked_user=TEST_USER)

    def _attach(self, thread):
        new_email = frappe.new_doc("Single Email CT")
        email_object = MagicMock()
        email_object.attachments = [{"fname": "quote.txt", "fcontent": b"quote"}]
        email_object.cid_map = {}
        with as_user(TEST_USER):
            process_attachments(new_email, thread, email_object)
        return json.loads(new_email.attachments_data)

    def test_saves_attachments_of_a_thread_not_created_yet(self):
        """The first email of a new thread keeps its attachments, though the thread isn't saved yet."""
        thread = frappe.new_doc("Gmail Thread")
        thread.gmail_thread_id = "attach-new-thread"
        thread.gmail_account = self.account.name
        data = self._attach(thread)
        self.assertEqual([row["file_name"] for row in data], ["quote.txt"])
        self.assertEqual(
            frappe.db.get_value("File", data[0]["file_doc_name"], "attached_to_name"),
            "attach-new-thread",
        )

    def test_saves_attachments_on_a_thread_the_user_is_not_part_of_yet(self):
        """An email that brings this mailbox into an existing thread keeps its attachments."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="attach-other-mailbox-thread",
        )
        data = self._attach(thread)
        self.assertEqual([row["file_name"] for row in data], ["quote.txt"])


class TestReplaceInlineImages(IntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        make_test_user(TEST_USER)
        with as_user(TEST_USER):
            cls.account = make_test_gmail_account(linked_user=TEST_USER)

    def test_substitutes_cid_refs_with_file_unique_url(self):
        """replace_inline_images rewrites every 'cid:<content-id>' in new_email.content to the matching File's unique_url."""
        thread = make_test_gmail_thread(
            gmail_account=self.account.name,
            gmail_thread_id="inline-thread",
        )
        file_doc = frappe.get_doc(
            {
                "doctype": "File",
                "file_name": "inline.png",
                "content": b"binary",
                "is_private": 1,
                "attached_to_doctype": "Gmail Thread",
                "attached_to_name": thread.name,
            }
        ).insert(ignore_permissions=True)
        new_email = frappe.new_doc("Single Email CT")
        new_email.gmail_message_id = "mid-inline"
        new_email.content = '<img src="cid:abc-123" />'
        new_email.attachments_data = json.dumps([{"file_doc_name": file_doc.name}])
        email_object = MagicMock()
        email_object.content = new_email.content
        email_object.cid_map = {file_doc.name: "abc-123"}
        email_object.replace_inline_images = (
            GmailInboundMail.replace_inline_images.__get__(email_object)
        )
        replace_inline_images(new_email, email_object)
        self.assertNotIn("cid:abc-123", new_email.content)
        self.assertIn(file_doc.unique_url, new_email.content)
