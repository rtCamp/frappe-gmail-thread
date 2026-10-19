from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase, UnitTestCase, change_settings
from frappe.utils import add_days, get_datetime, get_url_to_form, now_datetime

from frappe_gmail_thread.tests import (
    TEST_USER,
    as_user,
    make_test_gmail_account,
    make_test_user,
    set_password_field,
)
from frappe_gmail_thread.utils.reauthorization import (
    enqueue_reauthorization_email,
    get_built_in_email_template,
    is_reminder_due,
    make_default_email_template,
    send_reauthorization_email,
)

REAUTHORIZATION_MODULE = "frappe_gmail_thread.utils.reauthorization"


def _expired_account(*, email_sent_on=None):
    """The test user's Gmail Account, enabled, with an authorization that expired yesterday."""
    make_test_user(TEST_USER)
    with as_user(TEST_USER):
        account = make_test_gmail_account(linked_user=TEST_USER)
    set_password_field("Gmail Account", account.name, "refresh_token", "")
    frappe.db.set_value(
        "Gmail Account",
        account.name,
        {
            "gmail_enabled": 1,
            "authorization_expired_on": add_days(now_datetime(), -1),
            "reauthorization_email_sent_on": email_sent_on,
        },
        update_modified=False,
    )
    return account.name


def _email_template(subject, response_html):
    """An HTML Email Template with the given subject and body; return its name."""
    name = "Test Gmail Authorization Expired"
    if frappe.db.exists("Email Template", name):
        frappe.delete_doc("Email Template", name, force=True)
    return (
        frappe.get_doc(
            {
                "doctype": "Email Template",
                "name": name,
                "subject": subject,
                "use_html": 1,
                "response_html": response_html,
            }
        )
        .insert(ignore_permissions=True)
        .name
    )


def _email_sent_on(account):
    return frappe.db.get_value(
        "Gmail Account", account, "reauthorization_email_sent_on"
    )


class TestSendReauthorizationEmail(IntegrationTestCase):
    def _send(self, account, template=None):
        """Send the email for `account` with `template` linked in Google Settings; return the sendmail mock."""
        with (
            change_settings(
                "Google Settings",
                custom_gmail_reauthorization_reminder_days=3,
                custom_gmail_reauthorization_email_template=template,
            ),
            patch(f"{REAUTHORIZATION_MODULE}.frappe.sendmail") as mock_sendmail,
        ):
            send_reauthorization_email(account)
        return mock_sendmail

    def test_emails_owner_from_the_linked_template(self):
        """The email is rendered from the Email Template set in Google Settings, with the account's details."""
        account = _expired_account()
        template = _email_template(
            "Expired: {{ email_id }}",
            "<p>{{ first_name }}|{{ link }}|{{ reminder_days }}|{{ linked_user }}</p>",
        )

        mock_sendmail = self._send(account, template)

        mock_sendmail.assert_called_once()
        email = mock_sendmail.call_args.kwargs
        self.assertEqual(email["recipients"], [TEST_USER])
        self.assertEqual(email["subject"], f"Expired: {TEST_USER}")
        link = get_url_to_form("Gmail Account", account)
        self.assertEqual(
            email["message"], f"<p>{TEST_USER.split('@')[0]}|{link}|3|{TEST_USER}</p>"
        )
        self.assertEqual(
            (email["reference_doctype"], email["reference_name"]),
            ("Gmail Account", account),
        )
        self.assertIsNotNone(_email_sent_on(account))

    def test_default_template_explains_how_to_resume_sync(self):
        """The default Email Template names the mailbox, links to the account and states the reminder schedule."""
        account = _expired_account()

        mock_sendmail = self._send(account, make_default_email_template())

        email = mock_sendmail.call_args.kwargs
        self.assertEqual(email["subject"], "Gmail sync has stopped")
        for text in (
            TEST_USER,
            "Authorize Gmail",
            f'href="{get_url_to_form("Gmail Account", account)}"',
            "every 3 days",
        ):
            self.assertIn(text, email["message"])

    def test_later_email_is_a_reminder(self):
        """An email after the first one for the same expiry says it is a reminder and moves the send time on."""
        earlier = add_days(now_datetime(), -3)
        account = _expired_account(email_sent_on=earlier)

        mock_sendmail = self._send(account, make_default_email_template())

        email = mock_sendmail.call_args.kwargs
        self.assertEqual(email["subject"], "Reminder: Gmail sync has stopped")
        self.assertIn("still stopped", email["message"])
        self.assertGreater(get_datetime(_email_sent_on(account)), earlier)

    def test_built_in_email_without_a_template(self):
        """With no Email Template set, the built-in email still tells the owner."""
        account = _expired_account()

        mock_sendmail = self._send(account)

        email = mock_sendmail.call_args.kwargs
        self.assertEqual(email["subject"], "Gmail sync has stopped")
        self.assertIn(get_url_to_form("Gmail Account", account), email["message"])

    def test_broken_template_falls_back_to_built_in_email(self):
        """A template that fails to render is logged and the built-in email goes out instead."""
        account = _expired_account()
        template = _email_template("Expired", "{{ missing.value }}")

        with patch(f"{REAUTHORIZATION_MODULE}.frappe.log_error") as mock_log:
            mock_sendmail = self._send(account, template)

        mock_log.assert_called_once()
        self.assertEqual(mock_log.call_args.kwargs["reference_name"], template)
        self.assertEqual(
            mock_sendmail.call_args.kwargs["subject"], "Gmail sync has stopped"
        )
        self.assertIsNotNone(_email_sent_on(account))

    def test_reaches_owner_whose_email_differs_from_user_name(self):
        """The email goes to the owner's email address, which can differ from their user name."""
        account = _expired_account()
        frappe.db.set_value("User", TEST_USER, "email", "test_fgt_renamed@example.com")
        self.addCleanup(frappe.db.set_value, "User", TEST_USER, "email", TEST_USER)

        mock_sendmail = self._send(account)

        self.assertEqual(
            mock_sendmail.call_args.kwargs["recipients"],
            ["test_fgt_renamed@example.com"],
        )

    def test_skips_account_that_no_longer_needs_it(self):
        """No email once the account is authorized again, unticked or its owner disabled."""
        cases = {
            "authorized again": lambda name: set_password_field(
                "Gmail Account", name, "refresh_token", "new-rt"
            ),
            "expiry cleared": lambda name: frappe.db.set_value(
                "Gmail Account", name, "authorization_expired_on", None
            ),
            "unticked": lambda name: frappe.db.set_value(
                "Gmail Account", name, "gmail_enabled", 0
            ),
            "owner disabled": lambda name: frappe.db.set_value(
                "User", TEST_USER, "enabled", 0
            ),
        }
        self.addCleanup(frappe.db.set_value, "User", TEST_USER, "enabled", 1)
        for case, change in cases.items():
            with self.subTest(case):
                frappe.db.set_value("User", TEST_USER, "enabled", 1)
                account = _expired_account()
                change(account)

                mock_sendmail = self._send(account)

                mock_sendmail.assert_not_called()
                self.assertIsNone(_email_sent_on(account))


class TestEnqueueReauthorizationEmail(IntegrationTestCase):
    def test_sends_after_commit_once_per_account(self):
        """The email goes out by its own job after the expiry is committed, and isn't queued twice."""
        with (
            patch(
                f"{REAUTHORIZATION_MODULE}.is_job_enqueued", side_effect=[False, True]
            ),
            patch(f"{REAUTHORIZATION_MODULE}.frappe.enqueue") as mock_enqueue,
        ):
            enqueue_reauthorization_email("acc")
            enqueue_reauthorization_email("acc")

        mock_enqueue.assert_called_once()
        job = mock_enqueue.call_args
        self.assertEqual(
            job.args[0], f"{REAUTHORIZATION_MODULE}.send_reauthorization_email"
        )
        self.assertEqual(job.kwargs["account"], "acc")
        self.assertTrue(job.kwargs["enqueue_after_commit"])


class TestBuiltInEmail(IntegrationTestCase):
    def _render(self, reminder_days):
        return get_built_in_email_template().get_formatted_response(
            {
                "first_name": "Test",
                "email_id": "test@example.com",
                "expired_on": "01-10-2026",
                "link": "https://erp.example.com/app/gmail-account/test",
                "reminder_days": reminder_days,
                "is_reminder": False,
            }
        )

    def test_explains_how_to_resume_sync(self):
        """The email names the mailbox and expiry date, and links to the account to authorize Gmail again."""
        html = self._render(3)
        for text in (
            "test@example.com",
            "01-10-2026",
            "Authorize Gmail",
            'href="https://erp.example.com/app/gmail-account/test"',
        ):
            self.assertIn(text, html)

    def test_states_reminder_schedule(self):
        """The email states how often a reminder follows, and how to stop them, only while reminders are on."""
        self.assertIn("every 3 days", self._render(3))
        self.assertIn("every day", self._render(1))
        off = self._render(0)
        self.assertNotIn("reminder", off)
        self.assertNotIn("Gmail Enabled", off)


class TestIsReminderDue(UnitTestCase):
    def test_due_after_interval_since_last_email(self):
        """A reminder is due once the interval has passed, counted in days from the last email."""
        now = now_datetime()
        expired_on = add_days(now, -10)
        self.assertTrue(is_reminder_due(expired_on, add_days(now, -3), 3))
        self.assertFalse(is_reminder_due(expired_on, add_days(now, -2), 3))
        # an email late yesterday still counts as a day ago
        self.assertTrue(
            is_reminder_due(
                expired_on, add_days(now.replace(hour=23, minute=59), -1), 1
            )
        )

    def test_counts_from_expiry_when_no_email_went_out(self):
        """If the first email failed, a reminder follows one interval after the expiry."""
        now = now_datetime()
        self.assertTrue(is_reminder_due(add_days(now, -3), None, 3))
        self.assertFalse(is_reminder_due(add_days(now, -2), None, 3))

    def test_never_due_when_reminders_are_off(self):
        """An interval of 0 turns reminders off."""
        self.assertFalse(is_reminder_due(add_days(now_datetime(), -30), None, 0))
