"""Email the owner of a Gmail Account whose authorization expired, until it is authorized again."""

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils import (
    cint,
    date_diff,
    format_date,
    get_url_to_form,
    now_datetime,
    today,
)
from frappe.utils.background_jobs import is_job_enqueued

REMINDER_DAYS_FIELD = "custom_gmail_reauthorization_reminder_days"
DEFAULT_REMINDER_DAYS = 3
EMAIL_TEMPLATE_FIELD = "custom_gmail_reauthorization_email_template"
# also the content of the default Email Template
BUILT_IN_TEMPLATE = "templates/emails/gmail_authorization_expired.html"


def get_reminder_days() -> int:
    """Days between re-authorization reminder emails; 0 when reminders are off."""
    return cint(frappe.get_cached_doc("Google Settings").get(REMINDER_DAYS_FIELD))


def get_built_in_email_template() -> Document:
    """The built-in email, unsaved: what the default Email Template starts as."""
    return frappe.get_doc(
        {
            "doctype": "Email Template",
            "name": _("Gmail Authorization Expired"),
            "subject": "{% if is_reminder %}"
            + _("Reminder:")
            + " {% endif %}"
            + _("Gmail sync has stopped"),
            "reference_doctype": "Gmail Account",
            "use_html": 1,
            "response_html": frappe.read_file(
                frappe.get_app_path("frappe_gmail_thread", BUILT_IN_TEMPLATE)
            ),
        }
    )


def make_default_email_template() -> str:
    """Create the editable default Email Template, unless one by its name exists; return its name."""
    email_template = get_built_in_email_template()
    if not frappe.db.exists("Email Template", email_template.name):
        email_template.insert(ignore_permissions=True)
    return email_template.name


def render_reauthorization_email(context: dict) -> tuple[str, str]:
    """Subject and message from the Email Template set in Google Settings, else the built-in email."""
    template_name = frappe.get_cached_doc("Google Settings").get(EMAIL_TEMPLATE_FIELD)
    if template_name:
        try:
            return _render(
                frappe.get_cached_doc("Email Template", template_name), context
            )
        except Exception:
            # a deleted or broken template must not keep the owner from being told
            frappe.log_error(
                title="Gmail Thread: re-authorization Email Template failed",
                reference_doctype="Email Template",
                reference_name=template_name,
            )
    return _render(get_built_in_email_template(), context)


def _render(email_template: Document, context: dict) -> tuple[str, str]:
    return (
        email_template.get_formatted_subject(context),
        email_template.get_formatted_response(context),
    )


def enqueue_reauthorization_email(account: str) -> None:
    job_id = f"gmail_reauthorization_email_{account}"
    if is_job_enqueued(job_id):
        return
    # its own job: a failing send can neither undo nor repeat the expiry handling
    frappe.enqueue(
        "frappe_gmail_thread.utils.reauthorization.send_reauthorization_email",
        queue="short",
        job_id=job_id,
        enqueue_after_commit=True,
        account=account,
    )


def send_reauthorization_email(account: str) -> None:
    """Ask the owner of a Gmail Account whose authorization expired to authorize Gmail again."""
    gmail_account = frappe.db.get_value("Gmail Account", account, "*", as_dict=True)
    # re-authorized, unticked or deleted since the email was queued
    if (
        not gmail_account
        or not gmail_account.gmail_enabled
        or gmail_account.refresh_token
        or not gmail_account.authorization_expired_on
    ):
        return
    user = frappe.db.get_value(
        "User",
        gmail_account.linked_user,
        ["email", "first_name", "full_name", "enabled"],
        as_dict=True,
    )
    if not user or not user.enabled:
        return

    subject, message = render_reauthorization_email(
        {
            **gmail_account,
            "first_name": user.first_name or user.email,
            "full_name": user.full_name or user.email,
            "email_id": gmail_account.email_id or user.email,
            "expired_on": format_date(gmail_account.authorization_expired_on),
            "link": get_url_to_form("Gmail Account", account),
            "reminder_days": get_reminder_days(),
            "is_reminder": bool(gmail_account.reauthorization_email_sent_on),
        }
    )
    frappe.sendmail(
        recipients=[user.email],
        subject=subject,
        message=message,
        with_container=True,
        reference_doctype="Gmail Account",
        reference_name=account,
    )
    frappe.db.set_value(
        "Gmail Account",
        account,
        "reauthorization_email_sent_on",
        now_datetime(),
        update_modified=False,
    )


def is_reminder_due(
    authorization_expired_on, reauthorization_email_sent_on, reminder_days: int
) -> bool:
    """Whether `reminder_days` have passed since the last email, or since the expiry if none went out."""
    if reminder_days <= 0:
        return False
    last_email_on = reauthorization_email_sent_on or authorization_expired_on
    # by date: the daily job runs early in the day, an email can go out at any hour
    return date_diff(today(), last_email_on) >= reminder_days
