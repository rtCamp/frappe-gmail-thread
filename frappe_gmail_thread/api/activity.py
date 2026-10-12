from collections import defaultdict
from collections.abc import Iterable

import frappe
import frappe.utils
from frappe import _
from frappe.utils.data import get_absolute_url


def parse_attachments_data(email) -> list:
    raw_attachments = getattr(email, "attachments_data", None)
    if not raw_attachments:
        return []

    if isinstance(raw_attachments, str):
        try:
            attachments_data = frappe.parse_json(raw_attachments)
        except (TypeError, ValueError, frappe.ValidationError):
            frappe.log_error(
                title=_("Invalid Gmail attachments_data JSON"),
                message=frappe.get_traceback(),
            )
            return []
    elif isinstance(raw_attachments, list):
        attachments_data = raw_attachments
    else:
        return []

    if not attachments_data or not isinstance(attachments_data, list):
        return []
    return attachments_data


def get_file_urls(attachments_lists: Iterable[list]) -> dict[str, str]:
    """Map the File names referenced by the given attachment lists to their current URL, in one query."""
    file_doc_names = {
        a.get("file_doc_name")
        for attachments_data in attachments_lists
        for a in attachments_data
        if isinstance(a, dict) and a.get("file_doc_name")
    }
    if not file_doc_names:
        return {}
    return {
        r["name"]: r["file_url"]
        for r in frappe.get_list(
            "File",
            filters={"name": ("in", list(file_doc_names))},
            fields=["name", "file_url"],
        )
    }


def resolve_attachments(
    attachments_data: list, file_url_by_name: dict[str, str]
) -> list[dict]:
    valid: list[dict] = []
    for attachment in attachments_data:
        if not isinstance(attachment, dict):
            continue
        file_doc_name = attachment.get("file_doc_name")
        if file_doc_name:
            file_url = file_url_by_name.get(file_doc_name)
            if not file_url:
                continue
            attachment["file_url"] = file_url
        if attachment.get("file_url"):
            valid.append(attachment)
    return valid


def get_attachments_data(email) -> list[dict]:
    attachments_data = parse_attachments_data(email)
    return resolve_attachments(attachments_data, get_file_urls([attachments_data]))


def get_linked_gmail_threads(doctype: str, docname: str | int) -> list[dict]:
    frappe.has_permission(doctype, "read", doc=docname, throw=True)
    # A thread linked to a document the user can read is readable through the
    # Gmail Thread has_permission hook, so the list query's permission check is
    # enough and the threads aren't loaded one by one
    gmail_threads = frappe.get_list(
        "Gmail Thread",
        filters={
            "reference_doctype": doctype,
            "reference_name": docname,
        },
        fields=["name", "_liked_by", "reference_doctype", "reference_name"],
    )
    if not gmail_threads:
        return []

    emails_by_thread = defaultdict(list)
    for email in frappe.get_all(
        "Single Email CT",
        filters={
            "parenttype": "Gmail Thread",
            "parentfield": "emails",
            "parent": ("in", [thread.name for thread in gmail_threads]),
        },
        fields=[
            "name",
            "parent",
            "creation",
            "content",
            "sender",
            "sender_full_name",
            "cc",
            "bcc",
            "subject",
            "sent_or_received",
            "read_by_recipient",
            "recipients",
            "attachments_data",
        ],
        order_by="idx asc",
    ):
        emails_by_thread[email.parent].append(email)

    attachments_by_email = {
        email.name: parse_attachments_data(email)
        for emails in emails_by_thread.values()
        for email in emails
    }
    file_url_by_name = get_file_urls(attachments_by_email.values())

    data = []
    for thread in gmail_threads:
        thread_url = get_absolute_url("Gmail Thread", thread.name)
        for email in emails_by_thread[thread.name]:
            t_data = {
                "icon": "mail",
                "icon_size": "sm",
                "creation": email.creation,
                "is_card": True,
                "doctype": "Gmail Thread",
                "id": f"gmail-thread-{thread.name}",
                "template": "timeline_message_box",
                "owner": email.sender,
                "template_data": {
                    "doc": {
                        "name": thread.name,
                        "communication_type": "Gmail Thread",
                        "communication_medium": "Email",
                        "comment_type": "",
                        "communication_date": email.creation,
                        "content": email.content,
                        "sender": email.sender,
                        "sender_full_name": email.sender_full_name,
                        "cc": email.cc,
                        "bcc": email.bcc,
                        "creation": email.creation,
                        "subject": email.subject,
                        "delivery_status": (
                            "Sent" if email.sent_or_received == "Sent" else "Received"
                        ),
                        "_liked_by": thread._liked_by,
                        "reference_doctype": thread.reference_doctype,
                        "reference_name": thread.reference_name,
                        "read_by_recipient": email.read_by_recipient,
                        "rating": 0,  # TODO: add rating
                        "recipients": email.recipients,
                        "attachments": resolve_attachments(
                            attachments_by_email[email.name], file_url_by_name
                        ),
                        "_url": thread_url,
                        "_doc_status": (
                            "Sent" if email.sent_or_received == "Sent" else "Received"
                        ),
                        "_doc_status_indicator": (
                            "green" if email.sent_or_received == "Sent" else "blue"
                        ),
                        "owner": email.sender,
                        "user_full_name": email.sender_full_name,
                    }
                },
                "name": thread.name,
                "delivery_status": (
                    "Sent" if email.sent_or_received == "Sent" else "Received"
                ),
            }
            data.append(t_data)

    return data


@frappe.whitelist(methods=["POST"])
def relink_gmail_thread(name: str | int, doctype: str, docname: str | int):
    # the link decides who may read the thread, so the caller must hold the
    # target's write permission, not merely the thread's
    frappe.has_permission(doctype, "write", doc=docname, throw=True)
    thread = frappe.get_doc("Gmail Thread", name, check_permission="write")
    thread.reference_doctype = doctype
    thread.reference_name = docname
    thread.save()
    return thread.reference_name


@frappe.whitelist(methods=["POST"])
def unlink_gmail_thread(name: str | int):
    thread = frappe.get_doc("Gmail Thread", name)
    thread.reference_doctype = None
    thread.reference_name = None
    thread.save()
    return thread.reference_name
