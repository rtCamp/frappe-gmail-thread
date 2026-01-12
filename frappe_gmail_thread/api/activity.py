import json

import frappe
import frappe.utils


def get_attachments_data_batch(emails):
    """Batch fetch attachment URLs for all emails at once."""
    all_file_names = []
    email_attachments_map = {}

    for email in emails:
        attachments_data = json.loads(email.attachments_data or "[]")
        email_attachments_map[email.name] = attachments_data
        for att in attachments_data:
            if att.get("file_doc_name"):
                all_file_names.append(att["file_doc_name"])

    # Single query for all file URLs
    if all_file_names:
        file_urls = frappe.get_all(
            "File",
            filters={"name": ["in", all_file_names]},
            fields=["name", "file_url"],
        )
        file_url_map = {f.name: f.file_url for f in file_urls}

        # Update attachments with URLs
        for email_name, attachments in email_attachments_map.items():
            for att in attachments:
                file_name = att.get("file_doc_name")
                if file_name and file_name in file_url_map:
                    att["file_url"] = file_url_map[file_name]

    return email_attachments_map


@frappe.whitelist()
def get_linked_gmail_threads(doctype, docname):
    # Fetch threads with needed fields in single query
    gmail_threads = frappe.get_all(
        "Gmail Thread",
        filters={
            "reference_doctype": doctype,
            "reference_name": docname,
        },
        fields=["name", "reference_doctype", "reference_name", "_liked_by"],
    )

    if not gmail_threads:
        return []

    thread_names = [t.name for t in gmail_threads]
    thread_map = {t.name: t for t in gmail_threads}

    # Batch fetch all emails for all threads
    all_emails = frappe.get_all(
        "Single Email CT",  # Child table DocType
        filters={"parent": ["in", thread_names]},
        fields=[
            "name",
            "parent",
            "creation",
            "subject",
            "content",
            "sender",
            "sender_full_name",
            "cc",
            "bcc",
            "recipients",
            "sent_or_received",
            "read_by_recipient",
            "date_and_time",
            "attachments_data",
        ],
        order_by="creation asc",
    )

    # Batch fetch all attachment URLs
    attachments_map = get_attachments_data_batch(all_emails)

    data = []
    for email in all_emails:
        thread = thread_map[email.parent]
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
                    "attachments": attachments_map.get(email.name, []),
                    "_url": f"/app/gmail-thread/{thread.name}",
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


@frappe.whitelist()
def relink_gmail_thread(name, doctype, docname):
    thread = frappe.get_doc("Gmail Thread", name)
    thread.reference_doctype = doctype
    thread.reference_name = docname
    thread.save()
    return thread.reference_name


@frappe.whitelist()
def unlink_gmail_thread(name):
    thread = frappe.get_doc("Gmail Thread", name)
    thread.reference_doctype = None
    thread.reference_name = None
    thread.save()
    return thread.reference_name
