import json

import frappe
import frappe.utils


def get_attachments_data(email):
    attachments_data = json.loads(email.attachments_data)
    # instead of using file_url from attachments_data, we have to use frappe.get_value to get the latest file_url
    for attachment in attachments_data:
        file_doc_name = attachment.get("file_doc_name")
        if file_doc_name:
            file_url = frappe.db.get_value("File", file_doc_name, "file_url")
            attachment["file_url"] = file_url
    return attachments_data


@frappe.whitelist()
def get_linked_gmail_threads(doctype, docname):
    gmail_threads = frappe.get_all(
        "Gmail Thread",
        filters={
            "reference_doctype": doctype,
            "reference_name": docname,
        },
    )
    data = []
    for thread in gmail_threads:
        thread = frappe.get_doc("Gmail Thread", thread.name)
        for email in thread.emails:
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
                        "attachments": get_attachments_data(email),
                        "_url": thread.get_url(),
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
