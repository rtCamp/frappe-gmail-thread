import frappe
from frappe.utils.background_jobs import is_job_enqueued

from frappe_gmail_thread.utils.helpers import get_sync_queue


def sync_emails():
    gmail_accounts = frappe.get_all(
        "Gmail Account",
        filters={"gmail_enabled": 1},
        fields=["refresh_token", "linked_user"],
    )
    for gmail_account in gmail_accounts:
        if gmail_account.refresh_token:
            if not frappe.get_value("User", gmail_account.linked_user, "enabled"):
                continue
            job_name = f"gmail_thread_sync_{gmail_account.linked_user}"
            if not is_job_enqueued(job_name):
                frappe.enqueue(
                    "frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread.sync",
                    user=gmail_account.linked_user,
                    queue=get_sync_queue(),
                    job_name=job_name,
                    job_id=job_name,
                )
