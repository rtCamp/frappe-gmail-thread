import frappe
from frappe.utils.background_jobs import is_job_enqueued


def sync_emails():
    gmail_accounts = frappe.get_all(
        "Gmail Account",
        filters={"gmail_enabled": 1},
        fields=["name"],
    )
    for gmail_account in gmail_accounts:
        gaccount = frappe.get_doc("Gmail Account", gmail_account.name)
        if gaccount.refresh_token:
            user = gaccount.linked_user
            if not frappe.get_value("User", user, "enabled"):
                continue
            job_name = f"gmail_thread_sync_{user}"
            if not is_job_enqueued(job_name):
                frappe.enqueue(
                    "frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread.sync",
                    user=user,
                    queue="long",
                    job_name=job_name,
                    job_id=job_name,
                )
