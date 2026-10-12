import frappe
from frappe.model.document import Document

from frappe_gmail_thread.api.oauth import disable_pubsub, enable_pubsub


def on_update(doc: Document, method: str | None = None) -> None:
    if not doc.has_value_changed("custom_gmail_sync_in_realtime"):
        return
    # a Gmail API round trip per mailbox: keep it out of the settings save
    frappe.enqueue(
        "frappe_gmail_thread.doc_events.google_settings.update_gmail_watches",
        queue="long",
        enqueue_after_commit=True,
    )


def update_gmail_watches() -> None:
    """Start or stop the Gmail watch of every authorized mailbox to match the real-time sync setting."""
    # read when the job runs, so the last of several quick toggles wins
    realtime = frappe.db.get_single_value(
        "Google Settings", "custom_gmail_sync_in_realtime"
    )
    gmail_accounts = frappe.get_all(
        "Gmail Account",
        filters={"gmail_enabled": 1, "refresh_token": ["is", "set"]},
        fields=["name", "linked_user"],
    )
    if not gmail_accounts:
        return
    if realtime:
        # same rule as the daily watch renewal: disabled users get no watch
        enabled_users = set(
            frappe.get_all(
                "User",
                filters={
                    "name": ["in", [a.linked_user for a in gmail_accounts]],
                    "enabled": 1,
                },
                pluck="name",
            )
        )
        gmail_accounts = [a for a in gmail_accounts if a.linked_user in enabled_users]

    for gmail_account in gmail_accounts:
        gdoc = frappe.get_doc("Gmail Account", gmail_account.name)
        if not realtime:
            # logs its own failures
            disable_pubsub(gdoc)
            continue
        try:
            enable_pubsub(gdoc)
        except Exception:
            # one unreachable mailbox must not stop the others
            frappe.log_error(
                title="Gmail Thread: failed to start Gmail watch",
                message=f"{gdoc.name}\n\n{frappe.get_traceback()}",
            )
