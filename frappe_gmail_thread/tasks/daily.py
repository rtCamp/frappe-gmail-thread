import frappe

from frappe_gmail_thread.api.oauth import enable_pubsub


def enable_pubsub_everyday():
    google_settings = frappe.get_single("Google Settings")
    if not google_settings.enable:
        return
    if (
        not google_settings.custom_gmail_sync_in_realtime
        or not google_settings.custom_gmail_pubsub_topic
    ):
        return

    gmail_accounts = frappe.get_all(
        "Gmail Account", filters={"gmail_enabled": 1}, fields=["name", "linked_user"]
    )
    for gmail_account in gmail_accounts:
        if not frappe.get_value("User", gmail_account.linked_user, "enabled"):
            continue
        gaccount = frappe.get_doc("Gmail Account", gmail_account.name)
        try:
            enable_pubsub(gaccount)
        except Exception as e:
            frappe.log_error(title="PubSub Error", message=e)
            continue
