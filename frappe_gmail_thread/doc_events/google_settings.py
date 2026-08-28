import frappe

from frappe_gmail_thread.api.oauth import disable_pubsub, enable_pubsub


def on_update(doc, method=None):
    if doc.has_value_changed("custom_gmail_sync_in_realtime"):
        gmail_accounts = frappe.get_all(
            "Gmail Account",
            fields=["name"],
        )
        for gmail_account in gmail_accounts:
            gdoc = frappe.get_doc("Gmail Account", gmail_account.name)
            if doc.custom_gmail_sync_in_realtime:
                # start pubsub
                enable_pubsub(gdoc)
            else:
                # stop pubsub
                disable_pubsub(gdoc)
