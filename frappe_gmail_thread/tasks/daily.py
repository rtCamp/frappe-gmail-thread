import frappe

from frappe_gmail_thread.api.oauth import (
    GmailAuthorizationExpiredError,
    enable_pubsub,
    handle_expired_authorization,
)
from frappe_gmail_thread.utils.reauthorization import (
    enqueue_reauthorization_email,
    get_reminder_days,
    is_reminder_due,
)


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
        "Gmail Account",
        filters={"gmail_enabled": 1, "refresh_token": ["is", "set"]},
        fields=["name", "linked_user"],
    )
    for gmail_account in gmail_accounts:
        if not frappe.get_value("User", gmail_account.linked_user, "enabled"):
            continue
        gaccount = frappe.get_doc("Gmail Account", gmail_account.name)
        try:
            enable_pubsub(gaccount)
        except GmailAuthorizationExpiredError:
            handle_expired_authorization(gaccount)
            # release the account's row lock now: its owner may re-authorize while the other
            # mailboxes are processed
            frappe.db.commit()  # nosemgrep
        except Exception as e:
            frappe.log_error(title="PubSub Error", message=e)
            continue


def send_reauthorization_reminders() -> None:
    """Remind the owners of enabled Gmail Accounts whose authorization expired to authorize Gmail again."""
    if not frappe.db.get_single_value("Google Settings", "enable"):
        return
    reminder_days = get_reminder_days()
    if not reminder_days:
        return

    gmail_accounts = frappe.get_all(
        "Gmail Account",
        filters={
            "gmail_enabled": 1,
            "refresh_token": ["is", "not set"],
            "authorization_expired_on": ["is", "set"],
        },
        fields=["name", "authorization_expired_on", "reauthorization_email_sent_on"],
    )
    for gmail_account in gmail_accounts:
        if is_reminder_due(
            gmail_account.authorization_expired_on,
            gmail_account.reauthorization_email_sent_on,
            reminder_days,
        ):
            enqueue_reauthorization_email(gmail_account.name)
