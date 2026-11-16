import base64 as b64
import hmac
import json

import frappe
from frappe import _
from frappe.utils.background_jobs import is_job_enqueued

from frappe_gmail_thread.utils.queues import get_gmail_thread_sync_queue_name
from frappe_gmail_thread.utils.sync_pause import is_sync_paused


def verify_push_token(google_settings):
    """Refuse a push unless its ?token matches the PubSub Token in Google Settings.

    A Pub/Sub push carries no identity of its own, so the subscription's push endpoint
    must be .../api/method/frappe_gmail_thread.api.pubsub.callback?token=<PubSub Token>.
    When no token is configured the check is skipped, so a site that upgrades keeps
    working as before; set the token (and the subscription's ?token) to turn
    verification on. Without it, anyone can POST a forged notification and trigger a
    Gmail sync for any System User's mailbox.
    """
    expected = (
        google_settings.get_password("custom_gmail_pubsub_token", raise_exception=False)
        or ""
    )
    if not expected:
        return
    supplied = frappe.form_dict.get("token") or ""
    if not hmac.compare_digest(expected.encode(), supplied.encode()):
        frappe.throw(_("Invalid Pub/Sub token"), frappe.AuthenticationError)


@frappe.whitelist(allow_guest=True, methods=["POST"])  # nosemgrep
def callback():
    data = frappe.request.get_data(as_text=True)
    data = frappe.parse_json(data)
    message = data.get("message", {}).get("data")
    google_settings = frappe.get_single("Google Settings")
    if not google_settings.enable:
        return "OK"
    if not google_settings.custom_gmail_sync_in_realtime:
        return "OK"
    if not google_settings.custom_gmail_pubsub_topic:
        return "OK"
    verify_push_token(google_settings)
    if message:
        message = b64.b64decode(message).decode("utf-8")
        try:
            message = json.loads(message)
        except json.JSONDecodeError:
            frappe.log_error(f"Error while decoding message: {message}")
            return "OK"
        email_address = message.get("emailAddress")
        if not email_address:
            return "OK"
        # unknown or disabled mailbox: acknowledge, so Pub/Sub doesn't redeliver
        user = frappe.db.get_value(
            "User", {"email": email_address, "user_type": "System User", "enabled": 1}
        )
        if not user:
            return "OK"
        if is_sync_paused(user):
            return "OK"
        history_id = message.get("historyId")
        gmail_account = frappe.db.get_value(
            "Gmail Account",
            {"linked_user": user},
            ["gmail_enabled", "refresh_token"],
            as_dict=True,
        )
        if (
            history_id
            and gmail_account
            and gmail_account.gmail_enabled
            and gmail_account.refresh_token
        ):
            job_name = f"gmail_thread_sync_{user}"
            if not is_job_enqueued(job_name):
                frappe.enqueue(
                    "frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread.sync",
                    user=user,
                    queue=get_gmail_thread_sync_queue_name(),
                    job_name=job_name,
                    job_id=job_name,
                )
    return "OK"
