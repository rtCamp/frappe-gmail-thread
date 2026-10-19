# Copyright (c) 2024, rtCamp and contributors
# For license information, please see license.txt


import json
from datetime import datetime, timezone
from email.utils import getaddresses, parsedate_to_datetime

import frappe
import frappe.share
import googleapiclient.errors
from frappe import _
from frappe.model.document import Document
from frappe.permissions import get_doctypes_with_read
from frappe.utils import cint, get_datetime, get_string_between

from frappe_gmail_thread.api.oauth import (
    GmailAuthorizationExpiredError,
    get_gmail_object,
    handle_expired_authorization,
)
from frappe_gmail_thread.utils.helpers import (
    AlreadyExistsError,
    add_thread_id_reference,
    add_thread_references,
    create_new_email,
    find_gmail_thread,
    lock_gmail_thread,
    process_attachments,
    replace_inline_images,
)
from frappe_gmail_thread.utils.sync_pause import (
    DEFAULT_PAUSE_SECONDS,
    count_fetch_failure,
    is_sync_paused,
    pause_sync,
)

SCOPES = "https://www.googleapis.com/auth/gmail.readonly"
# retried with exponential backoff by the client on 429, 5xx and 403 userRateLimitExceeded
# or rateLimitExceeded; other rate limits (e.g. dailyLimitExceeded) pause the mailbox at once
GMAIL_API_RETRIES = 3
DEFAULT_MAX_THREADS_PER_LABEL = 100
RATE_LIMIT_REASONS = {
    "rateLimitExceeded",
    "userRateLimitExceeded",
    "dailyLimitExceeded",
    "RATE_LIMIT_EXCEEDED",
}
# failed fetches of one message (a server error each time) before it is skipped
MAX_FETCH_ATTEMPTS = 5


class GmailThread(Document):
    def has_value_changed(self, fieldname):
        # check if fieldname is child table
        if fieldname in ["involved_users"]:
            old_value = self.get_doc_before_save()
            if old_value:
                old_value = old_value.get(fieldname)
            new_value = self.get(fieldname)
            if old_value and new_value:
                if len(old_value) != len(new_value):
                    return True
                old_names = [d.name for d in old_value]
                new_names = [d.name for d in new_value]
                if set(old_names) != set(new_names):
                    return True
                return False
            if not old_value and not new_value:
                return False
            return True
        return super().has_value_changed(fieldname)

    def before_save(self):
        if self.has_value_changed("involved_users"):
            # give permission of all files to all involved users
            attachments = frappe.get_all(
                "File",
                filters={
                    "attached_to_doctype": "Gmail Thread",
                    "attached_to_name": self.name,
                },
                fields=["name"],
            )
            for attachment in attachments:
                for user in self.involved_users:
                    if user.account == self.owner:
                        continue
                    frappe.share.add_docshare(
                        "File",
                        attachment.name,
                        user.account,
                        flags={"ignore_share_permission": True},
                    )
        if self.has_value_changed("reference_doctype") and self.has_value_changed(
            "reference_name"
        ):
            if self.reference_doctype and self.reference_name:
                if self.status == "Open":
                    self.status = "Linked"
                # check if there is any other thread with same reference doctype and name
                threads = frappe.get_all(
                    "Gmail Thread",
                    filters={
                        "reference_doctype": self.reference_doctype,
                        "reference_name": self.reference_name,
                    },
                    fields=["name"],
                )
                for thread in threads:
                    if thread.name != self.name:
                        frappe.msgprint(
                            _(
                                "The document is already linked with another Gmail Thread. This may cause confusion in the document timeline."
                            )
                        )
                        break
            elif self.status == "Linked":
                self.status = "Open"

        emails = self.emails or []
        if emails and emails[0].subject:
            self.subject_of_first_mail = emails[0].subject

        subjects = set()
        if self.subject_of_first_mail:
            subjects.add(self.subject_of_first_mail.strip())
        for email in self.emails or []:
            if email.subject:
                subjects.add(email.subject.strip())
        self.all_subjects = "\n".join(sorted(subjects))


@frappe.whitelist(methods=["POST"])
def sync_labels(account_name: str | Document, should_save: bool = True):
    if isinstance(account_name, str):
        gmail_account = frappe.get_doc(
            "Gmail Account", account_name, check_permission="write"
        )
    else:
        gmail_account = account_name

    gmail = get_gmail_object(gmail_account)
    labels = gmail.users().labels().list(userId="me").execute()

    gmail_label_ids = {label["id"] for label in labels["labels"]}
    deleted = [x for x in gmail_account.labels if x.label_id not in gmail_label_ids]
    # Only when the account is saved here: inside the account's own save (a new
    # authorization), removing rows would count as a label change and reset the sync.
    if deleted and should_save:
        # labels deleted in Gmail. Delete the rows directly too, so saving the
        # account below doesn't see a label change and reset the sync
        frappe.db.delete(
            "Gmail Label",
            {
                "parent": gmail_account.name,
                "label_id": ["in", [x.label_id for x in deleted]],
            },
        )
        for row in deleted:
            gmail_account.remove(row)

    available_labels = [x.label_id for x in gmail_account.labels]

    for label in labels["labels"]:
        if label["name"] in ["DRAFT", "CHAT"]:
            continue
        if label["id"] in available_labels:
            continue
        gmail_account.append(
            "labels", {"label_id": label["id"], "label_name": label["name"]}
        )
    if should_save:
        gmail_account.save()


def is_not_found_error(error: googleapiclient.errors.HttpError) -> bool:
    details = error.error_details if isinstance(error.error_details, list) else []
    return any(
        isinstance(detail, dict) and detail.get("reason") == "notFound"
        for detail in details
    )


def gmail_label_exists(gmail, label_id: str) -> bool:
    try:
        gmail.users().labels().get(userId="me", id=label_id).execute(
            num_retries=GMAIL_API_RETRIES
        )
    except googleapiclient.errors.HttpError as e:
        if is_not_found_error(e):
            return False
        raise
    return True


def is_rate_limit_error(error: Exception) -> bool:
    """Google throttled the mailbox: a 429, or a 403 for a rate or daily limit."""
    if not isinstance(error, googleapiclient.errors.HttpError):
        return False
    if error.status_code == 429:
        return True
    return error.status_code == 403 and bool(
        get_error_reasons(error) & RATE_LIMIT_REASONS
    )


def get_error_reasons(error: googleapiclient.errors.HttpError) -> set[str]:
    """The reasons in Google's error body, from both its `errors` and `details` lists."""
    try:
        body = json.loads(error.content)
    except (TypeError, ValueError):
        return set()
    error_body = body.get("error") if isinstance(body, dict) else None
    if not isinstance(error_body, dict):
        return set()
    return {
        item["reason"]
        for key in ("errors", "details")
        for item in error_body.get(key) or []
        if isinstance(item, dict) and item.get("reason")
    }


def skip_unavailable(
    error: googleapiclient.errors.HttpError, user: str, item_id: str
) -> bool:
    """Whether to skip a thread or message Gmail didn't return, rather than stop the run.

    One deleted since it was listed (404) is skipped. A rate limit stops the run, and so
    does a server error until the same item has failed MAX_FETCH_ATTEMPTS times. Other
    errors won't go away on a retry. A skipped item is logged, except a deleted one, so a
    single item can't hold up the mailbox.
    """
    if error.status_code == 404:
        return True
    if is_rate_limit_error(error):
        return False
    if (
        error.status_code >= 500
        and count_fetch_failure(user, item_id) < MAX_FETCH_ATTEMPTS
    ):
        return False
    frappe.log_error(
        title="Gmail Thread: skipped what Gmail didn't return",
        message=f"{user}: {item_id}\n\n{frappe.get_traceback()}",
    )
    return True


def is_temporary_error(error: Exception) -> bool:
    """Gmail can't serve the sync right now: a rate limit, or a server error left after the retries."""
    if is_rate_limit_error(error):
        return True
    return (
        isinstance(error, googleapiclient.errors.HttpError) and error.status_code >= 500
    )


def pause_on_temporary_error(error: Exception, user: str) -> bool:
    """Pause the mailbox's sync when Gmail can't serve it right now; False for any other error."""
    if not is_temporary_error(error):
        return False
    pause_sync(user, get_retry_after(error))
    frappe.logger("frappe_gmail_thread").warning(
        f"Gmail sync of {user} paused after HTTP {error.status_code}"
    )
    return True


def get_retry_after(error: googleapiclient.errors.HttpError) -> int:
    """Seconds to wait as asked by Google's Retry-After header, else a default."""
    value = str((error.resp or {}).get("retry-after") or "").strip()
    seconds = DEFAULT_PAUSE_SECONDS
    if value.isdigit():
        seconds = int(value)
    elif value:
        try:
            retry_at = parsedate_to_datetime(value)
            seconds = int((retry_at - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError):
            pass
    return min(max(seconds, 60), 24 * 60 * 60)


def get_max_threads_per_label() -> int:
    return (
        cint(
            frappe.get_cached_doc("Google Settings").get(
                "custom_gmail_max_threads_per_label"
            )
        )
        or DEFAULT_MAX_THREADS_PER_LABEL
    )


def list_label_threads(gmail, label_id: str, limit: int) -> list[dict]:
    """The label's newest `limit` threads, newest first, across result pages."""
    threads = []
    page_token = None
    while len(threads) < limit:
        response = (
            gmail.users()
            .threads()
            .list(
                userId="me",
                labelIds=label_id,
                maxResults=min(limit - len(threads), 500),
                pageToken=page_token,
            )
            .execute(num_retries=GMAIL_API_RETRIES)
        )
        threads.extend(response.get("threads", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return threads[:limit]


def list_label_history(gmail, label_id: str, start_history_id: int) -> dict:
    """The label's history since `start_history_id` across result pages, with the mailbox's history id."""
    history = []
    page_token = None
    while True:
        response = (
            gmail.users()
            .history()
            .list(
                userId="me",
                startHistoryId=start_history_id,
                historyTypes=["messageAdded", "labelAdded"],
                labelId=label_id,
                maxResults=500,
                pageToken=page_token,
            )
            .execute(num_retries=GMAIL_API_RETRIES)
        )
        history.extend(response.get("history", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            return {"history": history, "historyId": response.get("historyId")}


def get_message_headers(message):
    """Map a Gmail message payload's headers to a ``{lower_name: value}`` dict."""
    headers = (message.get("payload") or {}).get("headers") or []
    return {
        (header.get("name") or "").lower(): (header.get("value") or "")
        for header in headers
    }


def get_rfc_message_id(headers):
    """Extract the RFC 5322 ``Message-ID`` from a ``{lower_name: value}`` header dict."""
    if "message-id" in headers:
        return get_string_between("<", headers["message-id"], ">")
    return None


def get_message_participants(headers):
    """Extract sender/recipient addresses from a ``{lower_name: value}`` header dict."""
    participants = set()
    for name in ("from", "to", "cc", "bcc"):
        if name in headers:
            for _name, address in getaddresses([headers[name]]):
                if address:
                    participants.add(address)
    return participants


def thread_has_email(gmail_thread: Document, email: Document) -> bool:
    return any(
        row.gmail_message_id == email.gmail_message_id
        or (email.email_message_id and row.email_message_id == email.email_message_id)
        for row in gmail_thread.emails
    )


def get_email_participants(email_object, linked_user: str) -> set[str]:
    return {
        email_object.from_email,
        *email_object.to,
        *email_object.cc,
        *email_object.bcc,
        linked_user,
    }


def add_participants(
    gmail_thread: Document, participants: set[str], thread_id: str | None = None
) -> None:
    """Add participants, and the mailbox's Gmail thread id, to an imported thread.

    `modified` stays at the thread's latest email's date.
    """
    before = len(gmail_thread.involved_users)
    update_involved_users(gmail_thread, participants)
    new_reference = add_thread_id_reference(gmail_thread, thread_id)
    if new_reference or len(gmail_thread.involved_users) != before:
        prev_modified = gmail_thread.modified
        gmail_thread.save(ignore_permissions=True)
        frappe.db.set_value(
            "Gmail Thread",
            gmail_thread.name,
            "modified",
            prev_modified,
            update_modified=False,
        )


def gmail_message_exists(gmail_message_id, rfc_message_id=None):
    """Return True if this message was already imported."""
    if gmail_message_id and frappe.db.exists(
        "Single Email CT", {"gmail_message_id": gmail_message_id}
    ):
        return True
    if rfc_message_id and frappe.db.exists(
        "Single Email CT", {"email_message_id": rfc_message_id}
    ):
        return True
    return False


def sync(user=None):
    if user:
        frappe.set_user(user)  # nosemgrep:
    user = frappe.session.user
    if is_sync_paused(user):
        return
    account_name = frappe.db.get_value("Gmail Account", {"linked_user": user})
    if not account_name:
        return
    gmail_account = frappe.get_doc("Gmail Account", account_name)
    # the account can be disabled or lose its authorization while this job is queued
    if not gmail_account.gmail_enabled or not gmail_account.refresh_token:
        return
    last_history_id = int(gmail_account.last_historyid or 0)
    try:
        gmail = get_gmail_object(gmail_account)
        label_ids = [x.label_id for x in gmail_account.labels if x.enabled]
        if not label_ids:
            return
        # Before an initial import, the mailbox's current history id: mail arriving from
        # now on is in the history after it, so the next run continues incrementally even
        # if this import brings nothing (an empty label, or threads Gmail won't return).
        start_history_id = last_history_id or int(
            gmail.users()
            .getProfile(userId="me")
            .execute(num_retries=GMAIL_API_RETRIES)["historyId"]
        )
    except GmailAuthorizationExpiredError:
        handle_expired_authorization(gmail_account)
        return
    except googleapiclient.errors.HttpError as e:
        if not pause_on_temporary_error(e, user):
            raise
        return

    # Always store the maximum history id seen, to avoid skipping emails
    max_history_id = start_history_id
    max_threads = get_max_threads_per_label()

    for label_id in label_ids:
        try:
            if not last_history_id:
                # Initial sync: fetch all threads for the label
                threads = list_label_threads(gmail, label_id, max_threads)
                for thread in threads[::-1]:
                    thread_id = thread["id"]
                    try:
                        thread_data = (
                            gmail.users()
                            .threads()
                            .get(userId="me", id=thread_id)
                            .execute(num_retries=GMAIL_API_RETRIES)
                        )
                    except googleapiclient.errors.HttpError as e:
                        if not skip_unavailable(e, user, f"thread {thread_id}"):
                            raise
                        continue
                    gmail_thread = find_gmail_thread(thread_id)
                    involved_users = set()
                    email = None
                    for message in thread_data["messages"]:
                        # Build the header lookup once per message and reuse it
                        message_headers = get_message_headers(message)
                        # Track max history id
                        msg_history_id = int(message.get("historyId", 0))
                        if msg_history_id > max_history_id:
                            max_history_id = msg_history_id
                        rfc_message_id = get_rfc_message_id(message_headers)
                        if gmail_message_exists(message["id"], rfc_message_id):
                            if not gmail_thread and rfc_message_id:
                                # thread ids are per mailbox: another mailbox may have imported it
                                gmail_thread = find_gmail_thread(
                                    thread_id, [rfc_message_id]
                                )
                            if gmail_thread:
                                gmail_thread = lock_gmail_thread(gmail_thread.name)
                                participants = get_message_participants(message_headers)
                                participants.add(gmail_account.linked_user)
                                add_participants(gmail_thread, participants, thread_id)
                            continue
                        try:
                            raw_email = (
                                gmail.users()
                                .messages()
                                .get(userId="me", id=message["id"], format="raw")
                                .execute(num_retries=GMAIL_API_RETRIES)
                            )
                        except googleapiclient.errors.HttpError as e:
                            if not skip_unavailable(
                                e, user, f"message {message['id']}"
                            ):
                                raise
                            continue
                        if "DRAFT" in raw_email.get("labelIds", []):
                            continue
                        is_new_thread = False
                        try:
                            email, email_object = create_new_email(
                                raw_email, gmail_account
                            )
                        except AlreadyExistsError:
                            # releases the thread create_new_email locked
                            frappe.db.commit()  # nosemgrep
                            continue
                        if not gmail_thread:
                            email_message_id = email_object.message_id
                            email_references = email_object.mail.get("References")
                            if email_references:
                                email_references = [
                                    get_string_between("<", x, ">")
                                    for x in email_references.split()
                                ]
                            else:
                                email_references = []
                            gmail_thread = find_gmail_thread(
                                thread_id, [email_message_id] + email_references
                            )
                        if gmail_thread:
                            gmail_thread = lock_gmail_thread(gmail_thread.name)
                            if thread_has_email(gmail_thread, email):
                                # imported by another mailbox's sync while this one waited
                                add_participants(
                                    gmail_thread,
                                    get_email_participants(
                                        email_object, gmail_account.linked_user
                                    ),
                                    thread_id,
                                )
                                frappe.db.commit()  # nosemgrep
                                continue
                        else:
                            gmail_thread = frappe.new_doc("Gmail Thread")
                            gmail_thread.gmail_thread_id = thread_id
                            gmail_thread.gmail_account = gmail_account.name
                            is_new_thread = True
                        if not gmail_thread.subject_of_first_mail:
                            gmail_thread.subject_of_first_mail = email.subject
                        if gmail_thread.creation is None or get_datetime(
                            email.date_and_time
                        ) < get_datetime(gmail_thread.creation):
                            is_new_thread = True
                        involved_users.add(email_object.from_email)
                        for recipient in email_object.to:
                            involved_users.add(recipient)
                        for recipient in email_object.cc:
                            involved_users.add(recipient)
                        for recipient in email_object.bcc:
                            involved_users.add(recipient)
                        involved_users.add(gmail_account.linked_user)
                        update_involved_users(gmail_thread, involved_users)
                        process_attachments(email, gmail_thread, email_object)
                        replace_inline_images(email, email_object)
                        add_thread_references(
                            gmail_thread,
                            email_object,
                            thread_id=thread_id,
                            gmail_message_id=message["id"],
                        )
                        pos = get_email_insert_position(
                            gmail_thread.emails or [], email
                        )
                        gmail_thread.append("emails", email, position=pos)
                        latest_dt = gmail_thread.emails[-1].date_and_time
                        gmail_thread.save(ignore_permissions=True)
                        # in the same transaction as the save, so a later failure
                        # in this thread can't leave it with the import time
                        thread_values = {
                            "modified": latest_dt,
                            "owner": gmail_account.linked_user,
                        }
                        if is_new_thread:
                            thread_values["creation"] = email.date_and_time
                        frappe.db.set_value(
                            "Gmail Thread",
                            gmail_thread.name,
                            thread_values,
                            update_modified=False,
                        )
                    # one transaction per thread: commit the thread's emails together
                    frappe.db.commit()  # nosemgrep
                gmail_account.reload()
                gmail_account.last_historyid = max_history_id
                gmail_account.save(ignore_permissions=True)
                frappe.db.commit()  # nosemgrep
            else:
                # Incremental sync using history API
                try:
                    history = list_label_history(gmail, label_id, last_history_id)
                except googleapiclient.errors.HttpError as e:
                    if not is_not_found_error(e):
                        raise e
                    if not gmail_label_exists(gmail, label_id):
                        # deleted in Gmail. Delete the row directly: saving the
                        # account with changed labels would reset the whole sync
                        frappe.db.delete(
                            "Gmail Label",
                            {"parent": gmail_account.name, "label_id": label_id},
                        )
                        # release the rows now: saving the account locks its labels
                        frappe.db.commit()  # nosemgrep
                        continue
                    # The label exists, so startHistoryId is too old. The error
                    # carries no newer history id: reset to 0 for an initial sync
                    gmail_account.reload()
                    gmail_account.last_historyid = 0
                    gmail_account.save(ignore_permissions=True)
                    frappe.db.commit()
                    return

                new_history_id = int(history["historyId"] or last_history_id)
                if new_history_id > max_history_id:
                    max_history_id = new_history_id
                updated_docs = set()
                if "history" in history:
                    for hist in history["history"]:
                        for message in hist.get("messages", []):
                            try:
                                raw_email = (
                                    gmail.users()
                                    .messages()
                                    .get(userId="me", id=message["id"], format="raw")
                                    .execute(num_retries=GMAIL_API_RETRIES)
                                )
                            except googleapiclient.errors.HttpError as e:
                                if not skip_unavailable(
                                    e, user, f"message {message['id']}"
                                ):
                                    raise
                                continue
                            if "DRAFT" in raw_email.get("labelIds", []):
                                continue
                            thread_id = message["threadId"]
                            gmail_thread = find_gmail_thread(thread_id)
                            involved_users = set()
                            is_new_thread = False
                            try:
                                email, email_object = create_new_email(
                                    raw_email, gmail_account
                                )
                            except AlreadyExistsError:
                                # releases the thread create_new_email locked
                                frappe.db.commit()  # nosemgrep
                                continue
                            if not gmail_thread:
                                email_message_id = email_object.message_id
                                email_references = email_object.mail.get("References")
                                if email_references:
                                    email_references = [
                                        get_string_between("<", x, ">")
                                        for x in email_references.split()
                                    ]
                                else:
                                    email_references = []
                                gmail_thread = find_gmail_thread(
                                    thread_id, [email_message_id] + email_references
                                )
                            if gmail_thread:
                                gmail_thread = lock_gmail_thread(gmail_thread.name)
                                if thread_has_email(gmail_thread, email):
                                    # imported by another mailbox's sync while this one waited
                                    add_participants(
                                        gmail_thread,
                                        get_email_participants(
                                            email_object, gmail_account.linked_user
                                        ),
                                        thread_id,
                                    )
                                    frappe.db.commit()  # nosemgrep
                                    continue
                            else:
                                gmail_thread = frappe.new_doc("Gmail Thread")
                                gmail_thread.gmail_thread_id = thread_id
                                gmail_thread.gmail_account = gmail_account.name
                                is_new_thread = True
                            if not gmail_thread.subject_of_first_mail:
                                gmail_thread.subject_of_first_mail = email.subject
                            if gmail_thread.creation is None or get_datetime(
                                email.date_and_time
                            ) < get_datetime(gmail_thread.creation):
                                is_new_thread = True
                            involved_users.add(email_object.from_email)
                            for recipient in email_object.to:
                                involved_users.add(recipient)
                            for recipient in email_object.cc:
                                involved_users.add(recipient)
                            for recipient in email_object.bcc:
                                involved_users.add(recipient)
                            involved_users.add(gmail_account.linked_user)
                            update_involved_users(gmail_thread, involved_users)
                            process_attachments(email, gmail_thread, email_object)
                            replace_inline_images(email, email_object)
                            add_thread_references(
                                gmail_thread,
                                email_object,
                                thread_id=thread_id,
                                gmail_message_id=message["id"],
                            )
                            pos = get_email_insert_position(
                                gmail_thread.emails or [], email
                            )
                            gmail_thread.append("emails", email, position=pos)
                            latest_dt = gmail_thread.emails[-1].date_and_time
                            gmail_thread.save(ignore_permissions=True)
                            frappe.db.set_value(
                                "Gmail Thread",
                                gmail_thread.name,
                                "modified",
                                latest_dt,
                                update_modified=False,
                            )
                            if is_new_thread:  # update creation date
                                frappe.db.set_value(
                                    "Gmail Thread",
                                    gmail_thread.name,
                                    "creation",
                                    email.date_and_time,
                                    update_modified=False,
                                )
                            # don't keep the thread locked for the rest of the label
                            frappe.db.commit()  # nosemgrep
                            if (
                                gmail_thread.reference_doctype
                                and gmail_thread.reference_name
                            ):
                                updated_docs.add(
                                    (
                                        gmail_thread.reference_doctype,
                                        gmail_thread.reference_name,
                                    )
                                )
                gmail_account.reload()
                gmail_account.last_historyid = max_history_id
                gmail_account.save(ignore_permissions=True)
                frappe.db.commit()  # nosemgrep
                if updated_docs:
                    for doctype, docname in updated_docs:
                        frappe.publish_realtime(
                            "gthread_new_email",
                            doctype=doctype,
                            docname=docname,
                        )
        except (frappe.QueryDeadlockError, frappe.QueryTimeoutError) as e:
            # A thread stayed locked by another sync: retry the whole job instead of
            # skipping this label's remaining emails. The retry starts from the sync
            # position this run started from, as labels done earlier in the run have
            # moved it past emails of the labels not reached yet.
            frappe.db.rollback()
            frappe.db.set_value(
                "Gmail Account",
                gmail_account.name,
                "last_historyid",
                last_history_id,
                update_modified=False,
            )
            # the job runner rolls back before retrying
            frappe.db.commit()  # nosemgrep
            raise frappe.RetryBackgroundJobError from e
        except Exception as e:
            if pause_on_temporary_error(e, user):
                # Go back to the sync position this run started from, as labels
                # already done may have moved it past emails of the labels not reached yet.
                frappe.db.set_value(
                    "Gmail Account",
                    gmail_account.name,
                    "last_historyid",
                    last_history_id,
                    update_modified=False,
                )
                return
            frappe.log_error(frappe.get_traceback(), "Gmail Thread Sync Error")
            continue


def update_involved_users(doc, involved_users):
    involved_users = list(involved_users)
    involved_users_linked = [x.account for x in doc.involved_users]
    all_users = frappe.get_all(
        "User",
        filters={"email": ["in", involved_users], "user_type": ["!=", "Website User"]},
        fields=["name"],
    )
    for user in all_users:
        if user.name not in involved_users_linked:
            involved_user = frappe.get_doc(doctype="Involved User", account=user.name)
            doc.append("involved_users", involved_user)


def get_email_insert_position(gmail_list, email):
    """Find the position to insert email by `date_and_time` in ascending order.

    Returns:
        int: 0-based position (or -1 to append at end)
    """
    if not gmail_list:
        return -1

    email_dt = get_datetime(email.date_and_time)
    last_dt = get_datetime(gmail_list[-1].date_and_time)

    if email_dt >= last_dt:
        return -1

    low, high = 0, len(gmail_list)
    while low < high:
        mid = (low + high) // 2
        mid_dt = get_datetime(gmail_list[mid].date_and_time)
        if mid_dt <= email_dt:
            low = mid + 1
        else:
            high = mid

    return low


def get_permission_query_conditions(user):
    if not user:
        user = frappe.session.user
    if user == "Administrator":
        return ""

    doctype_with_read_access = get_doctypes_with_read(user)

    return """(
      `tabGmail Thread`.name in (
            select parent from `tabInvolved User`
            where account = {user}
        ) or `tabGmail Thread`.owner = {user} or `tabGmail Thread`.reference_doctype in ({doctypes})
    )""".format(
        user=frappe.db.escape(user),
        doctypes=", ".join([frappe.db.escape(d) for d in doctype_with_read_access]),
    )


def has_permission(doc, ptype, user):
    if user == "Administrator":
        return True
    if ptype in ("read", "write", "delete", "create"):
        if (
            frappe.db.exists(
                "Involved User",
                {"parent": doc.name, "account": user},
            )
            is not None
        ):
            return True
        if doc.reference_doctype and doc.reference_name:
            return frappe.has_permission(
                doc.reference_doctype, ptype, doc.reference_name, user=user
            )

    return False
