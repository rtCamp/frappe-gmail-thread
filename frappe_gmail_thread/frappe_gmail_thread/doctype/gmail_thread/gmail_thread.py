# Copyright (c) 2024, rtCamp and contributors
# For license information, please see license.txt


import random
import time

import frappe
import frappe.share
import googleapiclient.errors
from frappe import _
from frappe.model.document import Document
from frappe.utils import get_datetime, get_string_between

from frappe_gmail_thread.api.oauth import get_gmail_object
from frappe_gmail_thread.utils.helpers import (
    AlreadyExistsError,
    add_thread_references,
    create_new_email,
    find_gmail_thread,
    process_attachments,
    replace_inline_images,
)

SCOPES = "https://www.googleapis.com/auth/gmail.readonly"

# --- Gmail API throughput tuning -------------------------------------------
# messages.get raw responses are fetched in HTTP batches (one round-trip for
# many ids) instead of one sequential call each. Batching cuts latency, not
# quota — every sub-request still costs its units — so we also retry rate-limit
# responses with backoff and pace each batch to stay under Gmail's per-user
# quota budget.
_BATCH_SIZE = 50  # sub-requests per batch round-trip (Gmail caps batches at ~100)
_MAX_RETRIES = 5  # retries (with backoff) for a rate-limited call
_QUOTA_UNITS_PER_SEC = 250  # Gmail's per-user budget; batches pace to this
_MESSAGES_GET_UNITS = 5  # quota cost of one messages.get


def _is_rate_limit_error(exc):
    """True for Gmail's rate-limit responses (HTTP 429, or a 403 rate-limit reason)."""
    if not isinstance(exc, googleapiclient.errors.HttpError):
        return False
    status = getattr(exc.resp, "status", None)
    if status == 429:
        return True
    if status != 403:
        return False
    # A 403 is a rate-limit signal only for specific reasons; other 403s
    # (e.g. insufficient permissions) must not be retried. Prefer the structured
    # error reason, but fall back to a substring match since error_details is
    # not reliably populated across googleapiclient versions.
    details = getattr(exc, "error_details", None)
    if isinstance(details, list):
        for detail in details:
            if isinstance(detail, dict) and detail.get("reason") in (
                "rateLimitExceeded",
                "userRateLimitExceeded",
            ):
                return True
    text = str(exc).lower()
    return "ratelimitexceeded" in text or "rate limit" in text


def _is_not_found_error(exc):
    """True when a message can no longer be fetched (deleted between list and get)."""
    if not isinstance(exc, googleapiclient.errors.HttpError):
        return False
    if getattr(exc.resp, "status", None) == 404:
        return True
    details = getattr(exc, "error_details", None)
    if isinstance(details, list):
        for detail in details:
            if isinstance(detail, dict) and detail.get("reason") == "notFound":
                return True
    return False


def _is_transient_error(exc):
    """True for transient Gmail server errors worth retrying (5xx / backendError).

    e.g. ``503 "The service is currently unavailable." (reason: backendError)`` —
    Google's guidance is to retry these with backoff, not to drop the message.
    """
    if not isinstance(exc, googleapiclient.errors.HttpError):
        return False
    if getattr(exc.resp, "status", None) in (500, 502, 503, 504):
        return True
    details = getattr(exc, "error_details", None)
    if isinstance(details, list):
        for detail in details:
            if isinstance(detail, dict) and detail.get("reason") in (
                "backendError",
                "internalError",
            ):
                return True
    return False


def _is_retryable_error(exc):
    """Rate-limit or transient server error — both worth a backoff retry."""
    return _is_rate_limit_error(exc) or _is_transient_error(exc)


def _sleep_backoff(attempt):
    """Exponential backoff with jitter: ~1s, 2s, 4s … capped at ~64s."""
    time.sleep(min(2**attempt, 64) + random.uniform(0, 1))


def batch_get_raw_messages(gmail, message_ids):
    """Fetch many messages in raw format via batched ``messages.get`` calls.

    - Bundles up to ``_BATCH_SIZE`` calls per HTTP round-trip instead of one
      sequential request per id.
    - Retries rate-limited ids with exponential backoff (a single retry budget,
      not per-batch, so it can't compound).
    - Paces each batch to the per-user quota budget to avoid bursting into 429s.
    - ``notFound`` (deleted) messages are skipped; anything still failing after
      retries is logged — never silently dropped.

    Returns ``{message_id: raw_message}`` for every id that was fetched.
    """
    results = {}
    pending = list(message_ids)
    for attempt in range(_MAX_RETRIES + 1):
        if not pending:
            break
        errors = {}

        def _cb(request_id, response, exception):
            if exception is not None:
                errors[request_id] = exception
            else:
                results[request_id] = response

        for i in range(0, len(pending), _BATCH_SIZE):
            chunk = pending[i : i + _BATCH_SIZE]
            batch = gmail.new_batch_http_request()
            for message_id in chunk:
                batch.add(
                    gmail.users()
                    .messages()
                    .get(userId="me", id=message_id, format="raw"),
                    callback=_cb,
                    request_id=message_id,
                )
            try:
                batch.execute()
            except googleapiclient.errors.HttpError as e:
                # The batch request as a whole failed, so no callbacks fired.
                # Re-queue the chunk if the error is retryable; otherwise re-raise.
                if not _is_retryable_error(e):
                    raise
                for message_id in chunk:
                    errors.setdefault(message_id, e)
            # Spread this batch's quota cost over time to avoid a burst.
            time.sleep(len(chunk) * _MESSAGES_GET_UNITS / _QUOTA_UNITS_PER_SEC)

        # Retry rate-limited AND transient (5xx/backendError) ids; skip notFound;
        # log the rest. Without this a one-off 503 would drop the message.
        pending = [mid for mid, exc in errors.items() if _is_retryable_error(exc)]
        for mid, exc in errors.items():
            if mid not in pending and not _is_not_found_error(exc):
                frappe.log_error(
                    title="Gmail batch fetch error", message=f"{mid}: {exc}"
                )
        if pending and attempt < _MAX_RETRIES:
            _sleep_backoff(attempt)

    if pending:
        frappe.log_error(
            title="Gmail batch fetch gave up",
            message=f"{len(pending)} message(s) still failing after retries",
        )
    return results


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
        gmail_account = frappe.get_doc("Gmail Account", account_name)
    else:
        gmail_account = account_name

    gmail = get_gmail_object(gmail_account)
    labels = gmail.users().labels().list(userId="me").execute()

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
        gmail_account.save(ignore_permissions=True)


def sync(user=None):
    if user:
        frappe.set_user(user)  # nosemgrep:
    user = frappe.session.user
    gmail_account = frappe.get_doc("Gmail Account", {"linked_user": user})
    if not gmail_account.gmail_enabled:
        frappe.throw(_("Please configure Gmail in Email Account."))
    if not gmail_account.refresh_token:
        frappe.throw(
            _("Please authorize Gmail by clicking on 'Authorize Gmail' button.")
        )
    gmail = get_gmail_object(gmail_account)
    label_ids = [x.label_id for x in gmail_account.labels if x.enabled]
    if not label_ids:
        return

    last_history_id = int(gmail_account.last_historyid or 0)
    max_history_id = last_history_id

    # Gate on initial_sync_complete, NOT last_historyid: a partial initial run
    # writes a historyId, and gating on that would flip later runs into
    # incremental mode and skip the rest of the backlog forever.
    if not int(gmail_account.initial_sync_complete or 0):
        run_initial_sync(gmail, gmail_account, label_ids, max_history_id)
        return

    for label_id in label_ids:
        try:
            # Incremental sync using the history API
            result = sync_history(gmail, gmail_account, label_id, last_history_id)
            if result is None:
                # Stored historyId is too old; force a fresh (checkpointed)
                # initial sync next run so we pick up everything we missed.
                frappe.db.set_value(
                    "Gmail Account",
                    gmail_account.name,
                    {
                        "last_historyid": 0,
                        "initial_sync_complete": 0,
                        "initial_sync_state": "",
                    },
                    update_modified=False,
                )
                frappe.db.commit()  # nosemgrep
                return
            label_max, updated_docs = result
            if label_max > max_history_id:
                max_history_id = label_max
            gmail_account.reload()
            gmail_account.last_historyid = max_history_id
            gmail_account.save(ignore_permissions=True)
            frappe.db.commit()  # nosemgrep
            for doctype, docname in updated_docs:
                frappe.publish_realtime(
                    "gthread_new_email",
                    doctype=doctype,
                    docname=docname,
                )
        except Exception:
            frappe.log_error(frappe.get_traceback(), "Gmail Thread Sync Error")
            continue


def _checkpoint_initial_sync(account_name, max_history_id, next_state):
    """Persist initial-sync progress and commit immediately so a crash can't lose it.

    ``next_state`` is the ``{"label_id", "page_token"}`` dict to resume from, or
    ``None`` once every label is done — in which case the initial sync is marked
    complete and the next run switches to incremental mode.
    """
    values = {
        "last_historyid": max_history_id,
        "initial_sync_state": frappe.as_json(next_state) if next_state else "",
    }
    if next_state is None:
        values["initial_sync_complete"] = 1
    frappe.db.set_value("Gmail Account", account_name, values, update_modified=False)
    frappe.db.commit()  # nosemgrep


def sync_threads_page(gmail, gmail_account, label_id, page_token):
    """Fetch and persist a single ``messages.list`` page during initial sync.

    Returns ``(max_history_id_on_page, next_page_token)``. ``messages.list``
    returns newest-first, so the page is processed oldest-first to keep thread
    creation dates chronological. Only one page of raw emails is held in memory
    at a time.
    """
    response = (
        gmail.users()
        .messages()
        .list(userId="me", labelIds=label_id, pageToken=page_token)
        .execute(num_retries=_MAX_RETRIES)
    )
    stubs = response.get("messages", [])
    next_page_token = response.get("nextPageToken")

    raw_by_id = batch_get_raw_messages(gmail, [s["id"] for s in stubs])

    max_history_id = 0
    for stub in reversed(stubs):
        raw_email = raw_by_id.get(stub["id"])
        if raw_email is None:
            continue
        msg_history_id = int(raw_email.get("historyId", 0))
        if msg_history_id > max_history_id:
            max_history_id = msg_history_id
        thread_id = raw_email.get("threadId", stub.get("threadId"))
        gmail_thread = find_gmail_thread(thread_id)
        process_message(
            raw_email,
            raw_email,
            gmail_account,
            thread_id,
            gmail_thread=gmail_thread,
            set_owner=True,
            commit=True,
        )

    return max_history_id, next_page_token


def run_initial_sync(gmail, gmail_account, label_ids, max_history_id):
    """Import the backlog for every enabled label, page by page.

    The resume point (current label + its next page token) is checkpointed after
    every page and committed immediately, so a run killed by the worker timeout
    resumes from the last finished page instead of re-fetching the whole label.
    """
    state = frappe.parse_json(gmail_account.initial_sync_state or "{}")
    resume_label_id = state.get("label_id")
    resume_page_token = state.get("page_token")
    if resume_label_id in label_ids:
        start_idx = label_ids.index(resume_label_id)
    else:
        # Checkpointed label is no longer enabled (or no checkpoint yet) — start
        # from the first label. Re-imports are deduped, skipping labels is not.
        start_idx = 0
        resume_page_token = None

    for label_idx in range(start_idx, len(label_ids)):
        label_id = label_ids[label_idx]
        # Use the saved page token only for the label we actually stopped on.
        page_token = resume_page_token if label_id == resume_label_id else None
        resume_page_token = None

        while True:
            try:
                page_max, next_page_token = sync_threads_page(
                    gmail, gmail_account, label_id, page_token
                )
            except Exception:
                # Log and stop without advancing the checkpoint, so the next run
                # retries this same page rather than skipping the backlog.
                frappe.log_error(frappe.get_traceback(), "Gmail Initial Sync Error")
                return

            if page_max > max_history_id:
                max_history_id = page_max

            # Persist where the next run should resume.
            if next_page_token:
                next_state = {"label_id": label_id, "page_token": next_page_token}
            elif label_idx + 1 < len(label_ids):
                next_state = {"label_id": label_ids[label_idx + 1], "page_token": None}
            else:
                next_state = None  # every label finished
            _checkpoint_initial_sync(gmail_account.name, max_history_id, next_state)

            page_token = next_page_token
            if not page_token:
                break  # this label is done


def sync_history(gmail, gmail_account, label_id, start_history_id):
    """Incremental sync for a label using the history API.

    Pages through history collecting the unique set of added message ids (the
    same message can be reported by multiple history records, e.g. both
    ``messageAdded`` and ``labelAdded``) so each raw message is fetched only
    once.

    Returns a ``(max_history_id, updated_docs)`` tuple, or ``None`` if the
    stored historyId is too old and a full re-sync is required.
    """
    max_history_id = start_history_id
    # message_id -> thread_id, deduped across history records and pages
    message_thread_ids = {}

    page_token = None
    while True:
        try:
            history = (
                gmail.users()
                .history()
                .list(
                    userId="me",
                    startHistoryId=start_history_id,
                    historyTypes=["messageAdded", "labelAdded"],
                    labelId=label_id,
                    pageToken=page_token,
                )
                .execute(num_retries=_MAX_RETRIES)
            )
        except googleapiclient.errors.HttpError as e:
            # notFound means the stored historyId has expired; signal a reset.
            if hasattr(e, "error_details"):
                for error in e.error_details:
                    if error.get("reason") == "notFound":
                        return None
            raise e

        new_history_id = int(history.get("historyId", start_history_id))
        if new_history_id > max_history_id:
            max_history_id = new_history_id

        for hist in history.get("history", []):
            for message in hist.get("messages", []):
                message_thread_ids[message["id"]] = message["threadId"]

        page_token = history.get("nextPageToken")
        if not page_token:
            break

    # Batch-fetch every raw message in one set of round-trips.
    raw_by_id = batch_get_raw_messages(gmail, list(message_thread_ids.keys()))

    updated_docs = set()
    for message_id, thread_id in message_thread_ids.items():
        raw_email = raw_by_id.get(message_id)
        if raw_email is None:
            continue
        gmail_thread = find_gmail_thread(thread_id)
        gmail_thread = process_message(
            raw_email,
            raw_email,
            gmail_account,
            thread_id,
            gmail_thread=gmail_thread,
        )
        if (
            gmail_thread
            and gmail_thread.reference_doctype
            and gmail_thread.reference_name
        ):
            updated_docs.add(
                (gmail_thread.reference_doctype, gmail_thread.reference_name)
            )

    return max_history_id, updated_docs


def process_message(
    raw_email,
    message,
    gmail_account,
    thread_id,
    gmail_thread=None,
    set_owner=False,
    commit=False,
):
    """Create the email from a raw Gmail message and append it to its Gmail Thread.

    Args:
        raw_email: The raw message payload returned by the Gmail API.
        message: The Gmail message dict (used for ``message["id"]``).
        gmail_account: The Gmail Account document being synced.
        thread_id: The Gmail thread id this message belongs to.
        gmail_thread: A pre-fetched Gmail Thread document (looked up by thread id),
            or ``None`` to resolve/create it here.
        set_owner: Whether to set the thread owner to the linked user.
        commit: Whether to commit after saving the thread.

    Returns:
        The saved Gmail Thread document, or ``None`` if the message was skipped
        (a draft or an email that already exists).
    """
    if "DRAFT" in raw_email.get("labelIds", []):
        return None

    try:
        email, email_object = create_new_email(raw_email, gmail_account)
    except AlreadyExistsError:
        return None

    if not gmail_thread:
        email_message_id = email_object.message_id
        email_references = email_object.mail.get("References")
        if email_references:
            email_references = [
                get_string_between("<", x, ">") for x in email_references.split()
            ]
        else:
            email_references = []
        gmail_thread = find_gmail_thread(
            thread_id, [email_message_id] + email_references
        )

    is_new_thread = False
    if gmail_thread:
        gmail_thread.reload()
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

    involved_users = set()
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
    pos = get_email_insert_position(gmail_thread.emails or [], email)
    gmail_thread.append("emails", email, position=pos)
    latest_dt = gmail_thread.emails[-1].date_and_time
    gmail_thread.save(ignore_permissions=True)
    if commit:
        frappe.db.commit()  # nosemgrep
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
    if set_owner:
        frappe.db.set_value(
            "Gmail Thread",
            gmail_thread.name,
            "owner",
            gmail_account.linked_user,
            modified_by=gmail_account.linked_user,
            update_modified=False,
        )
    return gmail_thread


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
    return """
        `tabGmail Thread`.name in (
            select parent from `tabInvolved User`
            where account = {user}
        ) or `tabGmail Thread`.owner = {user}
    """.format(user=frappe.db.escape(user))


def has_permission(doc, ptype, user):
    if user == "Administrator":
        return True
    if ptype in ("read", "write", "delete", "create"):
        return (
            frappe.db.exists(
                "Involved User",
                {"parent": doc.name, "account": user},
            )
            is not None
        )
    return False
