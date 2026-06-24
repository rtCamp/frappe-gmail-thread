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


_MESSAGES_GET_UNITS = 5
_SYNC_SETTINGS = {
    "batch_size": ("custom_gmail_batch_size", 100),  # sub-requests per round-trip
    "max_retries": ("custom_gmail_max_retries", 5),
    "max_run_seconds": (
        "custom_gmail_max_run_seconds",
        1200,
    ),  # initial-sync time budget
    "quota_units_per_sec": ("custom_gmail_quota_units_per_sec", 250),
}


def _sync_setting(name):
    """Read a tuning knob (key of _SYNC_SETTINGS) from Google Settings.

    Falls back to the default when the value is unset, non-positive, invalid, or
    the custom field hasn't been migrated in yet.
    """
    fieldname, default = _SYNC_SETTINGS[name]
    try:
        value = int(frappe.db.get_single_value("Google Settings", fieldname))
    except Exception:
        return default
    return value if value > 0 else default


def _should_retry(exc):
    """Retry rate-limit (429 / 403 rateLimitExceeded) and transient 5xx errors."""
    if not isinstance(exc, googleapiclient.errors.HttpError):
        return False
    status = getattr(exc.resp, "status", None)
    if status in (429, 500, 502, 503, 504):
        return True
    text = str(exc).lower()
    return status == 403 and ("rate limit" in text or "ratelimitexceeded" in text)


def _is_not_found(exc):
    """True for a 404 — message deleted between list and get; safe to skip."""
    return (
        isinstance(exc, googleapiclient.errors.HttpError)
        and getattr(exc.resp, "status", None) == 404
    )


def _sleep_backoff(attempt):
    """Exponential backoff with jitter: ~1s, 2s, 4s … capped at ~64s."""
    time.sleep(min(2**attempt, 64) + random.uniform(0, 1))


def batch_get_raw_messages(gmail, message_ids):
    """Fetch raw messages via batched ``messages.get``, retrying with backoff.

    Returns ``{message_id: raw_message}`` for every id fetched. notFound ids are
    skipped; retryable failures (rate-limit / 5xx) are retried; the rest logged.
    """
    batch_size = _sync_setting("batch_size")
    max_retries = _sync_setting("max_retries")
    units_per_sec = _sync_setting("quota_units_per_sec")
    results = {}
    pending = list(message_ids)
    for attempt in range(max_retries + 1):
        if not pending:
            break
        errors = {}

        def _cb(request_id, response, exception):
            if exception is not None:
                errors[request_id] = exception
            else:
                results[request_id] = response

        for i in range(0, len(pending), batch_size):
            chunk = pending[i : i + batch_size]
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
                # Whole-batch failure (no callbacks fired): re-queue if retryable.
                if not _should_retry(e):
                    raise
                for message_id in chunk:
                    errors.setdefault(message_id, e)
            time.sleep(len(chunk) * _MESSAGES_GET_UNITS / units_per_sec)

        pending = [mid for mid, exc in errors.items() if _should_retry(exc)]
        for mid, exc in errors.items():
            if mid not in pending and not _is_not_found(exc):
                frappe.log_error(
                    title="Gmail batch fetch error", message=f"{mid}: {exc}"
                )
        if pending and attempt < max_retries:
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

    # Gate on initial_sync_complete, not last_historyid: a partial initial run
    # writes a historyId, which would otherwise flip later runs into incremental
    # mode and skip the rest of the backlog.
    if not int(gmail_account.initial_sync_complete or 0):
        run_initial_sync(gmail, gmail_account, label_ids, max_history_id)
        return

    for label_id in label_ids:
        try:
            result = sync_history(gmail, gmail_account, label_id, last_history_id)
            if result is None:
                # History expired — force a fresh initial sync next run.
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
                    "gthread_new_email", doctype=doctype, docname=docname
                )
        except Exception:
            frappe.log_error(frappe.get_traceback(), "Gmail Thread Sync Error")
            continue


def _checkpoint_initial_sync(account_name, max_history_id, next_state):
    """Persist (and commit) the initial-sync resume point so a crash can't lose it.

    ``next_state`` is ``{"label_id", "page_token"}`` to resume from, or ``None``
    when every label is done — which marks the initial sync complete.
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
    """Fetch and persist one ``messages.list`` page. Returns (max_history_id, next_page_token)."""
    response = (
        gmail.users()
        .messages()
        .list(userId="me", labelIds=label_id, pageToken=page_token)
        .execute(num_retries=_sync_setting("max_retries"))
    )
    stubs = response.get("messages", [])
    next_page_token = response.get("nextPageToken")
    raw_by_id = batch_get_raw_messages(gmail, [s["id"] for s in stubs])

    # Group by thread (oldest-first; messages.list is newest-first) so each
    # thread is saved once with all its new messages.
    max_history_id = 0
    threads = {}
    order = []
    for stub in reversed(stubs):
        raw_email = raw_by_id.get(stub["id"])
        if raw_email is None:
            continue
        max_history_id = max(max_history_id, int(raw_email.get("historyId", 0)))
        thread_id = raw_email.get("threadId", stub.get("threadId"))
        if thread_id not in threads:
            threads[thread_id] = []
            order.append(thread_id)
        threads[thread_id].append(raw_email)

    for thread_id in order:
        try:
            process_thread_messages(gmail_account, thread_id, threads[thread_id])
        except Exception:
            # Skip one bad thread rather than abort the page (and its checkpoint).
            frappe.log_error(
                frappe.get_traceback(), f"Gmail Thread Sync Error: {thread_id}"
            )

    return max_history_id, next_page_token


def run_initial_sync(gmail, gmail_account, label_ids, max_history_id):
    """Import the backlog per label, page by page, checkpointing after each page
    so a killed run resumes from the last finished page instead of restarting.

    Stops early and resumes next run when either limit is hit:
      - page_incremental_sync (per-account): one page per scheduled run, or
      - max_run_seconds (global): the time budget.
    """
    one_page_per_run = bool(gmail_account.get("page_incremental_sync"))
    max_run_seconds = _sync_setting("max_run_seconds")
    start_time = time.monotonic()

    state = frappe.parse_json(gmail_account.initial_sync_state or "{}")
    resume_label_id = state.get("label_id")
    resume_page_token = state.get("page_token")
    if resume_label_id in label_ids:
        start_idx = label_ids.index(resume_label_id)
    else:
        # Stale/absent checkpoint — restart from the first label (re-imports dedupe).
        start_idx = 0
        resume_page_token = None

    for label_idx in range(start_idx, len(label_ids)):
        label_id = label_ids[label_idx]
        page_token = resume_page_token if label_id == resume_label_id else None
        resume_page_token = None

        while True:
            try:
                page_max, next_page_token = sync_threads_page(
                    gmail, gmail_account, label_id, page_token
                )
            except Exception:
                # Stop without advancing the checkpoint so the next run retries here.
                frappe.log_error(frappe.get_traceback(), "Gmail Initial Sync Error")
                return

            max_history_id = max(max_history_id, page_max)
            if next_page_token:
                next_state = {"label_id": label_id, "page_token": next_page_token}
            elif label_idx + 1 < len(label_ids):
                next_state = {"label_id": label_ids[label_idx + 1], "page_token": None}
            else:
                next_state = None
            _checkpoint_initial_sync(gmail_account.name, max_history_id, next_state)

            page_token = next_page_token
            if not page_token:
                break  # this label is done
            # Yield (resume from the checkpoint next run) when out of budget.
            if one_page_per_run:
                return
            if time.monotonic() - start_time >= max_run_seconds:
                return


def sync_history(gmail, gmail_account, label_id, start_history_id):
    """Incremental sync via the history API.

    Returns ``(max_history_id, updated_docs)``, or ``None`` if the stored
    historyId expired and a full re-sync is needed.
    """
    max_history_id = start_history_id
    message_thread_ids = {}  # message_id -> thread_id, deduped across pages

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
                .execute(num_retries=_sync_setting("max_retries"))
            )
        except googleapiclient.errors.HttpError as e:
            if _is_not_found(e):  # historyId expired
                return None
            raise

        max_history_id = max(
            max_history_id, int(history.get("historyId", start_history_id))
        )
        for hist in history.get("history", []):
            for message in hist.get("messages", []):
                message_thread_ids[message["id"]] = message["threadId"]
        page_token = history.get("nextPageToken")
        if not page_token:
            break

    raw_by_id = batch_get_raw_messages(gmail, list(message_thread_ids.keys()))

    # Group by thread so each thread is saved once.
    threads = {}
    for message_id, thread_id in message_thread_ids.items():
        raw_email = raw_by_id.get(message_id)
        if raw_email is not None:
            threads.setdefault(thread_id, []).append(raw_email)

    updated_docs = set()
    for thread_id, raw_emails in threads.items():
        gmail_thread = process_thread_messages(gmail_account, thread_id, raw_emails)
        if (
            gmail_thread
            and gmail_thread.reference_doctype
            and gmail_thread.reference_name
        ):
            updated_docs.add(
                (gmail_thread.reference_doctype, gmail_thread.reference_name)
            )
    return max_history_id, updated_docs


def process_thread_messages(gmail_account, thread_id, raw_emails):
    """Persist a thread's raw messages (oldest-first) with one save + commit.

    Resolves/creates the Gmail Thread once, appends every non-draft, non-
    duplicate message, then saves. Returns the thread, or ``None`` if nothing new.
    """
    gmail_thread = find_gmail_thread(thread_id)
    if gmail_thread:
        gmail_thread.reload()

    involved_users = set()
    is_new_thread = False
    creation_changed = False  # an earlier-dated email means `creation` must move
    creation_dt = None
    # A new thread has no name yet; save once before linking attachments.
    first_save_done = gmail_thread is not None
    appended = 0

    for raw_email in raw_emails:
        if "DRAFT" in raw_email.get("labelIds", []):
            continue
        try:
            email, email_object = create_new_email(raw_email, gmail_account)
        except AlreadyExistsError:
            continue

        if not gmail_thread:
            # Match an existing thread via the message id + References headers.
            refs = email_object.mail.get("References")
            refs = (
                [get_string_between("<", x, ">") for x in refs.split()] if refs else []
            )
            gmail_thread = find_gmail_thread(
                thread_id, [email_object.message_id] + refs
            )
            if gmail_thread:
                first_save_done = True

        if gmail_thread is None:
            gmail_thread = frappe.new_doc("Gmail Thread")
            gmail_thread.gmail_thread_id = thread_id
            gmail_thread.gmail_account = gmail_account.name
            is_new_thread = True

        if not gmail_thread.subject_of_first_mail:
            gmail_thread.subject_of_first_mail = email.subject

        email_dt = get_datetime(email.date_and_time)
        existing = (
            get_datetime(gmail_thread.creation) if gmail_thread.creation else None
        )
        if existing is None or email_dt < existing:
            creation_changed = True
            if creation_dt is None or email_dt < get_datetime(creation_dt):
                creation_dt = email.date_and_time

        involved_users.update(email_object.to, email_object.cc, email_object.bcc)
        involved_users.add(email_object.from_email)
        involved_users.add(gmail_account.linked_user)

        process_attachments(email, gmail_thread, email_object)
        replace_inline_images(email, email_object)
        add_thread_references(
            gmail_thread,
            email_object,
            thread_id=thread_id,
            gmail_message_id=raw_email["id"],
        )
        pos = get_email_insert_position(gmail_thread.emails or [], email)
        gmail_thread.append("emails", email, position=pos)
        appended += 1

        if not first_save_done:
            gmail_thread.save(ignore_permissions=True)
            first_save_done = True

    if not appended:
        return None

    update_involved_users(gmail_thread, involved_users)
    latest_dt = gmail_thread.emails[-1].date_and_time
    gmail_thread.save(ignore_permissions=True)
    frappe.db.set_value(
        "Gmail Thread", gmail_thread.name, "modified", latest_dt, update_modified=False
    )
    if is_new_thread or creation_changed:
        frappe.db.set_value(
            "Gmail Thread",
            gmail_thread.name,
            "creation",
            creation_dt or latest_dt,
            update_modified=False,
        )
    frappe.db.set_value(
        "Gmail Thread",
        gmail_thread.name,
        "owner",
        gmail_account.linked_user,
        modified_by=gmail_account.linked_user,
        update_modified=False,
    )
    frappe.db.commit()  # nosemgrep
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
