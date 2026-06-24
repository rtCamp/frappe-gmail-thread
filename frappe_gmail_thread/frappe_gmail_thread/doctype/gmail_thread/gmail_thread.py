# Copyright (c) 2024, rtCamp and contributors
# For license information, please see license.txt


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

    # Always store the maximum history id seen, to avoid skipping emails
    last_history_id = int(gmail_account.last_historyid or 0)
    max_history_id = last_history_id

    for label_id in label_ids:
        try:
            if not last_history_id:
                # Initial sync: import every message in the label
                label_max = sync_threads(gmail, gmail_account, label_id)
                if label_max > max_history_id:
                    max_history_id = label_max
                gmail_account.reload()
                gmail_account.last_historyid = max_history_id
                gmail_account.save(ignore_permissions=True)
                frappe.db.commit()  # nosemgrep
            else:
                # Incremental sync using the history API
                result = sync_history(gmail, gmail_account, label_id, last_history_id)
                if result is None:
                    # Stored historyId is too old; reset to 0 so the next run
                    # performs a full sync and pick up everything we missed.
                    gmail_account.last_historyid = 0
                    gmail_account.save(ignore_permissions=True)
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


def get_raw_message(gmail, message_id):
    """Fetch a single message in raw format.

    Returns the message resource (which carries ``id``, ``threadId``,
    ``labelIds``, ``historyId`` and ``raw``), or ``None`` if the message can no
    longer be fetched (e.g. it was deleted between listing and fetching).
    """
    try:
        return (
            gmail.users()
            .messages()
            .get(userId="me", id=message_id, format="raw")
            .execute()
        )
    except googleapiclient.errors.HttpError as e:
        if not hasattr(e, "error_details"):
            raise e
        # notFound / inaccessible message — skip it
        return None


def sync_threads(gmail, gmail_account, label_id):
    """Initial sync for a label.

    Lists all message ids for the label (following ``nextPageToken`` across
    pages) and fetches each raw message directly. This avoids the previous
    ``threads.get`` call per thread, since ``messages.list`` already returns the
    ``threadId`` for every message.

    Returns the maximum Gmail ``historyId`` seen.
    """
    max_history_id = 0

    # Collect message stubs across all pages. messages.list returns newest
    # first, so we process the reversed list (oldest first) to keep thread
    # creation dates consistent.
    message_stubs = []
    page_token = None
    while True:
        response = (
            gmail.users()
            .messages()
            .list(userId="me", labelIds=label_id, pageToken=page_token)
            .execute()
        )
        message_stubs.extend(response.get("messages", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    for stub in reversed(message_stubs):
        raw_email = get_raw_message(gmail, stub["id"])
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

    return max_history_id


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
                .execute()
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

    updated_docs = set()
    for message_id, thread_id in message_thread_ids.items():
        raw_email = get_raw_message(gmail, message_id)
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
