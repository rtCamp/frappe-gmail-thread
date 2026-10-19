# Copyright (c) 2024, rtCamp and contributors
# For license information, please see license.txt


from email.utils import getaddresses

import frappe
import frappe.share
import googleapiclient.errors
from frappe import _
from frappe.model.document import Document
from frappe.permissions import get_doctypes_with_read
from frappe.utils import get_datetime, get_string_between

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
        gmail.users().labels().get(userId="me", id=label_id).execute()
    except googleapiclient.errors.HttpError as e:
        if is_not_found_error(e):
            return False
        raise
    return True


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
    account_name = frappe.db.get_value("Gmail Account", {"linked_user": user})
    if not account_name:
        return
    gmail_account = frappe.get_doc("Gmail Account", account_name)
    # the account can be disabled or lose its authorization while this job is queued
    if not gmail_account.gmail_enabled or not gmail_account.refresh_token:
        return
    try:
        gmail = get_gmail_object(gmail_account)
    except GmailAuthorizationExpiredError:
        handle_expired_authorization(gmail_account)
        return
    label_ids = [x.label_id for x in gmail_account.labels if x.enabled]
    if not label_ids:
        return

    # Always store the maximum history id seen, to avoid skipping emails
    last_history_id = int(gmail_account.last_historyid or 0)
    max_history_id = last_history_id

    for label_id in label_ids:
        try:
            if not last_history_id:
                # Initial sync: fetch all threads for the label
                threads = (
                    gmail.users()
                    .threads()
                    .list(userId="me", labelIds=label_id)
                    .execute()
                )
                if "threads" not in threads:
                    continue
                for thread in threads["threads"][::-1]:
                    thread_id = thread["id"]
                    thread_data = (
                        gmail.users().threads().get(userId="me", id=thread_id).execute()
                    )
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
                                frappe.db.commit()  # nosemgrep
                            continue
                        try:
                            raw_email = (
                                gmail.users()
                                .messages()
                                .get(userId="me", id=message["id"], format="raw")
                                .execute()
                            )
                        except googleapiclient.errors.HttpError as e:
                            if hasattr(e, "error_details"):
                                for error in e.error_details:
                                    if error.get("reason") == "notFound":
                                        break
                            else:
                                raise e
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
                        frappe.db.set_value(
                            "Gmail Thread",
                            gmail_thread.name,
                            "owner",
                            gmail_account.linked_user,
                            modified_by=gmail_account.linked_user,
                            update_modified=False,
                        )
                        frappe.db.commit()  # nosemgrep
                gmail_account.reload()
                gmail_account.last_historyid = max_history_id
                gmail_account.save(ignore_permissions=True)
                frappe.db.commit()  # nosemgrep
            else:
                # Incremental sync using history API
                try:
                    history = (
                        gmail.users()
                        .history()
                        .list(
                            userId="me",
                            startHistoryId=last_history_id,
                            historyTypes=["messageAdded", "labelAdded"],
                            labelId=label_id,
                        )
                        .execute()
                    )
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

                new_history_id = int(history.get("historyId", last_history_id))
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
                                    .execute()
                                )
                            except googleapiclient.errors.HttpError as e:
                                if hasattr(e, "error_details"):
                                    for error in e.error_details:
                                        if error.get("reason") == "notFound":
                                            break
                                else:
                                    raise e
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
        except Exception:
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
