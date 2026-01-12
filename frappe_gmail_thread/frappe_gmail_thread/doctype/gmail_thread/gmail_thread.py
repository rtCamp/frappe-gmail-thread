# Copyright (c) 2024, rtCamp and contributors
# For license information, please see license.txt


import frappe
import frappe.share
import googleapiclient.errors
from frappe import _
from frappe.model.document import Document
from frappe.utils import get_string_between

from frappe_gmail_thread.api.oauth import get_gmail_object
from frappe_gmail_thread.utils.helpers import (
    AlreadyExistsError,
    create_new_email,
    find_gmail_thread,
    process_attachments,
    replace_inline_images,
)

SCOPES = "https://www.googleapis.com/auth/gmail.readonly"
BATCH_COMMIT_SIZE = 20  # Commit every N emails for performance optimization


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


@frappe.whitelist(methods=["POST"])
def sync_labels(account_name, should_save=True):
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
        frappe.set_user(user)
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
    emails_processed = 0  # Track emails for batch commits

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
                        # Track max history id
                        msg_history_id = int(message.get("historyId", 0))
                        if msg_history_id > max_history_id:
                            max_history_id = msg_history_id
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
                            gmail_thread.reload()
                        else:
                            gmail_thread = frappe.new_doc("Gmail Thread")
                            gmail_thread.gmail_thread_id = thread_id
                            gmail_thread.gmail_account = gmail_account.name
                            is_new_thread = True
                        if not gmail_thread.subject_of_first_mail:
                            gmail_thread.subject_of_first_mail = email.subject
                            gmail_thread.creation = email.date_and_time
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
                        gmail_thread.append("emails", email)
                        gmail_thread.save(ignore_permissions=True)
                        emails_processed += 1

                        # Batch commit every BATCH_COMMIT_SIZE emails
                        if emails_processed % BATCH_COMMIT_SIZE == 0:
                            frappe.db.commit()
                            # Update history ID periodically for crash recovery
                            gmail_account.reload()
                            gmail_account.last_historyid = max_history_id
                            gmail_account.save(ignore_permissions=True)
                            frappe.db.commit()

                        frappe.db.set_value(
                            "Gmail Thread",
                            gmail_thread.name,
                            "modified",
                            email.date_and_time,
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
                    # If notFound, update historyid to the value returned by API (if any)
                    # You won't find history id in error, so just reset to 0 and let next sync do initial sync
                    if hasattr(e, "error_details"):
                        for error in e.error_details:
                            if error.get("reason") == "notFound":
                                gmail_account.last_historyid = 0
                                gmail_account.save(ignore_permissions=True)
                                frappe.db.commit()
                                return
                    raise e

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
                            if not gmail_thread:
                                gmail_thread = frappe.new_doc("Gmail Thread")
                                gmail_thread.gmail_thread_id = thread_id
                                gmail_thread.gmail_account = gmail_account.name
                                is_new_thread = True
                            if not gmail_thread.subject_of_first_mail:
                                gmail_thread.subject_of_first_mail = email.subject
                                gmail_thread.creation = email.date_and_time
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
                            gmail_thread.append("emails", email)
                            gmail_thread.save(ignore_permissions=True)
                            emails_processed += 1

                            # Batch commit every BATCH_COMMIT_SIZE emails
                            if emails_processed % BATCH_COMMIT_SIZE == 0:
                                frappe.db.commit()
                                # Update history ID periodically for crash recovery
                                gmail_account.reload()
                                gmail_account.last_historyid = max_history_id
                                gmail_account.save(ignore_permissions=True)
                                frappe.db.commit()

                            frappe.db.set_value(
                                "Gmail Thread",
                                gmail_thread.name,
                                "modified",
                                email.date_and_time,
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
        except Exception:
            frappe.db.rollback()
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
