import base64
import json
import re
from uuid import uuid4

import frappe
from bs4 import BeautifulSoup
from frappe.email.receive import Email, MaxFileSizeReachedError
from frappe.utils import extract_email_id, sanitize_html


class GmailInboundMail(Email):
    def __init__(self, content):
        super().__init__(content)
        # remove quoted replies from email text content
        self.text_content = self.remove_quoted_replies(self.text_content, "text")
        self.html_content = self.remove_quoted_replies(self.html_content, "html")
        self.set_content_and_type()
        self.set_to_and_cc()

    def replace_inline_images(self, attachments):
        # replace inline images
        content = self.content
        for file in json.loads(attachments):
            file = frappe.get_doc("File", file["file_doc_name"])
            if self.cid_map.get(file.name):
                content = content.replace(
                    f"cid:{self.cid_map[file.name]}", file.unique_url
                )
        return content

    def remove_quoted_replies(self, content, type):
        if type == "text":
            regex = r"(\n|^)(On(.|\n)*?wrote:)((.|\n)*)"
            return re.sub(regex, "", content)
        if type == "html":
            soup = BeautifulSoup(content, "html.parser")
            # only works for gmail
            for div in soup.find_all("div", class_="gmail_quote"):
                div.decompose()
            return str(soup)

    def set_to_and_cc(self):
        """
        Set the to, cc and bcc fields from the email content.
        """
        _to_email = self.mail.get("To")
        _cc_email = self.mail.get("Cc")
        _bcc_email = self.mail.get("Bcc")
        self.to = self.get_email_list(_to_email)
        self.cc = self.get_email_list(_cc_email)
        self.bcc = self.get_email_list(_bcc_email)

    def get_email_list(self, email):
        if email:
            return [extract_email_id(e) for e in email.split(",")]
        return []


def html_to_text(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(separator=" ", strip=True)


def find_gmail_thread(thread_id, message_ids: list = None):
    try:
        gmail_thread = frappe.get_doc("Gmail Thread", {"gmail_thread_id": thread_id})
    except frappe.DoesNotExistError:
        gmail_thread = None
        if message_ids:
            for message_id in message_ids:
                try:
                    single_email_ct = frappe.get_doc(
                        "Single Email CT", {"email_message_id": message_id}
                    )
                    if single_email_ct:
                        gmail_thread = frappe.get_doc(
                            "Gmail Thread", single_email_ct.parent
                        )
                        break
                except frappe.DoesNotExistError:
                    pass
    return gmail_thread


class AlreadyExistsError(Exception):
    pass


def create_new_email(email, gmail_account):
    # decode raw email with errors='replace' to avoid UnicodeDecodeError
    email_content = base64.urlsafe_b64decode(email["raw"].encode("ASCII")).decode(
        "utf-8", errors="replace"
    )
    email_object = GmailInboundMail(content=email_content)
    # check if email is sent or received
    is_sent = False
    # check if there is a user (not website user) with the same email as the sender in frappe, if yes, then it is a sent email
    is_sent = (
        frappe.db.exists(
            "User",
            {"email": email_object.from_email, "user_type": ["!=", "Website User"]},
        )
        and True
        or False
    )

    try:
        email_ct = frappe.get_doc(
            "Single Email CT", {"email_message_id": email_object.message_id}
        )
        if email_ct:
            gmail_thread = frappe.get_doc("Gmail Thread", email_ct.parent)
            involved_users_linked = [
                user.account for user in gmail_thread.involved_users
            ]

            if gmail_account.linked_user not in involved_users_linked:
                involved_user = frappe.get_doc(
                    doctype="Involved User", account=gmail_account.linked_user
                )
                gmail_thread.append("involved_users", involved_user)
                gmail_thread.save(ignore_permissions=True)

            raise AlreadyExistsError
    except frappe.DoesNotExistError:
        pass

    def safe_str(val):
        if val is None:
            return ""
        if isinstance(val, bytes):
            return val.decode("utf-8", errors="replace")
        if isinstance(val, str):
            return val.encode("utf-8", errors="replace").decode("utf-8")
        return str(val)

    new_email = frappe.new_doc("Single Email CT")
    new_email.gmail_message_id = safe_str(email["id"])
    new_email.subject = safe_str(email_object.subject)
    new_email.sender = safe_str(email_object.from_email)
    new_email.recipients = safe_str(", ".join(email_object.to).strip())
    new_email.cc = safe_str(", ".join(email_object.cc).strip())
    new_email.bcc = safe_str(", ".join(email_object.bcc).strip())
    new_email.content = safe_str(email_object.content)
    new_email.plain_content = safe_str(
        email_object.text_content.strip() or html_to_text(email_object.html_content)
    )
    new_email.date_and_time = email_object.date
    new_email.sender_full_name = safe_str(email_object.from_real_name)
    new_email.read_receipt = False
    new_email.read_by_recipient = False
    new_email.read_by_recipient_on = None
    new_email.gmail_account = gmail_account.name
    new_email.email_status = "Open"
    new_email.email_message_id = safe_str(email_object.message_id)
    new_email.linked_communication = None
    new_email.sent_or_received = "Sent" if is_sent else "Received"
    # save attachments to private files
    # new_email.attachments_data_html = """ # TODO: Make it work
    # <table>
    #     <thead>
    #         <tr>
    #             <th>File Name</th>
    #             <th>URL</th>
    #         </tr>
    #     </thead>
    #     <tbody>
    #         {0}
    #     </tbody>
    # """.format(
    #     "".join(["<tr><td>{0}</td><td><a href='{1}'>Open</a></td></tr>".format(attachment["file_name"], attachment["file_url"]) for attachment in attachments])
    # )
    # set email creation date to the date of the email
    new_email.creation = new_email.date_and_time
    return new_email, email_object


def replace_inline_images(new_email, email_object):
    if new_email.attachments_data:
        new_email.content = sanitize_html(
            email_object.replace_inline_images(new_email.attachments_data)
        )


def process_attachments(new_email, gmail_thread, email_object):
    attachments = []
    for attachment in email_object.attachments:
        try:
            attachment["mapped_name"] = attachment["fname"]
            if len(attachment["fname"]) >= 140:
                attachment["mapped_name"] = (
                    str(uuid4()) + "." + attachment["fname"].split(".")[-1]
                )
            _file = frappe.get_doc(
                {
                    "doctype": "File",
                    "file_name": attachment["mapped_name"],
                    "attached_to_doctype": "Gmail Thread",
                    "attached_to_name": gmail_thread.name
                    or gmail_thread.gmail_thread_id,
                    "is_private": 1,
                    "content": attachment["fcontent"],
                }
            )
            _file.save()
            attachments.append(
                {
                    "file_name": _file.file_name,
                    "file_doc_name": _file.name,
                    "is_private": _file.is_private,
                }
            )

            if attachment["fname"] in email_object.cid_map:
                email_object.cid_map[_file.name] = email_object.cid_map[
                    attachment["fname"]
                ]

        except MaxFileSizeReachedError:
            # WARNING: bypass max file size exception
            pass
        except frappe.FileAlreadyAttachedException:
            pass
        except frappe.DuplicateEntryError:
            # same file attached twice??
            pass
    new_email.attachments_data = json.dumps(attachments)
