import base64
import json
import re
from uuid import uuid4

import frappe
from bs4 import BeautifulSoup
from frappe.email.receive import Email, MaxFileSizeReachedError
from frappe.utils import extract_email_id, get_string_between, sanitize_html


class GmailInboundMail(Email):
    def __init__(self, content, email_account=None):
        # temp compatibility with frappe.email.receive.Email
        self.email_account = email_account or frappe._dict()
        if not hasattr(self.email_account, "attachment_limit"):
            self.email_account.attachment_limit = None

        super().__init__(content)
        self.text_content = self.pop_down_quoted_replies(self.text_content, "text")
        self.html_content = self.pop_down_quoted_replies(self.html_content, "html")
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

    # Canonical Gmail quote style applied to all normalised quote containers
    _GMAIL_QUOTE_STYLE = "margin:0px 0px 0px 0.8ex;border-left:1px solid rgb(204,204,204);padding-left:1ex"

    def pop_down_quoted_replies(self, content, type):
        if type == "text":
            regex = r"(\n|^)(On(.|\n)*?wrote:)((.|\n)*)"
            return re.sub(regex, "", content)
        if type == "html":
            if not content:
                return content

            soup = BeautifulSoup(content, "html.parser")

            # Normalise every client's quote element to
            # <div class="gmail_quote" style="..."> so the frontend renders
            # them all identically with a left-border just like Gmail.
            #
            # Clients and their quote structures:
            #   Gmail                    → blockquote.gmail_quote  (already correct, rewrap as div)
            #   Gmail container          → div.gmail_quote_container (keep as-is, children handled)
            #   Apple Mail/Thunderbird   → blockquote[type="cite"]
            #   Outlook                  → div#divRplyFwdMsg, div.OutlookMessageHeader
            #   Yahoo Mail               → div.yahoo_quoted
            #   Frappe outgoing          → plain <blockquote style="...">

            QUOTE_SELECTORS = [
                "blockquote[type='cite']",
                "div#divRplyFwdMsg",
                "div.OutlookMessageHeader",
                "div.yahoo_quoted",
            ]

            for selector in QUOTE_SELECTORS:
                for node in soup.select(selector):
                    new_div = soup.new_tag(
                        "div",
                        **{"class": "gmail_quote", "style": self._GMAIL_QUOTE_STYLE},
                    )
                    children = list(node.children)
                    for child in children:
                        new_div.append(child)
                    node.replace_with(new_div)

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


def get_conversation_root(email_object, fallback=None):
    references = email_object.mail.get("References")
    if references:
        for token in references.split():
            message_id = get_string_between("<", token, ">")
            if message_id:
                return message_id
    in_reply_to = email_object.mail.get("In-Reply-To")
    if in_reply_to:
        message_id = get_string_between("<", in_reply_to, ">")
        if message_id:
            return message_id
    if email_object.message_id:
        return email_object.message_id
    return fallback


def find_gmail_thread(thread_id, conversation_root=None):
    name = frappe.db.get_value("Gmail Thread", {"gmail_thread_id": thread_id})
    if not name:
        name = frappe.db.get_value(
            "Gmail Thread Mapped ID", {"gmail_thread_id": thread_id}, "parent"
        )
    if not name and conversation_root:
        name = frappe.db.get_value(
            "Gmail Thread", {"conversation_root": conversation_root}
        )
    if name:
        return frappe.get_doc("Gmail Thread", name)
    return None


def register_thread_id(gmail_thread, thread_id):
    if not thread_id or thread_id == gmail_thread.gmail_thread_id:
        return
    if any(m.gmail_thread_id == thread_id for m in gmail_thread.gmail_thread_ids):
        return
    gmail_thread.append("gmail_thread_ids", {"gmail_thread_id": thread_id})


def _is_duplicate_entry(exc):
    if isinstance(
        exc,
        (
            frappe.exceptions.UniqueValidationError,
            frappe.exceptions.DuplicateEntryError,
        ),
    ):
        return True
    return "Duplicate entry" in str(exc)


def claim_thread(thread_id, conversation_root, gmail_account):
    savepoint = "gthread_claim"
    frappe.db.savepoint(savepoint)
    try:
        thread = frappe.new_doc("Gmail Thread")
        thread.gmail_thread_id = thread_id
        thread.conversation_root = conversation_root
        thread.gmail_account = gmail_account.name
        thread.insert(ignore_permissions=True)
        return thread, True
    except Exception as exc:
        if not _is_duplicate_entry(exc):
            raise
        frappe.db.rollback(save_point=savepoint)

    name = frappe.db.get_value(
        "Gmail Thread", {"gmail_thread_id": thread_id}, "name", for_update=True
    )
    if not name and conversation_root:
        name = frappe.db.get_value(
            "Gmail Thread",
            {"conversation_root": conversation_root},
            "name",
            for_update=True,
        )
    if not name:
        frappe.throw(
            "Failed to resolve Gmail Thread during concurrent create. "
            f"thread_id={thread_id}, conversation_root={conversation_root}"
        )
    return frappe.get_doc("Gmail Thread", name), False


def reconcile_thread(gmail_thread, thread_id, conversation_root):
    gmail_thread.reload()
    if (
        conversation_root
        and gmail_thread.conversation_root
        and gmail_thread.conversation_root != conversation_root
    ):
        owner = frappe.db.get_value(
            "Gmail Thread", {"conversation_root": conversation_root}, "name"
        )
        if owner and owner != gmail_thread.name:
            surviving = merge_gmail_threads(gmail_thread.name, owner)
            gmail_thread = frappe.get_doc("Gmail Thread", surviving)
    elif conversation_root and not gmail_thread.conversation_root:
        gmail_thread.conversation_root = conversation_root

    register_thread_id(gmail_thread, thread_id)
    return gmail_thread


def attach_message_to_thread(gmail_account, thread_id, email, email_object):
    conversation_root = get_conversation_root(email_object, fallback=thread_id)
    email.conversation_root = conversation_root

    gmail_thread = find_gmail_thread(thread_id, conversation_root)
    if gmail_thread:
        return reconcile_thread(gmail_thread, thread_id, conversation_root), False

    gmail_thread, created = claim_thread(thread_id, conversation_root, gmail_account)
    if created:
        return gmail_thread, True
    return reconcile_thread(gmail_thread, thread_id, conversation_root), False


class AlreadyExistsError(Exception):
    def __init__(self, thread_name=None, *args):
        self.thread_name = thread_name
        super().__init__(*args)


def merge_gmail_threads(name_a, name_b):
    if not name_a or not name_b or name_a == name_b:
        return name_a or name_b

    # Lock both rows in a stable order so concurrent syncs can't double-merge.
    for name in sorted([name_a, name_b]):
        frappe.db.get_value("Gmail Thread", name, "name", for_update=True)

    a = frappe.get_doc("Gmail Thread", name_a)
    b = frappe.get_doc("Gmail Thread", name_b)
    if a.creation is None or (b.creation and b.creation < a.creation):
        primary, duplicate = b, a
    else:
        primary, duplicate = a, b

    frappe.db.sql(
        """update `tabSingle Email CT`
           set parent = %s, parenttype = 'Gmail Thread', parentfield = 'emails'
           where parent = %s and parenttype = 'Gmail Thread'
             and parentfield = 'emails'""",
        (primary.name, duplicate.name),
    )
    frappe.db.sql(
        """update `tabFile`
           set attached_to_name = %s
           where attached_to_doctype = 'Gmail Thread'
             and attached_to_name = %s""",
        (primary.name, duplicate.name),
    )

    primary.reload()
    duplicate.reload()

    have_users = {u.account for u in primary.involved_users}
    for user in duplicate.involved_users:
        if user.account and user.account not in have_users:
            primary.append("involved_users", {"account": user.account})
            have_users.add(user.account)

    known_ids = {primary.gmail_thread_id} | {
        m.gmail_thread_id for m in primary.gmail_thread_ids
    }
    for tid in [duplicate.gmail_thread_id] + [
        m.gmail_thread_id for m in duplicate.gmail_thread_ids
    ]:
        if tid and tid not in known_ids:
            primary.append("gmail_thread_ids", {"gmail_thread_id": tid})
            known_ids.add(tid)

    if not (primary.reference_doctype and primary.reference_name) and (
        duplicate.reference_doctype and duplicate.reference_name
    ):
        primary.reference_doctype = duplicate.reference_doctype
        primary.reference_name = duplicate.reference_name
        if primary.status == "Open":
            primary.status = "Linked"

    primary.emails.sort(key=lambda e: e.date_and_time or primary.creation)
    for idx, child in enumerate(primary.emails, start=1):
        child.idx = idx
    if primary.emails:
        primary.subject_of_first_mail = primary.emails[0].subject

    earliest = min(
        [c for c in [primary.creation, duplicate.creation] if c],
        default=primary.creation,
    )

    frappe.delete_doc(
        "Gmail Thread",
        duplicate.name,
        ignore_permissions=True,
        force=True,
        delete_permanently=True,
    )
    primary.save(ignore_permissions=True)
    if earliest and earliest != primary.creation:
        frappe.db.set_value(
            "Gmail Thread", primary.name, "creation", earliest, update_modified=False
        )
    return primary.name


def create_new_email(email, gmail_account):
    # decode raw email with errors='replace' to avoid UnicodeDecodeError
    email_content = base64.urlsafe_b64decode(email["raw"].encode("ASCII")).decode(
        "utf-8", errors="replace"
    )
    email_object = GmailInboundMail(content=email_content, email_account=gmail_account)
    is_sent = bool(
        frappe.db.exists(
            "User",
            {"email": email_object.from_email, "user_type": ["!=", "Website User"]},
        )
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

            raise AlreadyExistsError(gmail_thread.name)
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


@frappe.whitelist()
def backfill_conversation_roots(merge: bool | str = True):
    from collections import defaultdict

    from frappe_gmail_thread.api.oauth import get_gmail_object

    frappe.only_for("System Manager")
    merge = frappe.parse_json(merge) if isinstance(merge, str) else merge

    gmail_objects = {}

    def gmail_for(account_name):
        if account_name not in gmail_objects:
            account = frappe.get_doc("Gmail Account", account_name)
            gmail_objects[account_name] = (account, get_gmail_object(account))
        return gmail_objects[account_name]

    emails = frappe.get_all(
        "Single Email CT",
        filters={"parenttype": "Gmail Thread", "parentfield": "emails"},
        fields=[
            "name",
            "parent",
            "gmail_message_id",
            "gmail_account",
            "date_and_time",
            "conversation_root",
        ],
    )

    thread_root = {}  # thread name -> (root, earliest date seen)
    for email in emails:
        root = email.conversation_root
        if not root:
            try:
                account, gmail = gmail_for(email.gmail_account)
                raw = (
                    gmail.users()
                    .messages()
                    .get(userId="me", id=email.gmail_message_id, format="raw")
                    .execute()
                )
                content = base64.urlsafe_b64decode(raw["raw"].encode("ASCII")).decode(
                    "utf-8", errors="replace"
                )
                email_object = GmailInboundMail(content=content, email_account=account)
                root = get_conversation_root(email_object, fallback=email.parent)
                frappe.db.set_value(
                    "Single Email CT",
                    email.name,
                    "conversation_root",
                    root,
                    update_modified=False,
                )
            except Exception:
                frappe.log_error(frappe.get_traceback(), "Gmail Thread Backfill Error")
                continue

        previous = thread_root.get(email.parent)
        if previous is None or (
            email.date_and_time
            and (previous[1] is None or email.date_and_time < previous[1])
        ):
            thread_root[email.parent] = (root, email.date_and_time)
    frappe.db.commit()  # nosemgrep

    groups = defaultdict(list)
    for thread_name, (root, _) in thread_root.items():
        if root:
            groups[root].append(thread_name)

    for root, names in groups.items():
        ordered = frappe.get_all(
            "Gmail Thread",
            filters={"name": ["in", names]},
            order_by="creation asc",
            pluck="name",
        )
        if not ordered:
            continue
        survivor = ordered[0]
        if merge:
            for other in ordered[1:]:
                try:
                    survivor = merge_gmail_threads(survivor, other)
                except Exception:
                    frappe.log_error(
                        frappe.get_traceback(), "Gmail Thread Backfill Merge Error"
                    )
        try:
            owner = frappe.db.get_value(
                "Gmail Thread", {"conversation_root": root}, "name"
            )
            if owner and owner != survivor:
                survivor = merge_gmail_threads(survivor, owner)
            elif not frappe.db.get_value("Gmail Thread", survivor, "conversation_root"):
                frappe.db.set_value(
                    "Gmail Thread",
                    survivor,
                    "conversation_root",
                    root,
                    update_modified=False,
                )
        except Exception:
            frappe.log_error(frappe.get_traceback(), "Gmail Thread Backfill Root Error")
        frappe.db.commit()  # nosemgrep
