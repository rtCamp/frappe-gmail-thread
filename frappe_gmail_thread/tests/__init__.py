"""Tests setup and helpers."""

import contextlib

import frappe
from frappe.utils.password import remove_encrypted_password, set_encrypted_password

TEST_USER = "test_fgt_user1@example.com"
TEST_USER_2 = "test_fgt_user2@example.com"
TEST_USER_3 = "test_fgt_user3@example.com"


def ensure_doc(doctype, name, **fields):
    """
    Return the existing doc with this name, or insert one and return it.
    Caller is responsible for picking a doctype whose autoname respects the supplied name.
    """
    if frappe.db.exists(doctype, name):
        return frappe.get_doc(doctype, name)
    return frappe.get_doc({"doctype": doctype, "name": name, **fields}).insert(
        ignore_permissions=True
    )


def set_password_field(doctype, name, fieldname, value):
    """Set or clear a Password field via the encrypted-storage helpers.

    Frappe stores Password fields in two places: the encrypted `__Auth` table
    (read by `doc.get_password`) and the doctype column (read by attribute
    access `doc.<fieldname>`, which source code uses for truthy/falsy gates).
    This helper writes both so the field stays consistent. Passing an empty
    string or None clears both sides.
    """
    if value:
        set_encrypted_password(doctype, name, value, fieldname)
    else:
        remove_encrypted_password(doctype, name, fieldname)
    frappe.db.set_value(doctype, name, fieldname, value or "", update_modified=False)


def make_test_user(email, user_type="System User"):
    """Create a User with no welcome email. user_type controls Website User vs System User (the latter is what most FGT tests need)."""
    if frappe.db.exists("User", email):
        return frappe.get_doc("User", email)
    return frappe.get_doc(
        {
            "doctype": "User",
            "email": email,
            "first_name": email.split("@")[0],
            "send_welcome_email": 0,
            "user_type": user_type,
        }
    ).insert(ignore_permissions=True)


def make_test_gmail_account(
    *,
    linked_user,
    gmail_enabled=0,
    refresh_token=None,
    last_historyid=0,
    labels=None,
):
    """
    Insert (or return) a Gmail Account for the given linked_user. Gmail Account uses linked_user as the unique identifier in tests; before_insert ties it to frappe.session.user, so callers should be operating under `as_user(linked_user)` when creating.
    labels is a list of dicts like [{"label_id": "INBOX", "label_name": "INBOX", "enabled": 1}, ...].
    """
    existing = frappe.db.get_value("Gmail Account", {"linked_user": linked_user})
    if existing:
        return frappe.get_doc("Gmail Account", existing)

    data = {
        "doctype": "Gmail Account",
        "gmail_enabled": gmail_enabled,
        "last_historyid": last_historyid,
    }
    if labels:
        data["labels"] = labels
    doc = frappe.get_doc(data)
    doc.flags.ignore_validate = True
    doc.insert(ignore_permissions=True)
    # refresh_token is a Password field; route through encrypted storage so doc.get_password reads
    # back what we set. Done after insert to avoid validate-time pubsub side-effects.
    if refresh_token:
        set_password_field("Gmail Account", doc.name, "refresh_token", refresh_token)
        doc.reload()
    return doc


def make_test_gmail_thread(
    *,
    gmail_account,
    gmail_thread_id,
    owner=None,
    reference_doctype=None,
    reference_name=None,
    status="Open",
    subject_of_first_mail=None,
    involved_users=None,
    emails=None,
):
    """
    Insert a Gmail Thread with the supplied child rows.
    involved_users is a list of user email strings; each becomes an Involved User child row.
    emails is a list of dicts to append as Single Email CT rows (caller specifies the fields).
    """
    data = {
        "doctype": "Gmail Thread",
        "gmail_account": gmail_account,
        "gmail_thread_id": gmail_thread_id,
        "status": status,
    }
    if reference_doctype is not None:
        data["reference_doctype"] = reference_doctype
    if reference_name is not None:
        data["reference_name"] = reference_name
    if subject_of_first_mail is not None:
        data["subject_of_first_mail"] = subject_of_first_mail
    if involved_users:
        data["involved_users"] = [{"account": u} for u in involved_users]
    if emails:
        data["emails"] = emails
    doc = frappe.get_doc(data)
    doc.flags.ignore_validate = True
    doc.insert(ignore_permissions=True)
    if owner:
        frappe.db.set_value(
            "Gmail Thread",
            doc.name,
            "owner",
            owner,
            modified_by=owner,
            update_modified=False,
        )
        doc.reload()
    return doc


@contextlib.contextmanager
def as_user(email):
    """Temporarily switch frappe.session.user for the duration of the block."""
    original = frappe.session.user
    frappe.set_user(email)
    try:
        yield
    finally:
        frappe.set_user(original)
