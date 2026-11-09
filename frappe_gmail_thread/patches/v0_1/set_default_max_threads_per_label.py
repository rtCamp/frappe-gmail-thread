import frappe
from frappe.utils import cint

from frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread import (
    DEFAULT_MAX_THREADS_PER_LABEL,
)
from frappe_gmail_thread.setup import install_custom_fields

FIELD = "custom_gmail_max_threads_per_label"


def execute() -> None:
    # Patch may run before `after_migrate` installs the custom field on a
    # fresh deploy — install it first so the field exists.
    install_custom_fields()

    # A Single that already exists doesn't get a new field's default, and saving
    # it stores 0, so the form would show 0 while the sync uses the default. Read
    # Singles directly to tell an unset value from a stored one.
    singles = frappe.qb.DocType("Singles")
    stored = (
        frappe.qb.from_(singles)
        .select(singles.value)
        .where((singles.doctype == "Google Settings") & (singles.field == FIELD))
        .run()
    )
    if not stored or not cint(stored[0][0]):
        frappe.db.set_single_value(
            "Google Settings",
            FIELD,
            DEFAULT_MAX_THREADS_PER_LABEL,
            update_modified=False,
        )
