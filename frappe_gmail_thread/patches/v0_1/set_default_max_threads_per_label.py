import frappe
from frappe.utils import cint

from frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread import (
    DEFAULT_MAX_THREADS_PER_LABEL,
)

FIELD = "custom_gmail_max_threads_per_label"


def execute():
    # A Single that already exists doesn't get a new field's default, and saving
    # it stores 0, so the form would show 0 while the sync uses the default. Read
    # Singles directly: the field itself is created by fixtures, synced after this.
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
