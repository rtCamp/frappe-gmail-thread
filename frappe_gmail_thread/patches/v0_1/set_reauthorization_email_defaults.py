import frappe

from frappe_gmail_thread.utils.reauthorization import (
    DEFAULT_REMINDER_DAYS,
    EMAIL_TEMPLATE_FIELD,
    REMINDER_DAYS_FIELD,
    make_default_email_template,
)


def execute():
    # A Single that already exists doesn't get new fields' defaults, and an unset Int
    # reads as 0, which turns reminders off. Read Singles directly: the fields
    # themselves are created by fixtures, synced after this. A stored 0 is a choice.
    singles = frappe.qb.DocType("Singles")
    stored = dict(
        frappe.qb.from_(singles)
        .select(singles.field, singles.value)
        .where(
            (singles.doctype == "Google Settings")
            & singles.field.isin([REMINDER_DAYS_FIELD, EMAIL_TEMPLATE_FIELD])
        )
        .run()
    )
    if REMINDER_DAYS_FIELD not in stored:
        frappe.db.set_single_value(
            "Google Settings",
            REMINDER_DAYS_FIELD,
            DEFAULT_REMINDER_DAYS,
            update_modified=False,
        )
    if not stored.get(EMAIL_TEMPLATE_FIELD):
        frappe.db.set_single_value(
            "Google Settings",
            EMAIL_TEMPLATE_FIELD,
            make_default_email_template(),
            update_modified=False,
        )
