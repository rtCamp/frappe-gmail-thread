import frappe
from frappe.tests import IntegrationTestCase

from frappe_gmail_thread.patches.v0_1.set_reauthorization_email_defaults import (
    execute,
)
from frappe_gmail_thread.utils.reauthorization import (
    BUILT_IN_TEMPLATE,
    EMAIL_TEMPLATE_FIELD,
    REMINDER_DAYS_FIELD,
)

DEFAULT_TEMPLATE = "Gmail Authorization Expired"


def _stored(field):
    singles = frappe.qb.DocType("Singles")
    rows = (
        frappe.qb.from_(singles)
        .select(singles.value)
        .where((singles.doctype == "Google Settings") & (singles.field == field))
        .run()
    )
    return rows[0][0] if rows else None


def _unset(*fields):
    frappe.db.delete(
        "Singles", {"doctype": "Google Settings", "field": ("in", list(fields))}
    )


class TestSetReauthorizationEmailDefaults(IntegrationTestCase):
    def test_stores_defaults_when_unset(self):
        """Sites configured before the fields existed get reminders every 3 days and the editable default template."""
        _unset(REMINDER_DAYS_FIELD, EMAIL_TEMPLATE_FIELD)
        frappe.delete_doc(
            "Email Template", DEFAULT_TEMPLATE, force=True, ignore_missing=True
        )

        execute()

        self.assertEqual(int(_stored(REMINDER_DAYS_FIELD)), 3)
        self.assertEqual(_stored(EMAIL_TEMPLATE_FIELD), DEFAULT_TEMPLATE)
        template = frappe.get_doc("Email Template", DEFAULT_TEMPLATE)
        self.assertEqual(template.reference_doctype, "Gmail Account")
        self.assertTrue(template.use_html)
        self.assertEqual(
            template.response_html,
            frappe.read_file(
                frappe.get_app_path("frappe_gmail_thread", BUILT_IN_TEMPLATE)
            ),
        )

    def test_keeps_configured_settings(self):
        """Reminders turned off (0) and a template an admin chose are left alone."""
        frappe.db.set_single_value(
            "Google Settings",
            {REMINDER_DAYS_FIELD: 0, EMAIL_TEMPLATE_FIELD: "Admin Template"},
            update_modified=False,
        )

        execute()

        self.assertEqual(int(_stored(REMINDER_DAYS_FIELD)), 0)
        self.assertEqual(_stored(EMAIL_TEMPLATE_FIELD), "Admin Template")

    def test_keeps_an_edited_default_template(self):
        """An existing template by the default name is linked as it is, not replaced."""
        _unset(EMAIL_TEMPLATE_FIELD)
        frappe.delete_doc(
            "Email Template", DEFAULT_TEMPLATE, force=True, ignore_missing=True
        )
        frappe.get_doc(
            {
                "doctype": "Email Template",
                "name": DEFAULT_TEMPLATE,
                "subject": "Edited",
                "response": "Edited",
            }
        ).insert(ignore_permissions=True)

        execute()

        self.assertEqual(_stored(EMAIL_TEMPLATE_FIELD), DEFAULT_TEMPLATE)
        self.assertEqual(
            frappe.db.get_value("Email Template", DEFAULT_TEMPLATE, "subject"), "Edited"
        )
        self.assertEqual(
            frappe.db.count("Email Template", {"name": DEFAULT_TEMPLATE}), 1
        )
