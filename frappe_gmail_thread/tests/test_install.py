import frappe
from frappe.tests import IntegrationTestCase

from frappe_gmail_thread.install import after_install

FIELDS = {
    "custom_gmail_max_threads_per_label": 100,
    "custom_gmail_reauthorization_reminder_days": 3,
    "custom_gmail_reauthorization_email_template": "Gmail Authorization Expired",
}


class TestAfterInstall(IntegrationTestCase):
    def test_stores_setting_defaults_on_existing_google_settings(self):
        """Installing on a site whose Google Settings already exist stores the Gmail settings' defaults."""
        frappe.db.delete(
            "Singles", {"doctype": "Google Settings", "field": ("in", list(FIELDS))}
        )

        after_install()

        for field, default in FIELDS.items():
            self.assertEqual(
                frappe.db.get_single_value("Google Settings", field), default
            )
