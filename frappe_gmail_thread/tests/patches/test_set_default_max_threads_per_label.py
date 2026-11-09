import frappe
from frappe.tests import IntegrationTestCase

from frappe_gmail_thread.patches.v0_1.set_default_max_threads_per_label import (
    FIELD,
    execute,
)


def _stored():
    singles = frappe.qb.DocType("Singles")
    rows = (
        frappe.qb.from_(singles)
        .select(singles.value)
        .where((singles.doctype == "Google Settings") & (singles.field == FIELD))
        .run()
    )
    return rows[0][0] if rows else None


class TestSetDefaultMaxThreadsPerLabel(IntegrationTestCase):
    def test_stores_default_when_unset(self):
        """Sites configured before the field existed get the default stored, so the form shows 100."""
        frappe.db.delete("Singles", {"doctype": "Google Settings", "field": FIELD})
        execute()
        self.assertEqual(int(_stored()), 100)

    def test_stores_default_over_zero(self):
        """A 0 left by saving the settings form before the field had a value is replaced by the default."""
        frappe.db.set_single_value("Google Settings", FIELD, 0, update_modified=False)
        execute()
        self.assertEqual(int(_stored()), 100)

    def test_keeps_a_configured_value(self):
        """A cap an admin already set is left alone."""
        frappe.db.set_single_value("Google Settings", FIELD, 250, update_modified=False)
        execute()
        self.assertEqual(int(_stored()), 250)
