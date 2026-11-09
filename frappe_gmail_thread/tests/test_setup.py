import json
from pathlib import Path

import frappe
from frappe.tests import IntegrationTestCase

import frappe_gmail_thread.setup as setup
from frappe_gmail_thread.install import after_install
from frappe_gmail_thread.setup import MODULE, install_custom_fields

CUSTOM_FIELDS: dict[str, list[dict]] = json.loads(
    (Path(setup.__file__).parent / "custom_fields.json").read_text()
)
DOCTYPE = "Google Settings"
FIELDNAMES = [df["fieldname"] for df in CUSTOM_FIELDS[DOCTYPE]]


def _installed() -> dict[str, dict]:
    rows = frappe.get_all(
        "Custom Field",
        filters={"dt": DOCTYPE, "fieldname": ["in", FIELDNAMES]},
        fields=["fieldname", "label", "module"],
    )
    return {row.fieldname: row for row in rows}


class TestInstallCustomFields(IntegrationTestCase):
    def test_installs_every_field_with_module(self) -> None:
        """Every field in custom_fields.json exists on Google Settings, tagged with the app's module."""
        install_custom_fields()

        installed = _installed()
        self.assertCountEqual(installed, FIELDNAMES)
        for fieldname, row in installed.items():
            self.assertEqual(row.module, MODULE, fieldname)

    def test_is_idempotent(self) -> None:
        """Running it again, as every migrate does, creates no duplicate fields."""
        install_custom_fields()
        install_custom_fields()

        count = frappe.db.count(
            "Custom Field", {"dt": DOCTYPE, "fieldname": ["in", FIELDNAMES]}
        )
        self.assertEqual(count, len(FIELDNAMES))

    def test_restores_a_changed_property(self) -> None:
        """A field edited on the site is brought back to the definition in custom_fields.json."""
        install_custom_fields()
        df = CUSTOM_FIELDS[DOCTYPE][1]
        frappe.db.set_value(
            "Custom Field",
            {"dt": DOCTYPE, "fieldname": df["fieldname"]},
            "label",
            "Changed",
        )

        install_custom_fields()

        self.assertEqual(_installed()[df["fieldname"]].label, df["label"])

    def test_after_install_creates_fields(self) -> None:
        """Installing the app creates the custom fields before storing their defaults."""
        after_install()

        self.assertCountEqual(_installed(), FIELDNAMES)
