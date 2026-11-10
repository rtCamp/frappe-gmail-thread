import json
from pathlib import Path

from frappe.custom.doctype.custom_field.custom_field import create_custom_fields

MODULE = "Frappe Gmail Thread"


def install() -> None:
    """Apply all Gmail Thread customisations (idempotent; runs on migrate)."""
    install_custom_fields()


def install_custom_fields() -> None:
    """Install / refresh custom fields from custom_fields.json."""
    data_file = Path(__file__).parent / "custom_fields.json"
    # Each entry declares "module" so /desk/custom-field shows which app owns it.
    custom_fields: dict[str, list[dict]] = json.loads(data_file.read_text())
    create_custom_fields(custom_fields, update=True)
