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
    custom_fields: dict[str, list[dict]] = json.loads(data_file.read_text())
    # Owner tag so /desk/custom-field shows which app created these fields.
    for fields in custom_fields.values():
        for df in fields:
            df.setdefault("module", MODULE)
    create_custom_fields(custom_fields, update=True)
