import frappe
from pypdf.errors import PyPdfError


class GmailThreadAttachmentTolerance:
    """Store unreadable PDFs that arrive as Gmail attachments, untyped."""

    def check_content(self):
        try:
            super().check_content()
        except PyPdfError:
            if self.attached_to_doctype != "Gmail Thread":
                raise

            frappe.log_error(
                title="Gmail Thread: PDF attachment stored without a file type",
                message=(
                    f"{self.file_name!r} on {self.attached_to_name}, "
                    "stored without a file type\n\n"
                    f"{frappe.get_traceback()}"
                ),
            )
            self.file_type = None
