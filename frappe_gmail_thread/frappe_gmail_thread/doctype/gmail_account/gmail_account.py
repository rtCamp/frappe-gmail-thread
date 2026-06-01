# Copyright (c) 2024, rtCamp and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils.background_jobs import is_job_enqueued

from frappe_gmail_thread.api.oauth import disable_pubsub, enable_pubsub
from frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread import (
    sync_labels,
)


class GmailAccount(Document):
    def before_insert(self):
        self.linked_user = frappe.session.user
        self.email_id = frappe.get_value("User", self.linked_user, "email")

    def on_trash(self):
        if not self.gmail_enabled:
            return
        if not self.refresh_token:
            return
        google_settings = frappe.get_single("Google Settings")
        if not google_settings.custom_gmail_pubsub_topic:
            return
        disable_pubsub(self)

    def validate(self):
        if self.gmail_enabled:
            google_settings = frappe.get_single("Google Settings")
            if not google_settings.enable:
                frappe.throw(_("Enable Google API in Google Settings."))
            if not google_settings.client_id or not google_settings.get_password(
                fieldname="client_secret", raise_exception=False
            ):
                frappe.throw(
                    _(
                        "Please set Client ID and Client Secret in Google Settings to enable Gmail"
                    )
                )

    def has_value_changed(self, fieldname):
        # check if fieldname is child table
        if fieldname in ["labels"]:
            old_value = self.get_doc_before_save()
            if old_value:
                old_value = old_value.get(fieldname)
            new_value = self.get(fieldname)
            if old_value and new_value:
                if len(old_value) != len(new_value):
                    return True
                old_names = [(d.name, d.enabled) for d in old_value]
                new_names = [(d.name, d.enabled) for d in new_value]
                if set(old_names) != set(new_names):
                    return True
                return False
            if not old_value and not new_value:
                return False
            return True
        return super().has_value_changed(fieldname)

    def before_save(self):
        if self.has_value_changed("gmail_enabled") and self.gmail_enabled:
            google_settings = frappe.get_single("Google Settings")
            if not google_settings.enable:
                frappe.throw(_("Enable Google API in Google Settings."))
            if not google_settings.client_id or not google_settings.get_password(
                fieldname="client_secret", raise_exception=False
            ):
                frappe.throw(
                    _(
                        "Please set Client ID and Client Secret in Google Settings to enable Gmail"
                    )
                )
        if self.has_value_changed("refresh_token") and self.refresh_token:
            sync_labels(self, should_save=False)
            google_settings = frappe.get_single("Google Settings")
            if (
                google_settings.custom_gmail_sync_in_realtime
                and google_settings.custom_gmail_pubsub_topic
            ):
                if self.gmail_enabled:
                    # start pubsub
                    enable_pubsub(self)
                    frappe.msgprint(
                        _("Enabled Realtime Sync for {0}").format(self.linked_user)
                    )
                else:
                    # stop pubsub
                    disable_pubsub(self)
                    frappe.msgprint(
                        _("Disabled Realtime Sync for {0}").format(self.linked_user)
                    )
        if self.has_value_changed("labels"):
            self.last_historyid = 0  # reset history id if labels are changed

            if self.gmail_enabled and self.refresh_token:
                has_labels = False
                for label in self.labels:
                    if label.enabled:
                        has_labels = True
                        break
                if has_labels:
                    frappe.msgprint(
                        _(
                            "The following labels will be synced in the background. Please confirm if you want to proceed:<br><br> - {0}"
                        ).format(
                            "<br> - ".join(
                                [
                                    label.label_name
                                    for label in self.labels
                                    if label.enabled
                                ]
                            )
                        ),
                        "Confirm Sync",
                        primary_action={
                            "label": _("Proceed"),
                            "server_action": "frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_account.gmail_account.sync_labels_api",
                            "args": {"doc_name": self.name},
                            "hide_on_success": True,
                        },
                    )
                    enable_pubsub(self)
                else:
                    frappe.msgprint(_("Please select at least one label."))


@frappe.whitelist()  # nosemgrep
def sync_labels_api(args: str | dict | None = None, doc_name: str | None = None):
    if args:
        if isinstance(args, str):
            args = frappe.parse_json(args)
        doc_name = doc_name or args.get("doc_name")
    if not args and not doc_name:
        frappe.throw(_("doc_name is required"))
    doc = frappe.get_doc("Gmail Account", doc_name)
    if args and args.get("reset_historyid", False):
        doc.last_historyid = 0
        doc.save()
        doc.reload()
    frappe.msgprint(_("Sync started in the background."), alert=True)
    job_name = f"gmail_thread_sync_{doc.linked_user}"
    if not is_job_enqueued(job_name):
        frappe.enqueue(
            "frappe_gmail_thread.frappe_gmail_thread.doctype.gmail_thread.gmail_thread.sync",
            user=doc.linked_user,
            queue="long",
            job_name=job_name,
            job_id=job_name,
        )
