from frappe_gmail_thread.patches.v0_1 import (
    set_default_max_threads_per_label,
    set_reauthorization_email_defaults,
)


def after_install() -> None:
    # Patches are marked as applied on install without running, and Google Settings
    # often exists already: store the defaults its new fields wouldn't get.
    set_default_max_threads_per_label.execute()
    set_reauthorization_email_defaults.execute()
