"""Pause a mailbox's Gmail sync after Google rate-limits it.

Kept free of heavy imports, like queues.py: the scheduler and the Pub/Sub
endpoint check it before enqueueing a sync.
"""

import frappe

DEFAULT_PAUSE_SECONDS = 15 * 60


def _key(user: str) -> str:
    return f"gmail_thread_sync_paused:{user}"


def pause_sync(user: str, seconds: int) -> None:
    frappe.cache.set_value(_key(user), 1, expires_in_sec=seconds)


def is_sync_paused(user: str) -> bool:
    return bool(frappe.cache.get_value(_key(user), expires=True))
