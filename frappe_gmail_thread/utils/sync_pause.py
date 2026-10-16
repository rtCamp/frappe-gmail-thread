"""Pause a mailbox's Gmail sync after Google rate-limits it, and count failed fetches.

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


def count_fetch_failure(user: str, item_id: str) -> int:
    """Record one more failed fetch of a thread or message (kept a day); returns the count."""
    key = f"gmail_thread_fetch_failures:{user}:{item_id}"
    count = (frappe.cache.get_value(key) or 0) + 1
    frappe.cache.set_value(key, count, expires_in_sec=24 * 60 * 60)
    return count
