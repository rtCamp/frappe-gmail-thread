"""Background queue selection for Gmail sync jobs.

Kept in its own lightweight module (no BeautifulSoup/email imports) so that
scheduler/web workers can pick a queue name without loading helpers.py.
"""

GMAIL_THREAD_SYNC_QUEUE = "gmail_thread_sync"
GMAIL_THREAD_SYNC_QUEUE_FALLBACK = "long"


def get_gmail_thread_sync_queue_name():
    """Return the queue to enqueue Gmail sync jobs on.

    Uses the dedicated ``gmail_thread_sync`` queue when it is configured under
    ``workers`` in common_site_config.json, otherwise falls back to ``long``
    (always available), so the app keeps working on benches without the
    custom queue/worker set up.
    """
    from frappe.utils.background_jobs import get_queue_list

    if GMAIL_THREAD_SYNC_QUEUE in get_queue_list():
        return GMAIL_THREAD_SYNC_QUEUE
    return GMAIL_THREAD_SYNC_QUEUE_FALLBACK
