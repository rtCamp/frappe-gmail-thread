"""Background queue selection for Gmail sync jobs.

Kept in its own lightweight module (no BeautifulSoup/email imports) so that
scheduler/web workers can pick a queue name without loading helpers.py.
"""

SYNC_QUEUE = "gmail_sync"
SYNC_QUEUE_FALLBACK = "long"


def get_sync_queue():
    """Return the queue to enqueue Gmail sync jobs on.

    Uses the dedicated ``gmail_sync`` queue when it is configured under
    ``workers`` in common_site_config.json, otherwise falls back to ``long``
    (always available), so the app keeps working on benches without the
    custom queue/worker set up.
    """
    from frappe.utils.background_jobs import get_queues_timeout

    if SYNC_QUEUE in get_queues_timeout():
        return SYNC_QUEUE
    return SYNC_QUEUE_FALLBACK
