"""
Backfill the `all_subjects` field for all existing Gmail Thread records.

For each thread, collect:
  - subject_of_first_mail  (parent field)
  - subject                (from every child Single Email CT row)

Store them newline-separated in all_subjects so the list-view subject
filter can search across all subjects in a thread.
"""

import frappe


def execute():
    GmailThread = frappe.qb.DocType("Gmail Thread")
    SingleEmailCT = frappe.qb.DocType("Single Email CT")

    # Single query: all threads with their parent subject
    threads = (
        frappe.qb.from_(GmailThread)
        .select(GmailThread.name, GmailThread.subject_of_first_mail)
        .run(as_dict=True)
    )
    subject_map = {}
    for t in threads:
        subjects = subject_map.setdefault(t.name, set())
        if t.subject_of_first_mail:
            subjects.add(t.subject_of_first_mail.strip())

    # Single query: all child email subjects across all threads
    child_rows = (
        frappe.qb.from_(SingleEmailCT)
        .select(SingleEmailCT.parent, SingleEmailCT.subject)
        .where(SingleEmailCT.subject.isnotnull())
        .run(as_dict=True)
    )
    for row in child_rows:
        if row.parent in subject_map and row.subject:
            subject_map[row.parent].add(row.subject.strip())

    updates = {
        thread_id: {"all_subjects": "\n".join(sorted(subjects))}
        for thread_id, subjects in subject_map.items()
    }
    if not updates:
        print("[backfill_all_subjects] Nothing to update. Exiting.")
        return

    frappe.db.bulk_update(
        "Gmail Thread",
        updates,
        chunk_size=100,
        update_modified=False,
    )

    frappe.logger().info(f"backfill_all_subjects: updated {len(updates)} threads.")
