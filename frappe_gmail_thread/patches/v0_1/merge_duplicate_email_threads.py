import frappe
from frappe.query_builder import DocType
from frappe.query_builder.functions import Count

_TMP_INDEX = "email_message_id_dedup_tmp"


def execute():
    # runs pre_model_sync, before the real unique index exists; add a temp one
    # so GROUP BY doesn't full-scan millions of rows, drop it when done
    added_index = _ensure_grouping_index()
    try:
        for group in _find_duplicate_message_ids():
            _resolve_group(group.email_message_id)
        frappe.db.commit()  # nosemgrep
    finally:
        if added_index:
            frappe.db.sql_ddl(
                f"alter table `tabSingle Email CT` drop index `{_TMP_INDEX}`"
            )


def _ensure_grouping_index():
    if frappe.db.sql(
        "show index from `tabSingle Email CT` where Column_name = 'email_message_id'"
    ):
        return False
    frappe.db.sql_ddl(
        f"alter table `tabSingle Email CT` add index `{_TMP_INDEX}` (email_message_id(191))"
    )
    return True


def _find_duplicate_message_ids():
    single_email_ct = DocType("Single Email CT")
    return (
        frappe.qb.from_(single_email_ct)
        .select(single_email_ct.email_message_id)
        .where(
            single_email_ct.email_message_id.isnotnull()
            & (single_email_ct.email_message_id != "")
        )
        .groupby(single_email_ct.email_message_id)
        .having(Count(single_email_ct.name) > 1)
        .run(as_dict=True)
    )


def _resolve_group(message_id):
    rows = frappe.get_all(
        "Single Email CT",
        filters={"email_message_id": message_id},
        fields=["name", "parent"],
    )

    # dedupe same-parent rows first so each parent keeps at most one row on
    # this value, else a cross-parent merge below would delete all of them
    by_parent = {}
    for row in rows:
        by_parent.setdefault(row.parent, []).append(row.name)
    for parent_rows in by_parent.values():
        for name in parent_rows[1:]:
            _disambiguate(name, message_id)

    parents = sorted(by_parent)
    if len(parents) < 2:
        return

    candidates = [_thread_info(name) for name in parents]
    linked = [c for c in candidates if c.reference_doctype and c.reference_name]
    linked_targets = {(c.reference_doctype, c.reference_name) for c in linked}

    if len(linked_targets) > 1:
        # ponytail: unlinked candidates here also go unmerged, not just the
        # conflicting linked ones — safe, just not maximal consolidation
        frappe.log_error(
            title="Gmail Thread: duplicate email linked to different records",
            message=(
                f"email_message_id {message_id!r} is split across threads linked "
                f"to different records: {[(c.name, c.reference_doctype, c.reference_name) for c in linked]}. "
                "Not merged — needs a human to pick which link is correct."
            ),
        )
        winner = _rank(candidates)[0]
        for row in rows:
            if row.parent != winner.name:
                _disambiguate(row.name, message_id)
        return

    winner = _rank(linked or candidates)[0]
    for candidate in candidates:
        if candidate.name != winner.name:
            _merge_thread(candidate.name, winner.name, message_id)


def _thread_info(name):
    doc = frappe.get_all(
        "Gmail Thread",
        filters={"name": name},
        fields=[
            "name",
            "reference_doctype",
            "reference_name",
            "creation",
            "gmail_thread_id",
        ],
    )[0]
    doc.email_count = frappe.db.count("Single Email CT", {"parent": name})
    return doc


def _rank(candidates):
    return sorted(candidates, key=lambda c: (-c.email_count, c.creation))


def _disambiguate(single_email_ct_name, message_id):
    frappe.db.set_value(
        "Single Email CT",
        single_email_ct_name,
        "email_message_id",
        f"{message_id}#dup-{single_email_ct_name}",
        update_modified=False,
    )


def _merge_thread(loser_name, winner_name, duplicate_message_id):
    winner = frappe.get_doc("Gmail Thread", winner_name)

    if loser_name == winner_name:
        return

    if not (winner.reference_doctype and winner.reference_name):
        loser_ref = frappe.db.get_value(
            "Gmail Thread",
            loser_name,
            ["reference_doctype", "reference_name"],
            as_dict=True,
        )
        if loser_ref.reference_doctype and loser_ref.reference_name:
            winner.reference_doctype = loser_ref.reference_doctype
            winner.reference_name = loser_ref.reference_name
            if winner.status == "Open":
                winner.status = "Linked"

    existing_accounts = {u.account for u in winner.involved_users}
    for row in frappe.get_all(
        "Involved User", filters={"parent": loser_name}, fields=["account"]
    ):
        if row.account not in existing_accounts:
            winner.append("involved_users", {"account": row.account})
            existing_accounts.add(row.account)

    existing_reference_ids = {r.reference_id for r in winner.references}
    for row in frappe.get_all(
        "Gmail Thread Reference",
        filters={"parent": loser_name},
        fields=["reference_id", "reference_type", "gmail_message_id"],
    ):
        if row.reference_id not in existing_reference_ids:
            winner.append("references", dict(row))
            existing_reference_ids.add(row.reference_id)

    loser_thread_id = frappe.db.get_value("Gmail Thread", loser_name, "gmail_thread_id")
    if loser_thread_id and loser_thread_id not in existing_reference_ids:
        winner.append(
            "references",
            {"reference_id": loser_thread_id, "reference_type": "Thread-ID"},
        )

    winner.save(ignore_permissions=True)

    # drop the loser's copy of the duplicate row, reparent the rest
    # ponytail: assumes a repeated bogus (non-RFC) id across parents is a
    # real duplicate too — no evidence otherwise in real data today
    for row in frappe.get_all(
        "Single Email CT",
        filters={"parent": loser_name},
        fields=["name", "email_message_id"],
    ):
        if row.email_message_id == duplicate_message_id:
            frappe.delete_doc(
                "Single Email CT", row.name, ignore_permissions=True, force=True
            )
        else:
            frappe.db.set_value(
                "Single Email CT",
                row.name,
                "parent",
                winner_name,
                update_modified=False,
            )

    for file_name in frappe.get_all(
        "File",
        filters={"attached_to_doctype": "Gmail Thread", "attached_to_name": loser_name},
        pluck="name",
    ):
        frappe.db.set_value("File", file_name, "attached_to_name", winner_name)

    frappe.delete_doc("Gmail Thread", loser_name, ignore_permissions=True, force=True)

    # moved rows just got appended — re-sort by date so the merged
    # thread's timeline still reads chronologically
    emails = frappe.get_all(
        "Single Email CT",
        filters={"parent": winner_name},
        fields=["name", "date_and_time"],
        order_by="date_and_time asc",
    )
    for idx, row in enumerate(emails, start=1):
        frappe.db.set_value(
            "Single Email CT", row.name, "idx", idx, update_modified=False
        )
