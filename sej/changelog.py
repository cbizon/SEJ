"""Change-set management for SEJ databases.

A change set groups edits made during an editing session. Each individual
mutation is recorded in the change_log table so it can be undone on discard.
"""

import json
from datetime import datetime, timezone

from sej.db import get_connection, create_schema


def get_open_change_set(conn):
    """Return the id of the currently open change_set, or None."""
    row = conn.execute(
        "SELECT id FROM change_sets WHERE status = 'open'"
    ).fetchone()
    return row["id"] if row else None


def _next_seq(conn, change_set_id):
    """Return the next sequence number for a change_set."""
    row = conn.execute(
        "SELECT MAX(seq) AS m FROM change_log WHERE change_set_id = ?",
        (change_set_id,),
    ).fetchone()
    return (row["m"] or 0) + 1


def record_change(conn, table_name, operation, row_id, old_values, new_values):
    """Write a change_log entry if a change_set is open; no-op otherwise."""
    cs_id = get_open_change_set(conn)
    if cs_id is None:
        return
    seq = _next_seq(conn, cs_id)
    conn.execute(
        """INSERT INTO change_log
           (change_set_id, seq, table_name, operation, row_id, old_values, new_values)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            cs_id,
            seq,
            table_name,
            operation,
            row_id,
            json.dumps(old_values) if old_values is not None else None,
            json.dumps(new_values) if new_values is not None else None,
        ),
    )


def create_change_set(db_path):
    """Create an open change_set. Error if one is already open.

    Returns the name of the new change_set.
    """
    conn = get_connection(db_path)
    create_schema(conn)
    if get_open_change_set(conn) is not None:
        conn.close()
        raise ValueError("A change set is already open")
    now = datetime.now(timezone.utc)
    name = now.strftime("edit-%Y%m%d-%H%M%S")
    conn.execute(
        "INSERT INTO change_sets (name, status, created_at) VALUES (?, 'open', ?)",
        (name, now.isoformat()),
    )
    conn.commit()
    conn.close()
    return name


def merge_change_set(db_path):
    """Mark the open change_set as merged and log to audit_log.

    Returns the number of changes in the set.
    """
    conn = get_connection(db_path)
    create_schema(conn)
    cs_id = get_open_change_set(conn)
    if cs_id is None:
        conn.close()
        raise ValueError("No open change set to merge")
    now = datetime.now(timezone.utc).isoformat()
    changes_count = conn.execute(
        "SELECT COUNT(*) FROM change_log WHERE change_set_id = ?", (cs_id,)
    ).fetchone()[0]
    cs_row = conn.execute(
        "SELECT name FROM change_sets WHERE id = ?", (cs_id,)
    ).fetchone()
    conn.execute(
        "UPDATE change_sets SET status = 'merged', closed_at = ? WHERE id = ?",
        (now, cs_id),
    )
    conn.execute(
        "INSERT INTO audit_log (timestamp, action, details) VALUES (?, ?, ?)",
        (now, "merge", json.dumps({
            "change_set_name": cs_row["name"],
            "changes_count": changes_count,
        })),
    )
    conn.commit()
    conn.close()
    return changes_count


def discard_change_set(db_path):
    """Replay change_log in reverse to undo all edits, then mark as discarded."""
    conn = get_connection(db_path)
    create_schema(conn)
    cs_id = get_open_change_set(conn)
    if cs_id is None:
        conn.close()
        raise ValueError("No open change set to discard")

    entries = conn.execute(
        "SELECT * FROM change_log WHERE change_set_id = ? ORDER BY seq DESC",
        (cs_id,),
    ).fetchall()

    # Temporarily disable FK constraints for undo ordering safety
    conn.execute("PRAGMA foreign_keys = OFF")

    allowed_tables = {"efforts", "employees", "groups", "projects",
                      "budget_lines", "allocation_lines"}

    for entry in entries:
        table = entry["table_name"]
        if table not in allowed_tables:
            raise ValueError(f"Unexpected table in change_log: {table!r}")
        op = entry["operation"]
        row_id = entry["row_id"]
        old_vals = json.loads(entry["old_values"]) if entry["old_values"] else None
        new_vals = json.loads(entry["new_values"]) if entry["new_values"] else None

        if op == "insert":
            # Undo INSERT → DELETE
            conn.execute(f"DELETE FROM {table} WHERE id = ?", (row_id,))
        elif op == "update":
            # Undo UPDATE → restore old values
            if old_vals:
                set_clause = ", ".join(f"{k} = ?" for k in old_vals)
                conn.execute(
                    f"UPDATE {table} SET {set_clause} WHERE id = ?",
                    list(old_vals.values()) + [row_id],
                )
        elif op == "delete":
            # Undo DELETE → re-INSERT
            if old_vals:
                cols = ["id"] + list(old_vals.keys())
                placeholders = ", ".join("?" for _ in cols)
                conn.execute(
                    f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})",
                    [row_id] + list(old_vals.values()),
                )

    conn.execute("PRAGMA foreign_keys = ON")

    now = datetime.now(timezone.utc).isoformat()
    cs_row = conn.execute(
        "SELECT name FROM change_sets WHERE id = ?", (cs_id,)
    ).fetchone()
    conn.execute(
        "UPDATE change_sets SET status = 'discarded', closed_at = ? WHERE id = ?",
        (now, cs_id),
    )
    conn.execute(
        "INSERT INTO audit_log (timestamp, action, details) VALUES (?, ?, ?)",
        (now, "discard", json.dumps({
            "change_set_name": cs_row["name"],
            "changes_undone": len(entries),
        })),
    )
    conn.commit()
    conn.close()


def get_change_set_info(db_path):
    """Return current change-set state for the API."""
    conn = get_connection(db_path)
    create_schema(conn)
    row = conn.execute(
        "SELECT id, name, status, created_at FROM change_sets WHERE status = 'open'"
    ).fetchone()
    conn.close()
    if row is None:
        return {"status": "none"}
    return {
        "status": row["status"],
        "name": row["name"],
        "id": row["id"],
        "created_at": row["created_at"],
    }
