"""Branch management for SEJ databases.

Branches are full file copies of the main database. Users create a branch,
edit it via the web UI, then merge it back to main (producing a change-log TSV).
"""

import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

from sej.db import create_schema, get_connection

_BRANCH_NAME_RE = re.compile(r"[A-Za-z0-9_-]+")


def _validate_branch_name(name: str) -> str:
    """Validate that a branch name is safe for use in filenames."""
    if not _BRANCH_NAME_RE.fullmatch(name):
        raise ValueError(
            f"Invalid branch name {name!r}. "
            "Allowed characters: letters, digits, underscore, hyphen."
        )
    return name


def branch_db_path(main_db_path: str | Path, branch_name: str) -> Path:
    """Return the file path for a branch database."""
    _validate_branch_name(branch_name)
    main = Path(main_db_path)
    return main.parent / f"{main.stem}_branch_{branch_name}{main.suffix}"


def _set_meta(conn, key: str, value: str | None) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        (key, value),
    )


def _get_meta(conn, key: str) -> str | None:
    row = conn.execute("SELECT value FROM _meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def _log_audit(conn, action: str, details: dict) -> None:
    conn.execute(
        "INSERT INTO audit_log (timestamp, action, details) VALUES (?, ?, ?)",
        (datetime.now(timezone.utc).isoformat(), action, json.dumps(details)),
    )


def create_branch(main_db_path: str | Path, branch_name: str) -> Path:
    """Copy the main DB to a branch file and record it in the audit log.

    Returns the path to the new branch database.
    """
    main_db_path = Path(main_db_path)
    dest = branch_db_path(main_db_path, branch_name)
    if dest.exists():
        raise FileExistsError(f"Branch '{branch_name}' already exists: {dest}")

    shutil.copy2(main_db_path, dest)

    # Mark the branch copy
    conn = get_connection(dest)
    create_schema(conn)
    _set_meta(conn, "db_role", "branch")
    _set_meta(conn, "branch_name", branch_name)
    _set_meta(conn, "source_db", str(main_db_path))
    conn.commit()
    conn.close()

    # Log in main
    conn = get_connection(main_db_path)
    create_schema(conn)
    _log_audit(conn, "branch_create", {"branch_name": branch_name, "branch_path": str(dest)})
    conn.commit()
    conn.close()

    return dest


def list_branches(main_db_path: str | Path) -> list[dict]:
    """Return info about existing branch files for this main DB.

    Scans the directory for matching branch files and reads their _meta.
    """
    main_db_path = Path(main_db_path)
    pattern = f"{main_db_path.stem}_branch_*{main_db_path.suffix}"
    branches = []
    for p in sorted(main_db_path.parent.glob(pattern)):
        conn = get_connection(p)
        create_schema(conn)
        name = _get_meta(conn, "branch_name")
        conn.close()
        if name:
            branches.append({"name": name, "path": str(p)})
    return branches


def delete_branch(main_db_path: str | Path, branch_name: str) -> None:
    """Delete a branch database file and log it."""
    main_db_path = Path(main_db_path)
    dest = branch_db_path(main_db_path, branch_name)
    if not dest.exists():
        raise FileNotFoundError(f"Branch '{branch_name}' not found: {dest}")

    dest.unlink()

    conn = get_connection(main_db_path)
    create_schema(conn)
    _log_audit(conn, "branch_delete", {"branch_name": branch_name})
    conn.commit()
    conn.close()


def diff_databases(main_db_path: str | Path, branch_db_path_: str | Path) -> list[dict]:
    """Compare two databases and return a list of change records.

    Each record is a dict with:
        type: "effort_changed" | "effort_added" | "effort_removed" |
              "allocation_line_added" | "allocation_line_removed"
        employee, project_code, year (optional), month (optional),
        old_value (optional), new_value (optional)
    """
    def _get_efforts(db_path):
        conn = get_connection(db_path)
        rows = conn.execute("""
            SELECT emp.name AS employee, p.project_code, e.year, e.month, e.percentage
            FROM efforts e
            JOIN allocation_lines al ON al.id = e.allocation_line_id
            JOIN employees emp ON emp.id = al.employee_id
            JOIN projects p ON p.id = al.project_id
        """).fetchall()
        conn.close()
        result = {}
        for r in rows:
            key = (r["employee"], r["project_code"], r["year"], r["month"])
            result[key] = r["percentage"]
        return result

    def _get_allocation_lines(db_path):
        conn = get_connection(db_path)
        rows = conn.execute("""
            SELECT emp.name AS employee, p.project_code
            FROM allocation_lines al
            JOIN employees emp ON emp.id = al.employee_id
            JOIN projects p ON p.id = al.project_id
        """).fetchall()
        conn.close()
        # Use a counter since same employee+project can have multiple lines
        from collections import Counter
        return Counter((r["employee"], r["project_code"]) for r in rows)

    changes = []

    # Diff efforts
    main_efforts = _get_efforts(main_db_path)
    branch_efforts = _get_efforts(branch_db_path_)

    all_keys = set(main_efforts.keys()) | set(branch_efforts.keys())
    for key in sorted(all_keys):
        old = main_efforts.get(key)
        new = branch_efforts.get(key)
        if old != new:
            employee, project_code, year, month = key
            if old is None:
                change_type = "effort_added"
            elif new is None:
                change_type = "effort_removed"
            else:
                change_type = "effort_changed"
            changes.append({
                "type": change_type,
                "employee": employee,
                "project_code": project_code,
                "year": year,
                "month": month,
                "old_value": old,
                "new_value": new,
            })

    # Diff allocation lines (detect structural adds/removes)
    main_lines = _get_allocation_lines(main_db_path)
    branch_lines = _get_allocation_lines(branch_db_path_)

    all_line_keys = set(main_lines.keys()) | set(branch_lines.keys())
    for key in sorted(all_line_keys):
        main_count = main_lines.get(key, 0)
        branch_count = branch_lines.get(key, 0)
        diff = branch_count - main_count
        employee, project_code = key
        if diff > 0:
            for _ in range(diff):
                changes.append({
                    "type": "allocation_line_added",
                    "employee": employee,
                    "project_code": project_code,
                })
        elif diff < 0:
            for _ in range(-diff):
                changes.append({
                    "type": "allocation_line_removed",
                    "employee": employee,
                    "project_code": project_code,
                })

    return changes


def merge_branch(main_db_path: str | Path, branch_name: str) -> Path | None:
    """Merge a branch back to main.

    1. Compute diff (branch vs main)
    2. Write diff to TSV file
    3. Replace main DB with branch DB
    4. Update _meta in the now-main DB
    5. Record merge in audit_log
    6. Delete branch file

    Returns the path to the change-log TSV, or None if there were no changes.
    """
    main_db_path = Path(main_db_path)
    branch_path = branch_db_path(main_db_path, branch_name)
    if not branch_path.exists():
        raise FileNotFoundError(f"Branch '{branch_name}' not found: {branch_path}")

    changes = diff_databases(main_db_path, branch_path)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")

    tsv_path = None
    if changes:
        tsv_path = main_db_path.parent / f"merge_{branch_name}_{timestamp}.tsv"
        with open(tsv_path, "w", newline="", encoding="utf-8") as fh:
            fh.write("type\temployee\tproject_code\tyear\tmonth\told_value\tnew_value\n")
            for c in changes:
                ctype = c["type"]
                year = c.get("year", "")
                month = c.get("month", "")
                old_val = c.get("old_value")
                new_val = c.get("new_value")
                old_str = f"{old_val:.2f}" if old_val is not None else ""
                new_str = f"{new_val:.2f}" if new_val is not None else ""
                fh.write(f"{ctype}\t{c['employee']}\t{c['project_code']}\t{year}\t{month}\t{old_str}\t{new_str}\n")

    # Back up main before replacing
    backup_path = main_db_path.parent / f"{main_db_path.stem}_backup_{timestamp}{main_db_path.suffix}"
    shutil.copy2(main_db_path, backup_path)

    # Carry main's audit log into the branch DB before it becomes main.
    # The branch was copied before audit entries were written to main
    # (e.g. branch_create), so those would be lost on file replacement.
    main_conn = get_connection(main_db_path)
    create_schema(main_conn)
    main_audit_rows = main_conn.execute(
        "SELECT timestamp, action, details FROM audit_log ORDER BY id"
    ).fetchall()
    main_conn.close()

    branch_conn = get_connection(branch_path)
    create_schema(branch_conn)
    # Clear branch audit log and replace with main's (authoritative)
    branch_conn.execute("DELETE FROM audit_log")
    for row in main_audit_rows:
        branch_conn.execute(
            "INSERT INTO audit_log (timestamp, action, details) VALUES (?, ?, ?)",
            (row["timestamp"], row["action"], row["details"]),
        )
    branch_conn.commit()
    branch_conn.close()

    # Replace main with branch (atomic on same filesystem)
    branch_path.replace(main_db_path)

    # Update _meta to reflect main status
    conn = get_connection(main_db_path)
    _set_meta(conn, "db_role", "main")
    _set_meta(conn, "branch_name", None)
    _set_meta(conn, "source_db", None)
    _log_audit(conn, "merge", {
        "branch_name": branch_name,
        "changes_count": len(changes),
        "tsv_path": str(tsv_path) if tsv_path else None,
        "backup_path": str(backup_path),
    })
    conn.commit()
    conn.close()

    return tsv_path


def list_backups(main_db_path: str | Path) -> list[dict]:
    """Return available backups for a main DB, most recent first.

    Each entry has keys: path, timestamp (str parsed from filename).
    """
    main_db_path = Path(main_db_path)
    pattern = f"{main_db_path.stem}_backup_*{main_db_path.suffix}"
    backups = []
    for p in sorted(main_db_path.parent.glob(pattern), reverse=True):
        # Extract timestamp from filename: stem_backup_YYYYMMDD_HHMMSS
        stem = p.stem  # e.g. "sej_backup_20260215_143000"
        prefix = f"{main_db_path.stem}_backup_"
        ts_str = stem[len(prefix):]
        backups.append({"path": str(p), "timestamp": ts_str})
    return backups


def revert(main_db_path: str | Path, backup_path: str | Path | None = None) -> Path:
    """Revert main DB to a backup.

    If backup_path is None, reverts to the most recent backup.
    Returns the path of the backup that was restored.
    """
    main_db_path = Path(main_db_path)

    if backup_path is None:
        backups = list_backups(main_db_path)
        if not backups:
            raise FileNotFoundError(f"No backups found for {main_db_path}")
        backup_path = Path(backups[0]["path"])
    else:
        backup_path = Path(backup_path)

    if not backup_path.exists():
        raise FileNotFoundError(f"Backup not found: {backup_path}")

    shutil.copy2(backup_path, main_db_path)

    conn = get_connection(main_db_path)
    _set_meta(conn, "db_role", "main")
    _set_meta(conn, "branch_name", None)
    _set_meta(conn, "source_db", None)
    _log_audit(conn, "revert", {"backup_path": str(backup_path)})
    conn.commit()
    conn.close()

    return backup_path


def prune_backups(main_db_path: str | Path, keep: int = 5) -> list[Path]:
    """Delete old backups, keeping the most recent `keep` files.

    Returns the list of deleted paths.
    """
    backups = list_backups(main_db_path)
    to_delete = backups[keep:]
    deleted = []
    for b in to_delete:
        p = Path(b["path"])
        p.unlink()
        deleted.append(p)
    return deleted


def main():
    """CLI entry point: ``sej-branch <command> [args]``."""
    import sys

    usage = (
        "Usage: sej-branch <command> [args]\n"
        "Commands:\n"
        "  create          <name> [--from-db DB_PATH]\n"
        "  merge           <name> [--db DB_PATH]\n"
        "  list            [--db DB_PATH]\n"
        "  delete          <name> [--db DB_PATH]\n"
        "  revert          [BACKUP_PATH] [--db DB_PATH]\n"
        "  backups         [--db DB_PATH]\n"
        "  prune-backups   [--keep N] [--db DB_PATH]\n"
    )

    if len(sys.argv) < 2:
        sys.exit(usage)

    command = sys.argv[1]
    args = sys.argv[2:]

    def _find_flag(flag, default=None):
        if flag in args:
            idx = args.index(flag)
            if idx + 1 < len(args):
                return args[idx + 1]
        return default

    def _positional():
        result = []
        skip = False
        for a in args:
            if skip:
                skip = False
                continue
            if a.startswith("--"):
                skip = True  # skip the flag's value too
                continue
            result.append(a)
        return result

    default_db = "sej.db"

    if command == "create":
        positional = _positional()
        if not positional:
            sys.exit("Usage: sej-branch create <name> [--from-db DB_PATH]")
        name = positional[0]
        db_path = _find_flag("--from-db", default_db)
        dest = create_branch(db_path, name)
        print(f"Created branch '{name}' → {dest}")

    elif command == "merge":
        positional = _positional()
        if not positional:
            sys.exit("Usage: sej-branch merge <name> [--db DB_PATH]")
        name = positional[0]
        db_path = _find_flag("--db", default_db)
        tsv = merge_branch(db_path, name)
        if tsv:
            print(f"Merged branch '{name}'. Change log: {tsv}")
        else:
            print(f"Merged branch '{name}'. No changes detected.")

    elif command == "list":
        db_path = _find_flag("--db", default_db)
        branches = list_branches(db_path)
        if branches:
            for b in branches:
                print(f"  {b['name']}  ({b['path']})")
        else:
            print("No branches found.")

    elif command == "delete":
        positional = _positional()
        if not positional:
            sys.exit("Usage: sej-branch delete <name> [--db DB_PATH]")
        name = positional[0]
        db_path = _find_flag("--db", default_db)
        delete_branch(db_path, name)
        print(f"Deleted branch '{name}'.")

    elif command == "revert":
        positional = _positional()
        db_path = _find_flag("--db", default_db)
        backup = positional[0] if positional else None
        restored = revert(db_path, backup)
        print(f"Reverted to backup: {restored}")

    elif command == "backups":
        db_path = _find_flag("--db", default_db)
        backups = list_backups(db_path)
        if backups:
            for i, b in enumerate(backups):
                label = " (latest)" if i == 0 else ""
                print(f"  {b['timestamp']}{label}  {b['path']}")
        else:
            print("No backups found.")

    elif command == "prune-backups":
        db_path = _find_flag("--db", default_db)
        keep = int(_find_flag("--keep", "5"))
        deleted = prune_backups(db_path, keep)
        if deleted:
            for p in deleted:
                print(f"  Deleted: {p}")
            print(f"Pruned {len(deleted)} backup(s), kept {keep}.")
        else:
            print("Nothing to prune.")

    else:
        sys.exit(f"Unknown command: {command}\n{usage}")
