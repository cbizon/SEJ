import csv
import json
import sys
from pathlib import Path

import pytest

from sej.db import get_connection, create_schema
from sej.importer import load_tsv
from sej.branch import (
    _validate_branch_name,
    branch_db_path,
    create_branch,
    list_branches,
    delete_branch,
    diff_databases,
    merge_branch,
    list_backups,
    revert,
    prune_backups,
    main as branch_main,
)


HEADER = [
    "EMPLOYEE", "Group", "Fund Code", "Source", "Account",
    "Cost Code 1", "Cost Code 2", "Cost Code 3", "Program Code",
    "Project Id", "Project Name",
    "July 2025", "August 2025",
]


def _write_tsv(path: Path, rows: list[list[str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(HEADER)
        writer.writerows(rows)


@pytest.fixture
def main_db(tmp_path):
    """Create a populated main database."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "sej.db"
    _write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "50.00%", "60.00%"],
        ["", "Engineering", "25210", "49000", "511120",
         "", "", "", "", "5120002", "Gadget Project",
         "50.00%", "40.00%"],
        ["Jones,Bob", "Ops", "", "", "",
         "", "", "", "", "N/A", "N/A",
         "100.00%", "100.00%"],
    ])
    load_tsv(tsv, db)
    return db


def test_branch_db_path():
    p = branch_db_path("/data/sej.db", "edits")
    assert p == Path("/data/sej_branch_edits.db")


def test_branch_name_validation_accepts_valid():
    _validate_branch_name("my-branch_123")


def test_branch_name_validation_rejects_path_traversal():
    with pytest.raises(ValueError, match="Invalid branch name"):
        _validate_branch_name("../evil")


def test_branch_name_validation_rejects_spaces():
    with pytest.raises(ValueError, match="Invalid branch name"):
        _validate_branch_name("has spaces")


def test_branch_name_validation_rejects_slashes():
    with pytest.raises(ValueError, match="Invalid branch name"):
        _validate_branch_name("sub/dir")


def test_create_branch(main_db):
    dest = create_branch(main_db, "test_edits")
    assert dest.exists()
    conn = get_connection(dest)
    role = conn.execute("SELECT value FROM _meta WHERE key='db_role'").fetchone()
    assert role["value"] == "branch"
    name = conn.execute("SELECT value FROM _meta WHERE key='branch_name'").fetchone()
    assert name["value"] == "test_edits"
    conn.close()


def test_create_branch_duplicate_raises(main_db):
    create_branch(main_db, "dup")
    with pytest.raises(FileExistsError):
        create_branch(main_db, "dup")


def test_create_branch_logs_audit(main_db):
    create_branch(main_db, "audited")
    conn = get_connection(main_db)
    log = conn.execute(
        "SELECT * FROM audit_log WHERE action='branch_create'"
    ).fetchone()
    conn.close()
    assert log is not None
    details = json.loads(log["details"])
    assert details["branch_name"] == "audited"


def test_list_branches(main_db):
    create_branch(main_db, "alpha")
    create_branch(main_db, "beta")
    branches = list_branches(main_db)
    names = [b["name"] for b in branches]
    assert "alpha" in names
    assert "beta" in names


def test_list_branches_empty(main_db):
    assert list_branches(main_db) == []


def test_delete_branch(main_db):
    create_branch(main_db, "to_delete")
    delete_branch(main_db, "to_delete")
    assert not branch_db_path(main_db, "to_delete").exists()


def test_delete_branch_not_found(main_db):
    with pytest.raises(FileNotFoundError):
        delete_branch(main_db, "nonexistent")


def test_delete_branch_logs_audit(main_db):
    create_branch(main_db, "del_audit")
    delete_branch(main_db, "del_audit")
    conn = get_connection(main_db)
    log = conn.execute(
        "SELECT * FROM audit_log WHERE action='branch_delete'"
    ).fetchone()
    conn.close()
    assert log is not None
    details = json.loads(log["details"])
    assert details["branch_name"] == "del_audit"


def test_diff_no_changes(main_db):
    dest = create_branch(main_db, "unchanged")
    changes = diff_databases(main_db, dest)
    assert changes == []


def test_diff_detects_changed_value(main_db):
    dest = create_branch(main_db, "edited")
    conn = get_connection(dest)
    # Change Smith,Jane's Widget Project July 2025 effort from 50 to 75
    conn.execute("""
        UPDATE efforts SET percentage = 75.0
        WHERE allocation_line_id = (
            SELECT al.id FROM allocation_lines al
            JOIN employees e ON e.id = al.employee_id
            JOIN budget_lines bl ON bl.id = al.budget_line_id
            WHERE e.name = 'Smith,Jane' AND bl.budget_line_code = '5120001'
        ) AND year = 2025 AND month = 7
    """)
    conn.commit()
    conn.close()

    changes = diff_databases(main_db, dest)
    effort_changes = [c for c in changes if c["type"] == "effort_changed"]
    assert len(effort_changes) == 1
    assert effort_changes[0]["employee"] == "Smith,Jane"
    assert effort_changes[0]["old_value"] == 50.0
    assert effort_changes[0]["new_value"] == 75.0


def test_diff_detects_added_effort(main_db):
    dest = create_branch(main_db, "added")
    conn = get_connection(dest)
    # Add a new effort row for an existing allocation line in a new month
    line_id = conn.execute("""
        SELECT al.id FROM allocation_lines al
        JOIN employees e ON e.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE e.name = 'Smith,Jane' AND bl.budget_line_code = '5120001'
    """).fetchone()[0]
    conn.execute(
        "INSERT INTO efforts (allocation_line_id, year, month, percentage) VALUES (?, 2025, 9, 30.0)",
        (line_id,),
    )
    conn.commit()
    conn.close()

    changes = diff_databases(main_db, dest)
    effort_adds = [c for c in changes if c["type"] == "effort_added"]
    assert len(effort_adds) == 1
    assert effort_adds[0]["old_value"] is None
    assert effort_adds[0]["new_value"] == 30.0


def test_diff_detects_removed_effort(main_db):
    dest = create_branch(main_db, "removed")
    conn = get_connection(dest)
    conn.execute("""
        DELETE FROM efforts
        WHERE allocation_line_id = (
            SELECT al.id FROM allocation_lines al
            JOIN employees e ON e.id = al.employee_id
            JOIN budget_lines bl ON bl.id = al.budget_line_id
            JOIN projects p ON p.id = bl.project_id
                WHERE e.name = 'Jones,Bob' AND p.is_nonproject = 1
        ) AND year = 2025 AND month = 7
    """)
    conn.commit()
    conn.close()

    changes = diff_databases(main_db, dest)
    effort_removes = [c for c in changes if c["type"] == "effort_removed"]
    assert len(effort_removes) == 1
    assert effort_removes[0]["old_value"] == 100.0
    assert effort_removes[0]["new_value"] is None


def test_diff_detects_added_allocation_line(main_db):
    dest = create_branch(main_db, "new_line")
    conn = get_connection(dest)
    emp_id = conn.execute("SELECT id FROM employees WHERE name = 'Smith,Jane'").fetchone()[0]
    bl_id = conn.execute("SELECT id FROM budget_lines WHERE budget_line_code = '5120001'").fetchone()[0]
    conn.execute("INSERT INTO allocation_lines (employee_id, budget_line_id) VALUES (?, ?)", (emp_id, bl_id))
    conn.commit()
    conn.close()

    changes = diff_databases(main_db, dest)
    line_adds = [c for c in changes if c["type"] == "allocation_line_added"]
    assert len(line_adds) == 1
    assert line_adds[0]["employee"] == "Smith,Jane"
    assert line_adds[0]["budget_line_code"] == "5120001"


def test_merge_produces_tsv(main_db):
    dest = create_branch(main_db, "merge_test")
    conn = get_connection(dest)
    conn.execute("""
        UPDATE efforts SET percentage = 80.0
        WHERE allocation_line_id = (
            SELECT al.id FROM allocation_lines al
            JOIN employees e ON e.id = al.employee_id
            JOIN budget_lines bl ON bl.id = al.budget_line_id
            WHERE e.name = 'Smith,Jane' AND bl.budget_line_code = '5120001'
        ) AND year = 2025 AND month = 7
    """)
    conn.commit()
    conn.close()

    tsv_path = merge_branch(main_db, "merge_test")
    assert tsv_path is not None
    assert tsv_path.exists()

    with open(tsv_path) as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["type"] == "effort_changed"
    assert rows[0]["employee"] == "Smith,Jane"
    assert rows[0]["old_value"] == "50.00"
    assert rows[0]["new_value"] == "80.00"


def test_merge_replaces_main(main_db):
    dest = create_branch(main_db, "replace_test")
    conn = get_connection(dest)
    conn.execute("""
        UPDATE efforts SET percentage = 99.0
        WHERE allocation_line_id = (
            SELECT al.id FROM allocation_lines al
            JOIN employees e ON e.id = al.employee_id
            JOIN budget_lines bl ON bl.id = al.budget_line_id
            JOIN projects p ON p.id = bl.project_id
                WHERE e.name = 'Jones,Bob' AND p.is_nonproject = 1
        ) AND year = 2025 AND month = 7
    """)
    conn.commit()
    conn.close()

    merge_branch(main_db, "replace_test")

    # Branch file should be gone
    assert not dest.exists()

    # Main should have the new value
    conn = get_connection(main_db)
    row = conn.execute("""
        SELECT e.percentage FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE emp.name = 'Jones,Bob' AND bl.budget_line_code = 'Non-Project'
        AND e.year = 2025 AND e.month = 7
    """).fetchone()
    assert row["percentage"] == 99.0

    # _meta should say main
    role = conn.execute("SELECT value FROM _meta WHERE key='db_role'").fetchone()
    assert role["value"] == "main"
    conn.close()


def test_merge_no_changes(main_db):
    create_branch(main_db, "noop")
    tsv_path = merge_branch(main_db, "noop")
    assert tsv_path is None


def test_merge_logs_audit(main_db):
    create_branch(main_db, "audit_merge")
    merge_branch(main_db, "audit_merge")
    conn = get_connection(main_db)
    log = conn.execute(
        "SELECT * FROM audit_log WHERE action='merge'"
    ).fetchone()
    conn.close()
    assert log is not None
    details = json.loads(log["details"])
    assert details["branch_name"] == "audit_merge"


def test_merge_preserves_main_audit_log(main_db):
    """Audit entries written to main after branch creation should survive merge."""
    create_branch(main_db, "audit_preserve")

    # Main's audit log now has the branch_create entry
    conn = get_connection(main_db)
    pre_merge_count = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
    conn.close()
    assert pre_merge_count > 0

    merge_branch(main_db, "audit_preserve")

    # After merge, main should still have the branch_create entry + the merge entry
    conn = get_connection(main_db)
    entries = conn.execute("SELECT action FROM audit_log ORDER BY id").fetchall()
    conn.close()
    actions = [e["action"] for e in entries]
    assert "branch_create" in actions
    assert "merge" in actions


def test_merge_nonexistent_raises(main_db):
    with pytest.raises(FileNotFoundError):
        merge_branch(main_db, "ghost")


# --- Backup / revert tests ---

def test_merge_creates_backup(main_db):
    create_branch(main_db, "backup_test")
    merge_branch(main_db, "backup_test")
    backups = list_backups(main_db)
    assert len(backups) == 1


def test_merge_backup_has_original_data(main_db):
    """The backup should contain pre-merge data."""
    # Get original value
    conn = get_connection(main_db)
    orig = conn.execute("""
        SELECT e.percentage FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE emp.name = 'Smith,Jane' AND bl.budget_line_code = '5120001'
        AND e.year = 2025 AND e.month = 7
    """).fetchone()["percentage"]
    conn.close()

    # Make a change in a branch and merge
    dest = create_branch(main_db, "change_and_backup")
    conn = get_connection(dest)
    conn.execute("""
        UPDATE efforts SET percentage = 99.0
        WHERE allocation_line_id = (
            SELECT al.id FROM allocation_lines al
            JOIN employees e ON e.id = al.employee_id
            JOIN budget_lines bl ON bl.id = al.budget_line_id
            WHERE e.name = 'Smith,Jane' AND bl.budget_line_code = '5120001'
        ) AND year = 2025 AND month = 7
    """)
    conn.commit()
    conn.close()
    merge_branch(main_db, "change_and_backup")

    # Backup should have original value
    backups = list_backups(main_db)
    backup_conn = get_connection(backups[0]["path"])
    backup_val = backup_conn.execute("""
        SELECT e.percentage FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE emp.name = 'Smith,Jane' AND bl.budget_line_code = '5120001'
        AND e.year = 2025 AND e.month = 7
    """).fetchone()["percentage"]
    backup_conn.close()
    assert backup_val == orig


def test_revert_to_latest(main_db):
    # Merge a change
    dest = create_branch(main_db, "revert_test")
    conn = get_connection(dest)
    conn.execute("""
        UPDATE efforts SET percentage = 1.0
        WHERE allocation_line_id = (
            SELECT al.id FROM allocation_lines al
            JOIN employees e ON e.id = al.employee_id
            JOIN budget_lines bl ON bl.id = al.budget_line_id
            JOIN projects p ON p.id = bl.project_id
                WHERE e.name = 'Jones,Bob' AND p.is_nonproject = 1
        ) AND year = 2025 AND month = 7
    """)
    conn.commit()
    conn.close()
    merge_branch(main_db, "revert_test")

    # Verify changed
    conn = get_connection(main_db)
    val = conn.execute("""
        SELECT e.percentage FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE emp.name = 'Jones,Bob' AND bl.budget_line_code = 'Non-Project'
        AND e.year = 2025 AND e.month = 7
    """).fetchone()["percentage"]
    conn.close()
    assert val == 1.0

    # Revert
    revert(main_db)

    # Should be back to original
    conn = get_connection(main_db)
    val = conn.execute("""
        SELECT e.percentage FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE emp.name = 'Jones,Bob' AND bl.budget_line_code = 'Non-Project'
        AND e.year = 2025 AND e.month = 7
    """).fetchone()["percentage"]
    conn.close()
    assert val == 100.0


def test_revert_to_specific_backup(main_db):
    # Do two merges to get two backups
    dest1 = create_branch(main_db, "merge1")
    conn = get_connection(dest1)
    conn.execute("""
        UPDATE efforts SET percentage = 11.0
        WHERE allocation_line_id = (
            SELECT al.id FROM allocation_lines al
            JOIN employees e ON e.id = al.employee_id
            JOIN budget_lines bl ON bl.id = al.budget_line_id
            JOIN projects p ON p.id = bl.project_id
                WHERE e.name = 'Jones,Bob' AND p.is_nonproject = 1
        ) AND year = 2025 AND month = 7
    """)
    conn.commit()
    conn.close()
    merge_branch(main_db, "merge1")

    dest2 = create_branch(main_db, "merge2")
    conn = get_connection(dest2)
    conn.execute("""
        UPDATE efforts SET percentage = 22.0
        WHERE allocation_line_id = (
            SELECT al.id FROM allocation_lines al
            JOIN employees e ON e.id = al.employee_id
            JOIN budget_lines bl ON bl.id = al.budget_line_id
            JOIN projects p ON p.id = bl.project_id
                WHERE e.name = 'Jones,Bob' AND p.is_nonproject = 1
        ) AND year = 2025 AND month = 7
    """)
    conn.commit()
    conn.close()
    merge_branch(main_db, "merge2")

    # We should have 2 backups; revert to the older one (original data)
    backups = list_backups(main_db)
    assert len(backups) == 2
    older_backup = backups[1]["path"]  # second is older (list is most-recent-first)
    revert(main_db, older_backup)

    conn = get_connection(main_db)
    val = conn.execute("""
        SELECT e.percentage FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE emp.name = 'Jones,Bob' AND bl.budget_line_code = 'Non-Project'
        AND e.year = 2025 AND e.month = 7
    """).fetchone()["percentage"]
    conn.close()
    assert val == 100.0


def test_revert_no_backups(main_db):
    with pytest.raises(FileNotFoundError, match="No backups"):
        revert(main_db)


def test_revert_logs_audit(main_db):
    create_branch(main_db, "aud_rev")
    merge_branch(main_db, "aud_rev")
    revert(main_db)
    conn = get_connection(main_db)
    log = conn.execute(
        "SELECT * FROM audit_log WHERE action='revert'"
    ).fetchone()
    conn.close()
    assert log is not None


def test_list_backups_empty(main_db):
    assert list_backups(main_db) == []


def test_list_backups_order(main_db):
    """Backups should be returned most-recent-first."""
    create_branch(main_db, "b1")
    merge_branch(main_db, "b1")
    create_branch(main_db, "b2")
    merge_branch(main_db, "b2")
    backups = list_backups(main_db)
    assert len(backups) == 2
    # First should have a later timestamp
    assert backups[0]["timestamp"] >= backups[1]["timestamp"]


def test_prune_backups(main_db):
    for i in range(4):
        create_branch(main_db, f"prune{i}")
        merge_branch(main_db, f"prune{i}")
    assert len(list_backups(main_db)) == 4

    deleted = prune_backups(main_db, keep=2)
    assert len(deleted) == 2
    assert len(list_backups(main_db)) == 2


def test_prune_nothing_to_prune(main_db):
    create_branch(main_db, "single")
    merge_branch(main_db, "single")
    deleted = prune_backups(main_db, keep=5)
    assert deleted == []


# --- CLI tests ---

def test_cli_no_args(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["sej-branch"])
    with pytest.raises(SystemExit):
        branch_main()


def test_cli_create(main_db, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["sej-branch", "create", "cli_test", "--from-db", str(main_db)])
    branch_main()
    out = capsys.readouterr().out
    assert "cli_test" in out


def test_cli_list(main_db, monkeypatch, capsys):
    create_branch(main_db, "listed")
    monkeypatch.setattr(sys, "argv", ["sej-branch", "list", "--db", str(main_db)])
    branch_main()
    out = capsys.readouterr().out
    assert "listed" in out


def test_cli_list_empty(main_db, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["sej-branch", "list", "--db", str(main_db)])
    branch_main()
    out = capsys.readouterr().out
    assert "No branches" in out


def test_cli_delete(main_db, monkeypatch, capsys):
    create_branch(main_db, "to_del_cli")
    monkeypatch.setattr(sys, "argv", ["sej-branch", "delete", "to_del_cli", "--db", str(main_db)])
    branch_main()
    out = capsys.readouterr().out
    assert "Deleted" in out


def test_cli_merge(main_db, monkeypatch, capsys):
    create_branch(main_db, "merge_cli")
    monkeypatch.setattr(sys, "argv", ["sej-branch", "merge", "merge_cli", "--db", str(main_db)])
    branch_main()
    out = capsys.readouterr().out
    assert "Merged" in out


def test_cli_revert(main_db, monkeypatch, capsys):
    create_branch(main_db, "rev_cli")
    merge_branch(main_db, "rev_cli")
    monkeypatch.setattr(sys, "argv", ["sej-branch", "revert", "--db", str(main_db)])
    branch_main()
    out = capsys.readouterr().out
    assert "Reverted" in out


def test_cli_backups(main_db, monkeypatch, capsys):
    create_branch(main_db, "bk_cli")
    merge_branch(main_db, "bk_cli")
    monkeypatch.setattr(sys, "argv", ["sej-branch", "backups", "--db", str(main_db)])
    branch_main()
    out = capsys.readouterr().out
    assert "latest" in out


def test_cli_backups_empty(main_db, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["sej-branch", "backups", "--db", str(main_db)])
    branch_main()
    out = capsys.readouterr().out
    assert "No backups" in out


def test_cli_prune_backups(main_db, monkeypatch, capsys):
    for i in range(3):
        create_branch(main_db, f"pr{i}")
        merge_branch(main_db, f"pr{i}")
    monkeypatch.setattr(sys, "argv", ["sej-branch", "prune-backups", "--keep", "1", "--db", str(main_db)])
    branch_main()
    out = capsys.readouterr().out
    assert "Pruned" in out


def test_cli_prune_nothing(main_db, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["sej-branch", "prune-backups", "--db", str(main_db)])
    branch_main()
    out = capsys.readouterr().out
    assert "Nothing to prune" in out


def test_cli_unknown_command(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["sej-branch", "foobar"])
    with pytest.raises(SystemExit):
        branch_main()
