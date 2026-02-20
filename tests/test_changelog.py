import json

import pytest

from sej.db import get_connection, create_schema
from sej.changelog import (
    get_open_change_set,
    record_change,
    create_change_set,
    merge_change_set,
    discard_change_set,
    get_change_set_info,
)


@pytest.fixture
def db_path(tmp_path):
    """Create a minimal database with schema."""
    path = tmp_path / "test.db"
    conn = get_connection(path)
    create_schema(conn)
    # Insert a group, employee, project, budget_line, allocation_line, and effort
    conn.execute("INSERT INTO groups (id, name) VALUES (1, 'Engineering')")
    conn.execute("INSERT INTO employees (id, name, group_id) VALUES (1, 'Smith,Jane', 1)")
    conn.execute("INSERT INTO projects (id, name) VALUES (1, 'Widget Project')")
    conn.execute(
        "INSERT INTO budget_lines (id, project_id, budget_line_code, display_name) "
        "VALUES (1, 1, '5120001', 'Widget Project')"
    )
    conn.execute(
        "INSERT INTO allocation_lines (id, employee_id, budget_line_id) VALUES (1, 1, 1)"
    )
    conn.execute(
        "INSERT INTO efforts (id, allocation_line_id, year, month, percentage) "
        "VALUES (1, 1, 2025, 7, 50.0)"
    )
    conn.commit()
    conn.close()
    return path


def test_no_open_change_set(db_path):
    conn = get_connection(db_path)
    assert get_open_change_set(conn) is None
    conn.close()


def test_create_change_set(db_path):
    name = create_change_set(db_path)
    assert name.startswith("edit-")
    conn = get_connection(db_path)
    cs_id = get_open_change_set(conn)
    assert cs_id is not None
    conn.close()


def test_create_change_set_duplicate_raises(db_path):
    create_change_set(db_path)
    with pytest.raises(ValueError, match="already open"):
        create_change_set(db_path)


def test_record_change_no_op_without_open_set(db_path):
    """record_change is a no-op when no change_set is open."""
    conn = get_connection(db_path)
    create_schema(conn)
    record_change(conn, "efforts", "update", 1, {"percentage": 50.0}, {"percentage": 80.0})
    count = conn.execute("SELECT COUNT(*) FROM change_log").fetchone()[0]
    assert count == 0
    conn.close()


def test_record_change_writes_entry(db_path):
    create_change_set(db_path)
    conn = get_connection(db_path)
    record_change(conn, "efforts", "update", 1, {"percentage": 50.0}, {"percentage": 80.0})
    conn.commit()
    row = conn.execute("SELECT * FROM change_log").fetchone()
    assert row["table_name"] == "efforts"
    assert row["operation"] == "update"
    assert row["row_id"] == 1
    assert json.loads(row["old_values"]) == {"percentage": 50.0}
    assert json.loads(row["new_values"]) == {"percentage": 80.0}
    conn.close()


def test_record_change_increments_seq(db_path):
    create_change_set(db_path)
    conn = get_connection(db_path)
    record_change(conn, "efforts", "update", 1, {"percentage": 50.0}, {"percentage": 60.0})
    record_change(conn, "efforts", "update", 1, {"percentage": 60.0}, {"percentage": 70.0})
    conn.commit()
    rows = conn.execute("SELECT seq FROM change_log ORDER BY seq").fetchall()
    assert [r["seq"] for r in rows] == [1, 2]
    conn.close()


def test_merge_change_set(db_path):
    create_change_set(db_path)
    conn = get_connection(db_path)
    record_change(conn, "efforts", "update", 1, {"percentage": 50.0}, {"percentage": 80.0})
    conn.commit()
    conn.close()

    count = merge_change_set(db_path)
    assert count == 1

    conn = get_connection(db_path)
    cs = conn.execute("SELECT status FROM change_sets").fetchone()
    assert cs["status"] == "merged"

    audit = conn.execute("SELECT * FROM audit_log WHERE action = 'merge'").fetchone()
    assert audit is not None
    details = json.loads(audit["details"])
    assert details["changes_count"] == 1
    conn.close()


def test_merge_no_open_set_raises(db_path):
    with pytest.raises(ValueError, match="No open change set"):
        merge_change_set(db_path)


def test_discard_undoes_update(db_path):
    """Discarding a change_set with an update reverts the row."""
    create_change_set(db_path)
    conn = get_connection(db_path)
    # Simulate an update: change effort percentage
    conn.execute("UPDATE efforts SET percentage = 80.0 WHERE id = 1")
    record_change(conn, "efforts", "update", 1, {"percentage": 50.0}, {"percentage": 80.0})
    conn.commit()
    conn.close()

    discard_change_set(db_path)

    conn = get_connection(db_path)
    row = conn.execute("SELECT percentage FROM efforts WHERE id = 1").fetchone()
    assert row["percentage"] == 50.0
    cs = conn.execute("SELECT status FROM change_sets").fetchone()
    assert cs["status"] == "discarded"
    conn.close()


def test_discard_undoes_insert(db_path):
    """Discarding a change_set with an insert removes the inserted row."""
    create_change_set(db_path)
    conn = get_connection(db_path)
    conn.execute(
        "INSERT INTO efforts (id, allocation_line_id, year, month, percentage) "
        "VALUES (99, 1, 2025, 8, 30.0)"
    )
    record_change(conn, "efforts", "insert", 99, None, {
        "allocation_line_id": 1, "year": 2025, "month": 8, "percentage": 30.0,
    })
    conn.commit()
    conn.close()

    discard_change_set(db_path)

    conn = get_connection(db_path)
    row = conn.execute("SELECT * FROM efforts WHERE id = 99").fetchone()
    assert row is None
    conn.close()


def test_discard_undoes_delete(db_path):
    """Discarding a change_set with a delete re-inserts the row."""
    create_change_set(db_path)
    conn = get_connection(db_path)
    conn.execute("DELETE FROM efforts WHERE id = 1")
    record_change(conn, "efforts", "delete", 1, {
        "allocation_line_id": 1, "year": 2025, "month": 7, "percentage": 50.0,
    }, None)
    conn.commit()
    conn.close()

    discard_change_set(db_path)

    conn = get_connection(db_path)
    row = conn.execute("SELECT * FROM efforts WHERE id = 1").fetchone()
    assert row is not None
    assert row["percentage"] == 50.0
    conn.close()


def test_discard_multiple_changes_reverse_order(db_path):
    """Multiple changes are undone in reverse sequence order."""
    create_change_set(db_path)
    conn = get_connection(db_path)

    # Change 1: update effort from 50 to 60
    conn.execute("UPDATE efforts SET percentage = 60.0 WHERE id = 1")
    record_change(conn, "efforts", "update", 1, {"percentage": 50.0}, {"percentage": 60.0})

    # Change 2: update effort from 60 to 70
    conn.execute("UPDATE efforts SET percentage = 70.0 WHERE id = 1")
    record_change(conn, "efforts", "update", 1, {"percentage": 60.0}, {"percentage": 70.0})

    conn.commit()
    conn.close()

    discard_change_set(db_path)

    conn = get_connection(db_path)
    row = conn.execute("SELECT percentage FROM efforts WHERE id = 1").fetchone()
    assert row["percentage"] == 50.0
    conn.close()


def test_discard_no_open_set_raises(db_path):
    with pytest.raises(ValueError, match="No open change set"):
        discard_change_set(db_path)


def test_discard_logs_audit(db_path):
    create_change_set(db_path)
    discard_change_set(db_path)

    conn = get_connection(db_path)
    audit = conn.execute("SELECT * FROM audit_log WHERE action = 'discard'").fetchone()
    assert audit is not None
    conn.close()


def test_get_change_set_info_none(db_path):
    info = get_change_set_info(db_path)
    assert info["status"] == "none"


def test_get_change_set_info_open(db_path):
    name = create_change_set(db_path)
    info = get_change_set_info(db_path)
    assert info["status"] == "open"
    assert info["name"] == name
    assert "id" in info


def test_get_change_set_info_after_merge(db_path):
    create_change_set(db_path)
    merge_change_set(db_path)
    info = get_change_set_info(db_path)
    assert info["status"] == "none"


def test_can_create_after_merge(db_path):
    """After merging, a new change_set can be created."""
    create_change_set(db_path)
    merge_change_set(db_path)
    name2 = create_change_set(db_path)
    assert name2.startswith("edit-")


def test_can_create_after_discard(db_path):
    """After discarding, a new change_set can be created."""
    create_change_set(db_path)
    discard_change_set(db_path)
    name2 = create_change_set(db_path)
    assert name2.startswith("edit-")
