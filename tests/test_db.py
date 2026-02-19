import pytest
import sqlite3
from sej.db import get_connection, create_schema


@pytest.fixture
def conn():
    """In-memory database with schema applied."""
    c = get_connection(":memory:")
    create_schema(c)
    yield c
    c.close()


def test_schema_creates_all_tables(conn):
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    assert tables == {"groups", "employees", "projects", "budget_lines", "allocation_lines", "efforts", "_meta", "audit_log"}


def test_create_schema_is_idempotent(conn):
    """Calling create_schema a second time should not raise."""
    create_schema(conn)


def test_foreign_keys_are_enforced(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO employees (name, group_id) VALUES ('Alice', 999)")


def test_insert_and_retrieve_full_chain(conn):
    conn.execute("INSERT INTO groups (name) VALUES ('Engineering')")
    group_id = conn.execute("SELECT id FROM groups WHERE name='Engineering'").fetchone()[0]

    conn.execute("INSERT INTO employees (name, group_id) VALUES ('Alice Smith', ?)", (group_id,))
    emp_id = conn.execute("SELECT id FROM employees WHERE name='Alice Smith'").fetchone()[0]

    conn.execute("INSERT INTO projects (name) VALUES ('The Widget Project')")
    proj_id = conn.execute("SELECT id FROM projects WHERE name='The Widget Project'").fetchone()[0]

    conn.execute(
        "INSERT INTO budget_lines (project_id, budget_line_code, name) VALUES (?, '5120307', 'The Widget Project')",
        (proj_id,),
    )
    bl_id = conn.execute("SELECT id FROM budget_lines WHERE budget_line_code='5120307'").fetchone()[0]

    conn.execute(
        "INSERT INTO allocation_lines (employee_id, budget_line_id, fund_code, account) VALUES (?, ?, '25210', '511120')",
        (emp_id, bl_id),
    )
    line_id = conn.execute("SELECT id FROM allocation_lines WHERE employee_id=?", (emp_id,)).fetchone()[0]

    conn.execute(
        "INSERT INTO efforts (allocation_line_id, year, month, percentage) VALUES (?, 2025, 7, 50.0)",
        (line_id,),
    )

    row = conn.execute(
        """
        SELECT e.name, bl.budget_line_code, al.fund_code, ef.year, ef.month, ef.percentage
        FROM efforts ef
        JOIN allocation_lines al ON al.id = ef.allocation_line_id
        JOIN employees e ON e.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE ef.year = 2025 AND ef.month = 7
        """
    ).fetchone()

    assert row["name"] == "Alice Smith"
    assert row["budget_line_code"] == "5120307"
    assert row["fund_code"] == "25210"
    assert row["percentage"] == 50.0


def test_effort_duplicate_rejected(conn):
    conn.execute("INSERT INTO groups (name) VALUES ('Ops')")
    group_id = conn.execute("SELECT id FROM groups WHERE name='Ops'").fetchone()[0]
    conn.execute("INSERT INTO employees (name, group_id) VALUES ('Bob Jones', ?)", (group_id,))
    emp_id = conn.execute("SELECT id FROM employees WHERE name='Bob Jones'").fetchone()[0]

    conn.execute("INSERT INTO projects (name, is_nonproject) VALUES ('Non-Project', 1)")
    proj_id = conn.execute("SELECT id FROM projects WHERE name='Non-Project'").fetchone()[0]
    conn.execute("INSERT INTO budget_lines (project_id, budget_line_code) VALUES (?, 'Non-Project')", (proj_id,))
    bl_id = conn.execute("SELECT id FROM budget_lines WHERE budget_line_code='Non-Project'").fetchone()[0]

    conn.execute("INSERT INTO allocation_lines (employee_id, budget_line_id) VALUES (?, ?)", (emp_id, bl_id))
    line_id = conn.execute("SELECT id FROM allocation_lines WHERE employee_id=?", (emp_id,)).fetchone()[0]

    conn.execute("INSERT INTO efforts (allocation_line_id, year, month, percentage) VALUES (?, 2025, 8, 100.0)", (line_id,))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO efforts (allocation_line_id, year, month, percentage) VALUES (?, 2025, 8, 80.0)", (line_id,))


def test_effort_month_constraint(conn):
    conn.execute("INSERT INTO groups (name) VALUES ('QA')")
    group_id = conn.execute("SELECT id FROM groups WHERE name='QA'").fetchone()[0]
    conn.execute("INSERT INTO employees (name, group_id) VALUES ('Carol White', ?)", (group_id,))
    emp_id = conn.execute("SELECT id FROM employees WHERE name='Carol White'").fetchone()[0]

    conn.execute("INSERT INTO projects (name) VALUES ('Test Project')")
    proj_id = conn.execute("SELECT id FROM projects WHERE name='Test Project'").fetchone()[0]
    conn.execute("INSERT INTO budget_lines (project_id, budget_line_code, name) VALUES (?, '9999999', 'Test Project')", (proj_id,))
    bl_id = conn.execute("SELECT id FROM budget_lines WHERE budget_line_code='9999999'").fetchone()[0]

    conn.execute("INSERT INTO allocation_lines (employee_id, budget_line_id) VALUES (?, ?)", (emp_id, bl_id))
    line_id = conn.execute("SELECT id FROM allocation_lines WHERE employee_id=?", (emp_id,)).fetchone()[0]

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO efforts (allocation_line_id, year, month, percentage) VALUES (?, 2025, 13, 50.0)", (line_id,))


def test_group_is_internal_defaults_to_1(conn):
    conn.execute("INSERT INTO groups (name) VALUES ('Alpha')")
    row = conn.execute("SELECT is_internal FROM groups WHERE name='Alpha'").fetchone()
    assert row["is_internal"] == 1


def test_group_is_internal_can_be_set_to_0(conn):
    conn.execute("INSERT INTO groups (name, is_internal) VALUES ('External Org', 0)")
    row = conn.execute("SELECT is_internal FROM groups WHERE name='External Org'").fetchone()
    assert row["is_internal"] == 0


def test_create_schema_migrates_missing_is_internal():
    """create_schema adds is_internal to an existing groups table that lacks it."""
    import sqlite3 as _sqlite3
    c = _sqlite3.connect(":memory:")
    c.row_factory = _sqlite3.Row
    # Simulate an old schema without is_internal
    c.execute("CREATE TABLE groups (id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL)")
    c.execute("INSERT INTO groups (name) VALUES ('OldGroup')")
    c.commit()

    create_schema(c)

    cols = {r[1] for r in c.execute("PRAGMA table_info(groups)").fetchall()}
    assert "is_internal" in cols
    row = c.execute("SELECT is_internal FROM groups WHERE name='OldGroup'").fetchone()
    assert row["is_internal"] == 1  # migration default applied to existing rows
    c.close()
