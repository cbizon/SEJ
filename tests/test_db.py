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
    assert tables == {"groups", "employees", "projects", "allocation_lines", "efforts"}


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

    conn.execute("INSERT INTO projects (project_code, name) VALUES ('5120307', 'The Widget Project')")
    proj_id = conn.execute("SELECT id FROM projects WHERE project_code='5120307'").fetchone()[0]

    conn.execute(
        "INSERT INTO allocation_lines (employee_id, project_id, fund_code, account) VALUES (?, ?, '25210', '511120')",
        (emp_id, proj_id),
    )
    line_id = conn.execute("SELECT id FROM allocation_lines WHERE employee_id=?", (emp_id,)).fetchone()[0]

    conn.execute(
        "INSERT INTO efforts (allocation_line_id, year, month, percentage) VALUES (?, 2025, 7, 50.0)",
        (line_id,),
    )

    row = conn.execute(
        """
        SELECT e.name, p.project_code, al.fund_code, ef.year, ef.month, ef.percentage
        FROM efforts ef
        JOIN allocation_lines al ON al.id = ef.allocation_line_id
        JOIN employees e ON e.id = al.employee_id
        JOIN projects p ON p.id = al.project_id
        WHERE ef.year = 2025 AND ef.month = 7
        """
    ).fetchone()

    assert row["name"] == "Alice Smith"
    assert row["project_code"] == "5120307"
    assert row["fund_code"] == "25210"
    assert row["percentage"] == 50.0


def test_effort_duplicate_rejected(conn):
    conn.execute("INSERT INTO groups (name) VALUES ('Ops')")
    group_id = conn.execute("SELECT id FROM groups WHERE name='Ops'").fetchone()[0]
    conn.execute("INSERT INTO employees (name, group_id) VALUES ('Bob Jones', ?)", (group_id,))
    emp_id = conn.execute("SELECT id FROM employees WHERE name='Bob Jones'").fetchone()[0]
    conn.execute("INSERT INTO projects (project_code, name) VALUES ('Non-Project', NULL)")
    proj_id = conn.execute("SELECT id FROM projects WHERE project_code='Non-Project'").fetchone()[0]
    conn.execute("INSERT INTO allocation_lines (employee_id, project_id) VALUES (?, ?)", (emp_id, proj_id))
    line_id = conn.execute("SELECT id FROM allocation_lines WHERE employee_id=?", (emp_id,)).fetchone()[0]

    conn.execute("INSERT INTO efforts (allocation_line_id, year, month, percentage) VALUES (?, 2025, 8, 100.0)", (line_id,))
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO efforts (allocation_line_id, year, month, percentage) VALUES (?, 2025, 8, 80.0)", (line_id,))


def test_effort_month_constraint(conn):
    conn.execute("INSERT INTO groups (name) VALUES ('QA')")
    group_id = conn.execute("SELECT id FROM groups WHERE name='QA'").fetchone()[0]
    conn.execute("INSERT INTO employees (name, group_id) VALUES ('Carol White', ?)", (group_id,))
    emp_id = conn.execute("SELECT id FROM employees WHERE name='Carol White'").fetchone()[0]
    conn.execute("INSERT INTO projects (project_code, name) VALUES ('9999999', 'Test Project')")
    proj_id = conn.execute("SELECT id FROM projects WHERE project_code='9999999'").fetchone()[0]
    conn.execute("INSERT INTO allocation_lines (employee_id, project_id) VALUES (?, ?)", (emp_id, proj_id))
    line_id = conn.execute("SELECT id FROM allocation_lines WHERE employee_id=?", (emp_id,)).fetchone()[0]

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO efforts (allocation_line_id, year, month, percentage) VALUES (?, 2025, 13, 50.0)", (line_id,))
