"""Queries that reshape the normalized database back into spreadsheet-style rows."""

import sqlite3
from pathlib import Path

from sej.db import get_connection, create_schema


def _discover_months(conn: sqlite3.Connection) -> list[tuple[int, int]]:
    """Return sorted (year, month) pairs that exist in the efforts table."""
    rows = conn.execute(
        "SELECT DISTINCT year, month FROM efforts ORDER BY year, month"
    ).fetchall()
    return [(r["year"], r["month"]) for r in rows]


_MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _month_label(year: int, month: int) -> str:
    return f"{_MONTH_NAMES[month]} {year}"


def get_spreadsheet_rows(db_path: str | Path) -> tuple[list[str], list[list[str]]]:
    """Return (headers, rows) mimicking the original TSV layout.

    Employee names are blanked on continuation rows (same employee,
    subsequent allocation lines) to match the fill-forward style of
    the original spreadsheet.
    """
    conn = get_connection(db_path)
    create_schema(conn)  # ensure _meta/audit_log tables exist for older DBs
    months = _discover_months(conn)

    # Build the pivot expressions for each month
    month_selects = []
    for year, month in months:
        col_alias = _month_label(year, month)
        month_selects.append(
            f"MAX(CASE WHEN e.year = {year} AND e.month = {month} "
            f"THEN e.percentage END) AS [{col_alias}]"
        )

    month_sql = ", ".join(month_selects) if month_selects else "NULL AS no_months"

    sql = f"""
        SELECT
            emp.name   AS employee_name,
            g.name     AS group_name,
            al.fund_code,
            al.source,
            al.account,
            al.cost_code_1,
            al.cost_code_2,
            al.cost_code_3,
            al.program_code,
            p.project_code,
            p.name     AS project_name,
            {month_sql}
        FROM allocation_lines al
        JOIN employees emp ON emp.id = al.employee_id
        JOIN groups g       ON g.id  = emp.group_id
        JOIN projects p     ON p.id  = al.project_id
        LEFT JOIN efforts e ON e.allocation_line_id = al.id
        GROUP BY al.id
        ORDER BY emp.name, al.id
    """

    rows = conn.execute(sql).fetchall()
    conn.close()

    headers = [
        "Employee", "Group", "Fund Code", "Source", "Account",
        "Cost Code 1", "Cost Code 2", "Cost Code 3", "Program Code",
        "Project Id", "Project Name",
    ] + [_month_label(y, m) for y, m in months]

    result = []
    for row in rows:
        employee = row["employee_name"]

        project_code = row["project_code"]

        line = [
            employee,
            row["group_name"],
            row["fund_code"] or "",
            row["source"] or "",
            row["account"] or "",
            row["cost_code_1"] or "",
            row["cost_code_2"] or "",
            row["cost_code_3"] or "",
            row["program_code"] or "",
            project_code,
            row["project_name"] or "",
        ]

        for year, month in months:
            label = _month_label(year, month)
            pct = row[label]
            line.append(f"{pct:.2f}%" if pct is not None else "")

        result.append(line)

    return headers, result


def get_spreadsheet_rows_with_ids(db_path: str | Path) -> tuple[list[str], list[list]]:
    """Like get_spreadsheet_rows but includes allocation_line_id as the first column.

    This is needed for the edit UI so we can map cell edits back to the database.
    """
    conn = get_connection(db_path)
    create_schema(conn)
    months = _discover_months(conn)

    month_selects = []
    for year, month in months:
        col_alias = _month_label(year, month)
        month_selects.append(
            f"MAX(CASE WHEN e.year = {year} AND e.month = {month} "
            f"THEN e.percentage END) AS [{col_alias}]"
        )

    month_sql = ", ".join(month_selects) if month_selects else "NULL AS no_months"

    sql = f"""
        SELECT
            al.id      AS allocation_line_id,
            emp.name   AS employee_name,
            g.name     AS group_name,
            al.fund_code,
            al.source,
            al.account,
            al.cost_code_1,
            al.cost_code_2,
            al.cost_code_3,
            al.program_code,
            p.project_code,
            p.name     AS project_name,
            {month_sql}
        FROM allocation_lines al
        JOIN employees emp ON emp.id = al.employee_id
        JOIN groups g       ON g.id  = emp.group_id
        JOIN projects p     ON p.id  = al.project_id
        LEFT JOIN efforts e ON e.allocation_line_id = al.id
        GROUP BY al.id
        ORDER BY emp.name, al.id
    """

    rows = conn.execute(sql).fetchall()
    conn.close()

    headers = [
        "allocation_line_id",
        "Employee", "Group", "Fund Code", "Source", "Account",
        "Cost Code 1", "Cost Code 2", "Cost Code 3", "Program Code",
        "Project Id", "Project Name",
    ] + [_month_label(y, m) for y, m in months]

    result = []
    for row in rows:
        line = [
            row["allocation_line_id"],
            row["employee_name"],
            row["group_name"],
            row["fund_code"] or "",
            row["source"] or "",
            row["account"] or "",
            row["cost_code_1"] or "",
            row["cost_code_2"] or "",
            row["cost_code_3"] or "",
            row["program_code"] or "",
            row["project_code"],
            row["project_name"] or "",
        ]

        for year, month in months:
            label = _month_label(year, month)
            pct = row[label]
            line.append(f"{pct:.2f}%" if pct is not None else "")

        result.append(line)

    return headers, result


def get_employees(db_path: str | Path) -> list[dict]:
    """Return list of employees with their id, name, and group."""
    conn = get_connection(db_path)
    rows = conn.execute("""
        SELECT e.id, e.name, g.name AS group_name
        FROM employees e
        JOIN groups g ON g.id = e.group_id
        ORDER BY e.name
    """).fetchall()
    conn.close()
    return [{"id": r["id"], "name": r["name"], "group": r["group_name"]} for r in rows]


def get_projects(db_path: str | Path) -> list[dict]:
    """Return list of projects with their id, code, and name."""
    conn = get_connection(db_path)
    rows = conn.execute("""
        SELECT id, project_code, name
        FROM projects
        ORDER BY project_code
    """).fetchall()
    conn.close()
    return [{"id": r["id"], "project_code": r["project_code"], "name": r["name"]} for r in rows]


def get_branch_info(db_path: str | Path) -> dict:
    """Return branch metadata from _meta table."""
    conn = get_connection(db_path)
    create_schema(conn)
    rows = conn.execute("SELECT key, value FROM _meta").fetchall()
    conn.close()
    return {r["key"]: r["value"] for r in rows}


def update_effort(db_path: str | Path, allocation_line_id: int,
                  year: int, month: int, percentage: float | None) -> None:
    """Insert or update an effort percentage. If percentage is None, delete the row."""
    conn = get_connection(db_path)
    if percentage is None:
        conn.execute(
            "DELETE FROM efforts WHERE allocation_line_id = ? AND year = ? AND month = ?",
            (allocation_line_id, year, month),
        )
    else:
        conn.execute(
            """INSERT INTO efforts (allocation_line_id, year, month, percentage)
               VALUES (?, ?, ?, ?)
               ON CONFLICT (allocation_line_id, year, month)
               DO UPDATE SET percentage = excluded.percentage""",
            (allocation_line_id, year, month, percentage),
        )
    conn.commit()
    conn.close()


def add_allocation_line(db_path: str | Path, employee_name: str,
                        project_code: str, project_name: str | None = None) -> int:
    """Add a new allocation line for an employee and project.

    Creates the project if it doesn't exist. Returns the new allocation_line_id.
    """
    conn = get_connection(db_path)

    emp = conn.execute(
        "SELECT id FROM employees WHERE name = ?", (employee_name,)
    ).fetchone()
    if emp is None:
        raise ValueError(f"Employee not found: {employee_name}")

    proj = conn.execute(
        "SELECT id FROM projects WHERE project_code = ?", (project_code,)
    ).fetchone()
    if proj is None:
        cur = conn.execute(
            "INSERT INTO projects (project_code, name) VALUES (?, ?)",
            (project_code, project_name),
        )
        project_id = cur.lastrowid
    else:
        project_id = proj["id"]

    cur = conn.execute(
        "INSERT INTO allocation_lines (employee_id, project_id) VALUES (?, ?)",
        (emp["id"], project_id),
    )
    conn.commit()
    line_id = cur.lastrowid
    conn.close()
    return line_id
