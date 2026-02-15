"""Queries that reshape the normalized database back into spreadsheet-style rows."""

import sqlite3
from pathlib import Path

from sej.db import get_connection


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
