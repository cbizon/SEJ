"""Queries that reshape the normalized database back into spreadsheet-style rows."""

import json
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


def get_groups(db_path: str | Path) -> list[str]:
    """Return sorted list of group names."""
    conn = get_connection(db_path)
    rows = conn.execute("SELECT name FROM groups ORDER BY name").fetchall()
    conn.close()
    return [r["name"] for r in rows]


def get_group_details(db_path: str | Path, group_name: str) -> dict:
    """Return per-person Non-Project percentages and per-project effort totals for a group.

    Returns a dict with:
        months:   list of month label strings in chronological order
        people:   list of {name, <month>: np_pct, ...} dicts, sorted by name, with a
                  trailing Total row showing the group-level Non-Project percentage
        projects: list of {project_code, project_name, <month>: total_effort, ...} dicts,
                  sorted by project_code
    """
    conn = get_connection(db_path)
    create_schema(conn)
    months = _discover_months(conn)

    person_rows = conn.execute("""
        SELECT
            emp.name AS employee_name,
            e.year,
            e.month,
            SUM(CASE WHEN p.project_code = 'Non-Project' THEN e.percentage ELSE 0 END) AS np_effort,
            SUM(e.percentage) AS total_effort
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN groups g ON g.id = emp.group_id
        JOIN projects p ON p.id = al.project_id
        WHERE g.name = ?
        GROUP BY emp.id, e.year, e.month
        ORDER BY emp.name, e.year, e.month
    """, (group_name,)).fetchall()

    project_rows = conn.execute("""
        SELECT
            p.project_code,
            p.name AS project_name,
            e.year,
            e.month,
            SUM(e.percentage) AS total_effort
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN groups g ON g.id = emp.group_id
        JOIN projects p ON p.id = al.project_id
        WHERE g.name = ?
        GROUP BY p.id, e.year, e.month
        ORDER BY p.project_code, e.year, e.month
    """, (group_name,)).fetchall()

    group_total_rows = conn.execute("""
        SELECT
            e.year,
            e.month,
            SUM(CASE WHEN p.project_code = 'Non-Project' THEN e.percentage ELSE 0 END) AS np_effort,
            SUM(e.percentage) AS total_effort
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN groups g ON g.id = emp.group_id
        JOIN projects p ON p.id = al.project_id
        WHERE g.name = ?
        GROUP BY e.year, e.month
        ORDER BY e.year, e.month
    """, (group_name,)).fetchall()
    conn.close()

    month_labels = [_month_label(y, m) for y, m in months]

    # Per-person Non-Project percentage table
    person_data: dict[str, dict[str, float]] = {}
    for r in person_rows:
        name = r["employee_name"]
        label = _month_label(r["year"], r["month"])
        total = r["total_effort"]
        pct = (r["np_effort"] / total * 100.0) if total else 0.0
        person_data.setdefault(name, {})[label] = pct

    people_result: list[dict] = []
    for name in sorted(person_data.keys()):
        row: dict = {"name": name}
        for label in month_labels:
            row[label] = round(person_data[name].get(label, 0.0), 1)
        people_result.append(row)

    # Total row: group-level Non-Project percentage
    total_row: dict = {"name": "Total"}
    for r in group_total_rows:
        label = _month_label(r["year"], r["month"])
        t = r["total_effort"]
        total_row[label] = round((r["np_effort"] / t * 100.0) if t else 0.0, 1)
    for label in month_labels:
        total_row.setdefault(label, 0.0)
    people_result.append(total_row)

    # Per-project total effort table
    project_data: dict[str, dict[str, float]] = {}
    project_names: dict[str, str] = {}
    for r in project_rows:
        code = r["project_code"]
        label = _month_label(r["year"], r["month"])
        project_data.setdefault(code, {})[label] = r["total_effort"]
        project_names[code] = r["project_name"] or ""

    projects_result: list[dict] = []
    for code in sorted(project_data.keys()):
        row = {"project_code": code, "project_name": project_names[code]}
        for label in month_labels:
            row[label] = round(project_data[code].get(label, 0.0), 1)
        projects_result.append(row)

    return {"months": month_labels, "people": people_result, "projects": projects_result}


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


def fix_totals(db_path: str | Path) -> list[dict]:
    """Adjust each employee's Non-Project effort so monthly totals sum to 100%.

    Under 100: add the shortfall to the preferred Non-Project line (fund_code
    '20152'), creating one if needed.

    Over 100: reduce Non-Project lines until the total reaches 100, starting
    with the preferred line.  If all NP effort is zeroed and it's still over
    100, stop — can't fix it.

    Returns a list of dicts describing each change:
    {allocation_line_id, year, month, old_percentage, new_percentage}.
    """
    conn = get_connection(db_path)
    months = _discover_months(conn)

    # Collect ALL Non-Project lines per employee, preferred first.
    np_rows = conn.execute("""
        SELECT al.employee_id, al.id AS line_id
        FROM allocation_lines al
        JOIN projects p ON p.id = al.project_id
        WHERE p.project_code = 'Non-Project'
        ORDER BY al.employee_id,
                 CASE WHEN al.fund_code = '20152' THEN 0 ELSE 1 END,
                 al.id
    """).fetchall()

    np_lines_for_emp = {}
    for row in np_rows:
        emp_id = row["employee_id"]
        np_lines_for_emp.setdefault(emp_id, []).append(row["line_id"])

    changes = []
    for year, month in months:
        emp_totals = conn.execute("""
            SELECT al.employee_id, SUM(e.percentage) AS total
            FROM efforts e
            JOIN allocation_lines al ON al.id = e.allocation_line_id
            WHERE e.year = ? AND e.month = ?
            GROUP BY al.employee_id
        """, (year, month)).fetchall()

        for row in emp_totals:
            emp_id = row["employee_id"]
            total = row["total"]

            if abs(total - 100) <= 0.01:
                continue

            diff = 100.0 - total  # positive = under, negative = over

            all_np_lines = np_lines_for_emp.get(emp_id, [])

            if diff > 0:
                # Under 100 — add shortfall to the preferred NP line.
                if not all_np_lines:
                    np_project = conn.execute(
                        "SELECT id FROM projects WHERE project_code = 'Non-Project'"
                    ).fetchone()
                    if np_project is None:
                        continue
                    cur = conn.execute(
                        "INSERT INTO allocation_lines (employee_id, project_id)"
                        " VALUES (?, ?)",
                        (emp_id, np_project["id"]),
                    )
                    conn.commit()
                    all_np_lines = [cur.lastrowid]
                    np_lines_for_emp[emp_id] = all_np_lines

                # Pick the NP line with the most effort this month so we
                # bump an existing visible row, not an empty one.
                best_line = all_np_lines[0]
                best_pct = -1.0
                for np_lid in all_np_lines:
                    eff = conn.execute(
                        "SELECT percentage FROM efforts"
                        " WHERE allocation_line_id = ? AND year = ? AND month = ?",
                        (np_lid, year, month),
                    ).fetchone()
                    pct = eff["percentage"] if eff else 0.0
                    if pct > best_pct:
                        best_pct = pct
                        best_line = np_lid
                old_pct = best_pct if best_pct > 0 else 0.0
                changes.append({
                    "allocation_line_id": best_line,
                    "year": year, "month": month,
                    "old_percentage": old_pct,
                    "new_percentage": old_pct + diff,
                })
            else:
                # Over 100 — reduce NP lines until we've cut enough.
                excess = -diff  # positive amount to remove
                for np_lid in all_np_lines:
                    if excess <= 0.01:
                        break
                    eff = conn.execute(
                        "SELECT percentage FROM efforts"
                        " WHERE allocation_line_id = ? AND year = ? AND month = ?",
                        (np_lid, year, month),
                    ).fetchone()
                    if eff is None:
                        continue
                    old_pct = eff["percentage"]
                    if old_pct <= 0.01:
                        continue
                    cut = min(old_pct, excess)
                    changes.append({
                        "allocation_line_id": np_lid,
                        "year": year, "month": month,
                        "old_percentage": old_pct,
                        "new_percentage": old_pct - cut,
                    })
                    excess -= cut

    conn.close()

    for c in changes:
        pct = c["new_percentage"]
        update_effort(db_path, c["allocation_line_id"], c["year"], c["month"],
                      pct if pct > 0.01 else None)

    return changes


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


def get_nonproject_by_group(db_path: str | Path) -> dict:
    """Return Non-Project effort by group and month.

    Returns a dict with:
        months:    list of month label strings in chronological order
        rows:      {group, <month>: np_pct, ...} — average NP % per group
        fte_rows:  {group, <month>: fte, ...} — NP FTE (avg_pct * employee_count)
    """
    conn = get_connection(db_path)
    create_schema(conn)
    months = _discover_months(conn)

    group_rows = conn.execute("""
        SELECT
            g.name AS group_name,
            e.year,
            e.month,
            SUM(CASE WHEN p.project_code = 'Non-Project' THEN e.percentage ELSE 0 END) AS np_effort,
            SUM(e.percentage) AS total_effort,
            COUNT(DISTINCT emp.id) AS employee_count
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN groups g ON g.id = emp.group_id
        JOIN projects p ON p.id = al.project_id
        GROUP BY g.id, e.year, e.month
        ORDER BY g.name, e.year, e.month
    """).fetchall()

    total_rows = conn.execute("""
        SELECT
            e.year,
            e.month,
            SUM(CASE WHEN p.project_code = 'Non-Project' THEN e.percentage ELSE 0 END) AS np_effort,
            SUM(e.percentage) AS total_effort,
            COUNT(DISTINCT emp.id) AS employee_count
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN projects p ON p.id = al.project_id
        GROUP BY e.year, e.month
        ORDER BY e.year, e.month
    """).fetchall()
    conn.close()

    month_labels = [_month_label(y, m) for y, m in months]

    pct_data: dict[str, dict[str, float]] = {}
    fte_data: dict[str, dict[str, float]] = {}
    for r in group_rows:
        g = r["group_name"]
        label = _month_label(r["year"], r["month"])
        total = r["total_effort"]
        np_e = r["np_effort"]
        emp_count = r["employee_count"]
        pct = (np_e / total * 100.0) if total else 0.0
        fte = (np_e / total * emp_count) if total else 0.0
        pct_data.setdefault(g, {})[label] = pct
        fte_data.setdefault(g, {})[label] = fte

    result_rows = []
    fte_rows = []
    for g in sorted(pct_data.keys()):
        pct_row: dict = {"group": g}
        fte_row: dict = {"group": g}
        for label in month_labels:
            pct_row[label] = round(pct_data[g].get(label, 0.0), 1)
            fte_row[label] = round(fte_data[g].get(label, 0.0), 2)
        result_rows.append(pct_row)
        fte_rows.append(fte_row)

    # Total rows
    pct_total: dict = {"group": "Total"}
    fte_total: dict = {"group": "Total"}
    for r in total_rows:
        label = _month_label(r["year"], r["month"])
        t = r["total_effort"]
        np_e = r["np_effort"]
        emp_count = r["employee_count"]
        pct_total[label] = round((np_e / t * 100.0) if t else 0.0, 1)
        fte_total[label] = round((np_e / t * emp_count) if t else 0.0, 2)
    for label in month_labels:
        pct_total.setdefault(label, 0.0)
        fte_total.setdefault(label, 0.0)
    result_rows.append(pct_total)
    fte_rows.append(fte_total)

    return {"months": month_labels, "rows": result_rows, "fte_rows": fte_rows}


def get_audit_log(db_path: str | Path) -> list[dict]:
    """Return audit log entries, most recent first.

    Each entry has: id, timestamp, action, details (parsed dict).
    """
    conn = get_connection(db_path)
    create_schema(conn)
    rows = conn.execute(
        "SELECT id, timestamp, action, details FROM audit_log ORDER BY id DESC"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        details = json.loads(r["details"]) if r["details"] else {}
        result.append({
            "id": r["id"],
            "timestamp": r["timestamp"],
            "action": r["action"],
            "details": details,
        })
    return result
