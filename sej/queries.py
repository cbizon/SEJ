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


def get_groups(db_path: str | Path) -> list[dict]:
    """Return sorted list of groups with id, name and is_internal flag."""
    conn = get_connection(db_path)
    create_schema(conn)
    rows = conn.execute("SELECT id, name, is_internal FROM groups ORDER BY name").fetchall()
    conn.close()
    return [{"id": r["id"], "name": r["name"], "is_internal": bool(r["is_internal"])} for r in rows]


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
    """Return list of employees with their id, name, group, and is_internal flag."""
    conn = get_connection(db_path)
    rows = conn.execute("""
        SELECT e.id, e.name, g.name AS group_name, g.is_internal
        FROM employees e
        JOIN groups g ON g.id = e.group_id
        ORDER BY e.name
    """).fetchall()
    conn.close()
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "group": r["group_name"],
            "is_internal": bool(r["is_internal"]),
        }
        for r in rows
    ]


def get_projects(db_path: str | Path) -> list[dict]:
    """Return list of projects with their id, code, name, and detail fields."""
    conn = get_connection(db_path)
    rows = conn.execute("""
        SELECT p.id, p.project_code, p.name,
               p.start_year, p.start_month, p.end_year, p.end_month,
               p.local_pi_id, e.name AS local_pi_name,
               p.personnel_budget,
               p.admin_group_id, g.name AS admin_group_name
        FROM projects p
        LEFT JOIN employees e ON e.id = p.local_pi_id
        LEFT JOIN groups g ON g.id = p.admin_group_id
        ORDER BY p.project_code
    """).fetchall()
    conn.close()
    return [
        {
            "id": r["id"],
            "project_code": r["project_code"],
            "name": r["name"],
            "start_year": r["start_year"],
            "start_month": r["start_month"],
            "end_year": r["end_year"],
            "end_month": r["end_month"],
            "local_pi_id": r["local_pi_id"],
            "local_pi_name": r["local_pi_name"],
            "personnel_budget": r["personnel_budget"],
            "admin_group_id": r["admin_group_id"],
            "admin_group_name": r["admin_group_name"],
        }
        for r in rows
    ]


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

    # Collect ALL Non-Project lines per internal employee, preferred first.
    np_rows = conn.execute("""
        SELECT al.employee_id, al.id AS line_id
        FROM allocation_lines al
        JOIN employees emp ON emp.id = al.employee_id
        JOIN groups g ON g.id = emp.group_id
        JOIN projects p ON p.id = al.project_id
        WHERE p.project_code = 'Non-Project'
        AND g.is_internal = 1
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
            JOIN employees emp ON emp.id = al.employee_id
            JOIN groups g ON g.id = emp.group_id
            WHERE e.year = ? AND e.month = ?
            AND g.is_internal = 1
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
                        project_code: str) -> int:
    """Add a new allocation line for an employee and project.

    Returns the new allocation_line_id. Raises ValueError if the employee
    or project is not found.
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
        raise ValueError(f"Project not found: {project_code}")

    cur = conn.execute(
        "INSERT INTO allocation_lines (employee_id, project_id) VALUES (?, ?)",
        (emp["id"], proj["id"]),
    )
    conn.commit()
    line_id = cur.lastrowid
    conn.close()
    return line_id


def add_employee(db_path: str | Path, last_name: str, first_name: str,
                 middle_name: str, group_name: str) -> int:
    """Add a new employee to the given group.

    Name is stored as 'LastName,FirstName' or 'LastName,FirstName Middle'
    if a middle name is provided. Returns the new employee id.
    Raises ValueError if the group is not found.
    """
    name = f"{last_name},{first_name}"
    if middle_name:
        name = f"{name} {middle_name}"

    conn = get_connection(db_path)
    group = conn.execute(
        "SELECT id FROM groups WHERE name = ?", (group_name,)
    ).fetchone()
    if group is None:
        conn.close()
        raise ValueError(f"Group not found: {group_name}")

    cur = conn.execute(
        "INSERT INTO employees (name, group_id) VALUES (?, ?)",
        (name, group["id"]),
    )
    conn.commit()
    emp_id = cur.lastrowid
    conn.close()
    return emp_id


def add_group(db_path: str | Path, name: str, is_internal: bool) -> int:
    """Add a new group. Returns the new group id. Raises ValueError if name already exists."""
    conn = get_connection(db_path)
    existing = conn.execute("SELECT id FROM groups WHERE name = ?", (name,)).fetchone()
    if existing:
        conn.close()
        raise ValueError(f"Group already exists: {name}")
    cur = conn.execute(
        "INSERT INTO groups (name, is_internal) VALUES (?, ?)",
        (name, 1 if is_internal else 0),
    )
    conn.commit()
    group_id = cur.lastrowid
    conn.close()
    return group_id


_MONTH_ABBR = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _validate_local_pi(conn: sqlite3.Connection, local_pi_id: int) -> None:
    """Raise ValueError if the employee is not internal."""
    row = conn.execute(
        "SELECT g.is_internal FROM employees e JOIN groups g ON g.id = e.group_id WHERE e.id = ?",
        (local_pi_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"Employee not found: {local_pi_id}")
    if not row["is_internal"]:
        raise ValueError(f"Local PI must be an internal employee (id={local_pi_id})")


def _validate_project_dates(
    conn: sqlite3.Connection,
    project_id: int,
    start_year: int | None,
    start_month: int | None,
    end_year: int | None,
    end_month: int | None,
) -> None:
    """Raise ValueError if any effort for the project falls outside the date bounds.

    Only checks a bound when both the year and month for that bound are provided.
    """
    rows = conn.execute("""
        SELECT DISTINCT e.year, e.month
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        WHERE al.project_id = ?
        ORDER BY e.year, e.month
    """, (project_id,)).fetchall()

    if not rows:
        return

    def fmt(year: int, month: int) -> str:
        return f"{_MONTH_ABBR[month]} {year}"

    if start_year is not None and start_month is not None:
        start_ym = start_year * 12 + start_month
        early = [fmt(r["year"], r["month"]) for r in rows
                 if r["year"] * 12 + r["month"] < start_ym]
        if early:
            raise ValueError(
                f"Effort exists before project start ({fmt(start_year, start_month)}): "
                + ", ".join(early)
            )

    if end_year is not None and end_month is not None:
        end_ym = end_year * 12 + end_month
        late = [fmt(r["year"], r["month"]) for r in rows
                if r["year"] * 12 + r["month"] > end_ym]
        if late:
            raise ValueError(
                f"Effort exists after project end ({fmt(end_year, end_month)}): "
                + ", ".join(late)
            )


def add_project(
    db_path: str | Path,
    name: str,
    start_year: int | None = None,
    start_month: int | None = None,
    end_year: int | None = None,
    end_month: int | None = None,
    local_pi_id: int | None = None,
    personnel_budget: float | None = None,
    admin_group_id: int | None = None,
) -> str:
    """Add a new project with an auto-generated project code.

    Finds the maximum numeric project code and increments by 1.
    Returns the new project_code.
    Raises ValueError if local_pi_id refers to an external employee.
    """
    conn = get_connection(db_path)
    if local_pi_id is not None:
        _validate_local_pi(conn, local_pi_id)
    row = conn.execute("""
        SELECT MAX(CAST(project_code AS INTEGER)) AS max_code
        FROM projects
        WHERE project_code GLOB '[0-9]*'
    """).fetchone()
    max_code = row["max_code"] if row["max_code"] is not None else 0
    new_code = str(max_code + 1)
    conn.execute(
        """INSERT INTO projects
           (project_code, name, start_year, start_month, end_year, end_month,
            local_pi_id, personnel_budget, admin_group_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (new_code, name, start_year, start_month, end_year, end_month,
         local_pi_id, personnel_budget, admin_group_id),
    )
    conn.commit()
    conn.close()
    return new_code


def update_project(
    db_path: str | Path,
    project_code: str,
    name: str | None = None,
    start_year: int | None = None,
    start_month: int | None = None,
    end_year: int | None = None,
    end_month: int | None = None,
    local_pi_id: int | None = None,
    personnel_budget: float | None = None,
    admin_group_id: int | None = None,
) -> None:
    """Update detail fields on an existing project.

    Only fields that are explicitly passed (including None to clear) are updated.
    Raises ValueError if the project_code does not exist, or if local_pi_id
    refers to an external employee.
    """
    conn = get_connection(db_path)
    existing = conn.execute(
        "SELECT id FROM projects WHERE project_code = ?", (project_code,)
    ).fetchone()
    if existing is None:
        conn.close()
        raise ValueError(f"Project not found: {project_code}")
    if local_pi_id is not None:
        _validate_local_pi(conn, local_pi_id)
    _validate_project_dates(conn, existing["id"], start_year, start_month, end_year, end_month)
    conn.execute(
        """UPDATE projects
           SET name = ?, start_year = ?, start_month = ?,
               end_year = ?, end_month = ?, local_pi_id = ?,
               personnel_budget = ?, admin_group_id = ?
           WHERE project_code = ?""",
        (name, start_year, start_month, end_year, end_month,
         local_pi_id, personnel_budget, admin_group_id, project_code),
    )
    conn.commit()
    conn.close()


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
        WHERE g.is_internal = 1
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
        JOIN groups g ON g.id = emp.group_id
        JOIN projects p ON p.id = al.project_id
        WHERE g.is_internal = 1
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


def get_nonproject_by_person(db_path: str | Path) -> dict:
    """Return Non-Project effort by person and month.

    Returns a dict with:
        months:  list of month label strings in chronological order
        rows:    list of {name, group, <month>: np_pct, ...} dicts,
                 sorted by name, with a trailing Total row showing the
                 combined Non-Project percentage across all people
    """
    conn = get_connection(db_path)
    create_schema(conn)
    months = _discover_months(conn)

    person_rows = conn.execute("""
        SELECT
            emp.name AS employee_name,
            g.name AS group_name,
            e.year,
            e.month,
            SUM(CASE WHEN p.project_code = 'Non-Project' THEN e.percentage ELSE 0 END) AS np_effort,
            SUM(e.percentage) AS total_effort
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN groups g ON g.id = emp.group_id
        JOIN projects p ON p.id = al.project_id
        WHERE g.is_internal = 1
        GROUP BY emp.id, e.year, e.month
        ORDER BY emp.name, e.year, e.month
    """).fetchall()

    total_rows = conn.execute("""
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
        WHERE g.is_internal = 1
        GROUP BY e.year, e.month
        ORDER BY e.year, e.month
    """).fetchall()
    conn.close()

    month_labels = [_month_label(y, m) for y, m in months]

    person_data: dict[str, dict[str, float]] = {}
    person_group: dict[str, str] = {}
    for r in person_rows:
        name = r["employee_name"]
        label = _month_label(r["year"], r["month"])
        total = r["total_effort"]
        pct = (r["np_effort"] / total * 100.0) if total else 0.0
        person_data.setdefault(name, {})[label] = pct
        person_group[name] = r["group_name"]

    result_rows: list[dict] = []
    for name in sorted(person_data.keys()):
        row: dict = {"name": name, "group": person_group[name]}
        for label in month_labels:
            row[label] = round(person_data[name].get(label, 0.0), 1)
        result_rows.append(row)

    total_row: dict = {"name": "Total", "group": ""}
    for r in total_rows:
        label = _month_label(r["year"], r["month"])
        t = r["total_effort"]
        total_row[label] = round((r["np_effort"] / t * 100.0) if t else 0.0, 1)
    for label in month_labels:
        total_row.setdefault(label, 0.0)
    result_rows.append(total_row)

    return {"months": month_labels, "rows": result_rows}


def get_project_details(db_path: str | Path, project_code: str) -> dict:
    """Return total FTE and per-person effort for a project by month.

    Returns a dict with:
        months:       list of month label strings in chronological order
        fte_rows:     [{label, <month>: fte_value, ...}, ...]
        people:       list of {name, group, <month>: effort_pct, ...} sorted by name
        project_info: dict with name, start, end, local_pi_name, personnel_budget,
                      admin_group_name (all optional fields are None when not set)
    """
    conn = get_connection(db_path)
    create_schema(conn)
    months = _discover_months(conn)

    proj_row = conn.execute("""
        SELECT p.name, p.start_year, p.start_month, p.end_year, p.end_month,
               e.name AS local_pi_name, p.personnel_budget,
               g.name AS admin_group_name
        FROM projects p
        LEFT JOIN employees e ON e.id = p.local_pi_id
        LEFT JOIN groups g ON g.id = p.admin_group_id
        WHERE p.project_code = ?
    """, (project_code,)).fetchone()

    def _period(year, month):
        if year is not None and month is not None:
            return _month_label(year, month)
        return None

    project_info = None
    if proj_row is not None:
        project_info = {
            "name": proj_row["name"],
            "start": _period(proj_row["start_year"], proj_row["start_month"]),
            "end": _period(proj_row["end_year"], proj_row["end_month"]),
            "local_pi_name": proj_row["local_pi_name"],
            "personnel_budget": proj_row["personnel_budget"],
            "admin_group_name": proj_row["admin_group_name"],
        }

    internal_fte_rows = conn.execute("""
        SELECT e.year, e.month, SUM(e.percentage) / 100.0 AS total_fte
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN groups g ON g.id = emp.group_id
        JOIN projects p ON p.id = al.project_id
        WHERE p.project_code = ? AND g.is_internal = 1
        GROUP BY e.year, e.month
        ORDER BY e.year, e.month
    """, (project_code,)).fetchall()

    external_fte_rows = conn.execute("""
        SELECT e.year, e.month, SUM(e.percentage) / 100.0 AS total_fte
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN groups g ON g.id = emp.group_id
        JOIN projects p ON p.id = al.project_id
        WHERE p.project_code = ? AND g.is_internal = 0
        GROUP BY e.year, e.month
        ORDER BY e.year, e.month
    """, (project_code,)).fetchall()

    person_rows = conn.execute("""
        SELECT
            emp.name AS employee_name,
            g.name AS group_name,
            e.year,
            e.month,
            SUM(e.percentage) AS total_pct
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN groups g ON g.id = emp.group_id
        JOIN projects p ON p.id = al.project_id
        WHERE p.project_code = ?
        GROUP BY emp.id, e.year, e.month
        ORDER BY emp.name, e.year, e.month
    """, (project_code,)).fetchall()
    conn.close()

    month_labels = [_month_label(y, m) for y, m in months]

    internal_fte_data: dict[str, float] = {}
    for r in internal_fte_rows:
        internal_fte_data[_month_label(r["year"], r["month"])] = round(r["total_fte"], 2)

    external_fte_data: dict[str, float] = {}
    for r in external_fte_rows:
        external_fte_data[_month_label(r["year"], r["month"])] = round(r["total_fte"], 2)

    internal_row: dict = {"label": "Internal FTE"}
    for label in month_labels:
        internal_row[label] = internal_fte_data.get(label, 0.0)

    fte_result: list[dict] = [internal_row]
    if external_fte_data:
        external_row: dict = {"label": "External FTE"}
        for label in month_labels:
            external_row[label] = external_fte_data.get(label, 0.0)
        fte_result.append(external_row)

    person_data: dict[str, dict[str, float]] = {}
    person_group: dict[str, str] = {}
    for r in person_rows:
        name = r["employee_name"]
        label = _month_label(r["year"], r["month"])
        person_data.setdefault(name, {})[label] = r["total_pct"]
        person_group[name] = r["group_name"]

    people_result: list[dict] = []
    for name in sorted(person_data.keys()):
        row: dict = {"name": name, "group": person_group[name]}
        for label in month_labels:
            row[label] = round(person_data[name].get(label, 0.0), 1)
        people_result.append(row)

    return {
        "months": month_labels,
        "fte_rows": fte_result,
        "people": people_result,
        "project_info": project_info,
    }


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
