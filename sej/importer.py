"""Import effort allocation data from a TSV file into the SEJ database.

For the first load (no existing main DB), data is loaded directly.
For subsequent loads, data is loaded into a branch that can be merged.
"""

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from sej.db import create_schema, get_connection


# Column names in the TSV that hold monthly effort values
_MONTH_COLUMNS = [
    "July 2025", "August 2025", "September 2025", "October 2025",
    "November 2025", "December 2025", "January 2026", "February 2026",
    "March 2026", "April 2026", "May 2026", "June 2026",
]

NON_PROJECT_CODE = "Non-Project"


def _parse_month_column(col: str) -> tuple[int, int]:
    """Return (year, month) from a column header like 'July 2025'."""
    dt = datetime.strptime(col, "%B %Y")
    return dt.year, dt.month


def _parse_percentage(value: str) -> float | None:
    """Return a float from '82.00%', or None if the cell is blank."""
    value = value.strip()
    if not value:
        return None
    return float(value.rstrip("%"))


def _clear_data(conn) -> None:
    """Delete all rows from all data tables in dependency order."""
    conn.execute("DELETE FROM efforts")
    conn.execute("DELETE FROM allocation_lines")
    conn.execute("DELETE FROM employees")
    conn.execute("DELETE FROM budget_lines")
    conn.execute("DELETE FROM projects")
    conn.execute("DELETE FROM groups")


def _get_or_insert(conn, table: str, lookup_col: str, value: str) -> int:
    """Return the id for a row, inserting it if it does not exist."""
    row = conn.execute(
        f"SELECT id FROM {table} WHERE {lookup_col} = ?", (value,)
    ).fetchone()
    if row:
        return row["id"]
    cur = conn.execute(
        f"INSERT INTO {table} ({lookup_col}) VALUES (?)", (value,)
    )
    return cur.lastrowid


def load_tsv(tsv_path: str | Path, db_path: str | Path) -> None:
    """Wipe the database and reload it from the given TSV file.

    Args:
        tsv_path: Path to the input TSV file.
        db_path:  Path to the SQLite database file (created if absent).
    """
    tsv_path = Path(tsv_path)
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path)
    create_schema(conn)

    # Save employee date ranges before clearing so they can be restored after reload
    saved_dates = {
        r["name"]: (r["start_year"], r["start_month"], r["end_year"], r["end_month"])
        for r in conn.execute(
            "SELECT name, start_year, start_month, end_year, end_month FROM employees"
        ).fetchall()
    }

    # Save budget line display names before clearing so they can be restored after reload
    saved_display_names = {
        r["budget_line_code"]: r["display_name"]
        for r in conn.execute(
            "SELECT budget_line_code, display_name FROM budget_lines WHERE display_name IS NOT NULL"
        ).fetchall()
    }

    _clear_data(conn)

    # Pre-populate the Non-Project sentinel as both a project and a budget line
    cur = conn.execute(
        "INSERT INTO projects (name, is_nonproject) VALUES (?, 1)",
        (NON_PROJECT_CODE,),
    )
    np_project_id = cur.lastrowid
    np_display_name = saved_display_names.get(NON_PROJECT_CODE, NON_PROJECT_CODE)
    conn.execute(
        "INSERT INTO budget_lines (project_id, budget_line_code, display_name) VALUES (?, ?, ?)",
        (np_project_id, NON_PROJECT_CODE, np_display_name),
    )

    with open(tsv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")

        # Discover which month columns are actually present in this file
        month_cols = [c for c in _MONTH_COLUMNS if c in reader.fieldnames]

        current_employee_id = None

        for row in reader:
            # Fill forward: employee name only appears on the first row of a block
            raw_name = row["EMPLOYEE"].strip()
            if raw_name:
                group_name = row["Group"].strip()
                group_id = _get_or_insert(conn, "groups", "name", group_name)

                cur = conn.execute(
                    "INSERT INTO employees (name, group_id) VALUES (?, ?)",
                    (raw_name, group_id),
                )
                current_employee_id = cur.lastrowid

                # Restore date range if this employee had one before the reload
                if raw_name in saved_dates:
                    sy, sm, ey, em = saved_dates[raw_name]
                    if any(v is not None for v in (sy, sm, ey, em)):
                        conn.execute(
                            "UPDATE employees SET start_year=?, start_month=?,"
                            " end_year=?, end_month=? WHERE id=?",
                            (sy, sm, ey, em, current_employee_id),
                        )

            if current_employee_id is None:
                raise ValueError(
                    f"Data row has no employee context: {dict(row)}"
                )

            # Resolve budget line (TSV columns are still "Project Id" / "Project Name")
            budget_line_code = row["Project Id"].strip()
            is_imputed = False
            if not budget_line_code or "N/A" in budget_line_code:
                # Construct imputed code from accounting fields
                fields = [
                    row["Fund Code"].strip(),
                    row["Source"].strip(),
                    row["Account"].strip(),
                    row["Cost Code 1"].strip(),
                    row["Cost Code 2"].strip(),
                    row["Cost Code 3"].strip(),
                    row["Program Code"].strip(),
                ]
                # If all accounting fields are empty, fall back to the Non-Project sentinel
                if all(not f for f in fields):
                    budget_line_code = NON_PROJECT_CODE
                else:
                    budget_line_code = "I:" + ":".join(fields)
                    is_imputed = True

            raw_bl_name = row["Project Name"].strip()
            if is_imputed:
                bl_name = budget_line_code
            else:
                bl_name = raw_bl_name if raw_bl_name and "N/A" not in raw_bl_name else None

            existing = conn.execute(
                "SELECT id, name FROM budget_lines WHERE budget_line_code = ?",
                (budget_line_code,),
            ).fetchone()

            if existing:
                budget_line_id = existing["id"]
                # Always refresh the finance name from the TSV
                if bl_name and not is_imputed:
                    conn.execute(
                        "UPDATE budget_lines SET name = ? WHERE id = ?",
                        (bl_name, budget_line_id),
                    )
            else:
                if is_imputed:
                    # Imputed budget lines always belong to the Non-Project project
                    project_id = np_project_id
                else:
                    # Reuse an existing project with the same name, or create a new one
                    proj_name = bl_name or budget_line_code
                    existing_proj = conn.execute(
                        "SELECT id FROM projects WHERE name = ? AND is_nonproject = 0",
                        (proj_name,),
                    ).fetchone()
                    if existing_proj:
                        project_id = existing_proj["id"]
                    else:
                        cur = conn.execute(
                            "INSERT INTO projects (name) VALUES (?)",
                            (proj_name,),
                        )
                        project_id = cur.lastrowid
                # Use saved display_name if available, otherwise initialize from finance name
                saved_dn = saved_display_names.get(budget_line_code, bl_name or budget_line_code)
                cur = conn.execute(
                    "INSERT INTO budget_lines (project_id, budget_line_code, name, display_name)"
                    " VALUES (?, ?, ?, ?)",
                    (project_id, budget_line_code, bl_name, saved_dn),
                )
                budget_line_id = cur.lastrowid

            # Insert allocation line
            cur = conn.execute(
                """
                INSERT INTO allocation_lines
                    (employee_id, budget_line_id, fund_code, source, account,
                     cost_code_1, cost_code_2, cost_code_3, program_code)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    current_employee_id,
                    budget_line_id,
                    row["Fund Code"].strip() or None,
                    row["Source"].strip() or None,
                    row["Account"].strip() or None,
                    row["Cost Code 1"].strip() or None,
                    row["Cost Code 2"].strip() or None,
                    row["Cost Code 3"].strip() or None,
                    row["Program Code"].strip() or None,
                ),
            )
            line_id = cur.lastrowid

            # Insert effort rows for each month that has a value
            for col in month_cols:
                pct = _parse_percentage(row[col])
                if not pct:
                    continue
                year, month = _parse_month_column(col)
                conn.execute(
                    "INSERT INTO efforts (allocation_line_id, year, month, percentage) VALUES (?, ?, ?, ?)",
                    (line_id, year, month, pct),
                )

    # Validate that restored employee date ranges don't conflict with loaded effort
    _month_abbr = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def _fmt(year, month):
        return f"{_month_abbr[month]} {year}"

    dated_employees = conn.execute("""
        SELECT id, name, start_year, start_month, end_year, end_month
        FROM employees
        WHERE start_year IS NOT NULL OR end_year IS NOT NULL
    """).fetchall()

    # Batch-fetch effort months for all dated employees to avoid N+1 queries
    effort_months_by_emp = {}
    if dated_employees:
        emp_ids = [emp["id"] for emp in dated_employees]
        placeholders = ", ".join("?" for _ in emp_ids)
        effort_rows = conn.execute(f"""
            SELECT DISTINCT al.employee_id, e.year, e.month
            FROM efforts e
            JOIN allocation_lines al ON al.id = e.allocation_line_id
            WHERE al.employee_id IN ({placeholders})
            ORDER BY al.employee_id, e.year, e.month
        """, emp_ids).fetchall()
        for row in effort_rows:
            effort_months_by_emp.setdefault(row["employee_id"], []).append(row)

    try:
        for emp in dated_employees:
            sy, sm = emp["start_year"], emp["start_month"]
            ey, em = emp["end_year"], emp["end_month"]
            effort_months = effort_months_by_emp.get(emp["id"], [])

            if sy is not None and sm is not None:
                start_ym = sy * 12 + sm
                early = [_fmt(r["year"], r["month"]) for r in effort_months
                         if r["year"] * 12 + r["month"] < start_ym]
                if early:
                    raise ValueError(
                        f"Effort exists before employee start ({_fmt(sy, sm)}): "
                        + ", ".join(early)
                    )

            if ey is not None and em is not None:
                end_ym = ey * 12 + em
                late = [_fmt(r["year"], r["month"]) for r in effort_months
                        if r["year"] * 12 + r["month"] > end_ym]
                if late:
                    raise ValueError(
                        f"Effort exists after employee end ({_fmt(ey, em)}): "
                        + ", ".join(late)
                    )
    except ValueError:
        conn.rollback()
        conn.close()
        raise

    conn.execute(
        "INSERT INTO audit_log (timestamp, action, details) VALUES (?, ?, ?)",
        (datetime.now(timezone.utc).isoformat(), "load",
         json.dumps({"tsv_path": str(tsv_path)})),
    )
    conn.commit()
    conn.close()


def load_tsv_as_branch(tsv_path: str | Path, main_db_path: str | Path,
                       branch_name: str | None = None) -> Path:
    """Load a TSV into the database, optionally within a change_set.

    If the main DB does not exist (bootstrap), loads directly into it.
    Otherwise, creates a change_set, loads data, and leaves the change_set
    open for review/merge.

    Args:
        tsv_path: Path to the input TSV file.
        main_db_path: Path to the main SQLite database.
        branch_name: Ignored (kept for CLI compatibility).

    Returns:
        The path to the database that was loaded into (always main_db_path).
    """
    from sej.changelog import create_change_set

    main_db_path = Path(main_db_path)

    if not main_db_path.exists():
        # Bootstrap: load directly into main
        load_tsv(tsv_path, main_db_path)
        return main_db_path

    # Subsequent load: create a change_set, load data, leave open for review
    create_change_set(main_db_path)
    load_tsv(tsv_path, main_db_path)
    return main_db_path


def augment_sample_data(db_path: str | Path) -> str | None:
    """Enrich one project with full metadata and split its budget line into Y1/Y2.

    Picks the non-sentinel project with the most distinct employees, sets
    project dates/PI/admin group, splits the budget line into two year-halves,
    and reassigns effort rows to the correct half.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        The name of the augmented project, or None if no candidate was found.
    """
    conn = get_connection(db_path)

    # 1. Pick the non-sentinel project with the most distinct employees
    candidate = conn.execute("""
        SELECT bl.project_id, p.name, COUNT(DISTINCT al.employee_id) AS emp_count
        FROM budget_lines bl
        JOIN allocation_lines al ON al.budget_line_id = bl.id
        JOIN projects p ON p.id = bl.project_id
        WHERE p.is_nonproject = 0
        GROUP BY bl.project_id
        ORDER BY emp_count DESC
        LIMIT 1
    """).fetchone()
    if candidate is None:
        conn.close()
        return None
    project_id = candidate["project_id"]
    project_name = candidate["name"]

    # Pick an employee on this project to be PI
    pi_row = conn.execute("""
        SELECT al.employee_id, e.group_id
        FROM allocation_lines al
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        JOIN employees e ON e.id = al.employee_id
        WHERE bl.project_id = ?
        LIMIT 1
    """, (project_id,)).fetchone()
    pi_id = pi_row["employee_id"]
    admin_group_id = pi_row["group_id"]

    # 2. Set project metadata
    conn.execute("""
        UPDATE projects
        SET start_year = 2025, start_month = 7,
            end_year = 2026, end_month = 6,
            local_pi_id = ?, admin_group_id = ?
        WHERE id = ?
    """, (pi_id, admin_group_id, project_id))

    # 3. Split the budget line into Y1 and Y2
    # Get the first budget line for this project
    bl = conn.execute("""
        SELECT id, budget_line_code, name, display_name
        FROM budget_lines WHERE project_id = ?
        LIMIT 1
    """, (project_id,)).fetchone()
    y1_bl_id = bl["id"]
    orig_code = bl["budget_line_code"]
    orig_display = bl["display_name"] or bl["name"] or orig_code

    # Update existing budget line to be Y1
    conn.execute("""
        UPDATE budget_lines
        SET start_year = 2025, start_month = 7,
            end_year = 2026, end_month = 2,
            display_name = ?
        WHERE id = ?
    """, (f"{orig_display} — Y1", y1_bl_id))

    # Create Y2 budget line
    y2_code = f"{orig_code}-Y2"
    cur = conn.execute("""
        INSERT INTO budget_lines
            (project_id, budget_line_code, name, display_name,
             start_year, start_month, end_year, end_month)
        VALUES (?, ?, ?, ?, 2026, 3, 2026, 6)
    """, (project_id, y2_code, bl["name"], f"{orig_display} — Y2"))
    y2_bl_id = cur.lastrowid

    # 4. Reassign effort to the correct budget line
    # For each allocation line on Y1, create a duplicate pointing to Y2
    # and move March 2026+ efforts to the new line
    alloc_lines = conn.execute("""
        SELECT id, employee_id, fund_code, source, account,
               cost_code_1, cost_code_2, cost_code_3, program_code
        FROM allocation_lines WHERE budget_line_id = ?
    """, (y1_bl_id,)).fetchall()

    for al in alloc_lines:
        # Create duplicate allocation line for Y2
        cur = conn.execute("""
            INSERT INTO allocation_lines
                (employee_id, budget_line_id, fund_code, source, account,
                 cost_code_1, cost_code_2, cost_code_3, program_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (al["employee_id"], y2_bl_id, al["fund_code"], al["source"],
              al["account"], al["cost_code_1"], al["cost_code_2"],
              al["cost_code_3"], al["program_code"]))
        y2_al_id = cur.lastrowid

        # Move efforts for months March 2026+ from old allocation line to new one
        conn.execute("""
            UPDATE efforts
            SET allocation_line_id = ?
            WHERE allocation_line_id = ?
              AND (year > 2026 OR (year = 2026 AND month >= 3))
        """, (y2_al_id, al["id"]))

    # 5. Set personnel budgets based on effort data
    for bl_id, label in [(y1_bl_id, "Y1"), (y2_bl_id, "Y2")]:
        total_cost = conn.execute("""
            SELECT COALESCE(SUM(e.percentage / 100.0 * emp.salary / 12.0), 0) AS cost
            FROM efforts e
            JOIN allocation_lines al ON al.id = e.allocation_line_id
            JOIN employees emp ON emp.id = al.employee_id
            WHERE al.budget_line_id = ?
        """, (bl_id,)).fetchone()["cost"]
        # Add ~15% headroom so spending chart looks realistic
        budget = round(total_cost * 1.15, 2)
        conn.execute(
            "UPDATE budget_lines SET personnel_budget = ? WHERE id = ?",
            (budget, bl_id),
        )

    conn.commit()
    conn.close()
    return project_name


def main():
    """CLI entry point: ``sej-load TSV_PATH [DB_PATH] [--augment]``."""
    import sys

    args = [a for a in sys.argv[1:] if a != "--augment"]
    do_augment = "--augment" in sys.argv

    if len(args) < 1 or len(args) > 2:
        sys.exit("Usage: sej-load TSV_PATH [DB_PATH] [--augment]")
    tsv_path = Path(args[0])
    db_path = Path(args[1]) if len(args) == 2 else Path("data/sej.db")
    result = load_tsv_as_branch(tsv_path, db_path)
    if result == db_path:
        print(f"Loaded {tsv_path} → {db_path}")
    else:
        print(f"Loaded {tsv_path} → branch at {result}")
        print(f"Run 'sej-branch merge' to apply changes to main.")

    if do_augment:
        project_name = augment_sample_data(db_path)
        if project_name:
            print(f"Augmented project '{project_name}' with metadata + Y1/Y2 budget line split.")
        else:
            print("No candidate project found for augmentation.")
