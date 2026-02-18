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

    _clear_data(conn)

    # Pre-populate the Non-Project sentinel so every allocation line can
    # reference a project row regardless of whether a project ID was supplied.
    conn.execute(
        "INSERT INTO projects (project_code, name) VALUES (?, NULL)",
        (NON_PROJECT_CODE,),
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

            # Resolve project
            project_code = row["Project Id"].strip()
            if not project_code or "N/A" in project_code:
                project_code = NON_PROJECT_CODE

            raw_project_name = row["Project Name"].strip()
            project_name = NON_PROJECT_CODE if not raw_project_name or "N/A" in raw_project_name else raw_project_name
            existing = conn.execute(
                "SELECT id, name FROM projects WHERE project_code = ?",
                (project_code,),
            ).fetchone()

            if existing:
                project_id = existing["id"]
                # Fill in name if we now have one and didn't before
                if project_name and not existing["name"]:
                    conn.execute(
                        "UPDATE projects SET name = ? WHERE id = ?",
                        (project_name, project_id),
                    )
            else:
                cur = conn.execute(
                    "INSERT INTO projects (project_code, name) VALUES (?, ?)",
                    (project_code, project_name),
                )
                project_id = cur.lastrowid

            # Insert allocation line
            cur = conn.execute(
                """
                INSERT INTO allocation_lines
                    (employee_id, project_id, fund_code, source, account,
                     cost_code_1, cost_code_2, cost_code_3, program_code)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    current_employee_id,
                    project_id,
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

    dated_employees = conn.execute("""
        SELECT id, name, start_year, start_month, end_year, end_month
        FROM employees
        WHERE start_year IS NOT NULL OR end_year IS NOT NULL
    """).fetchall()

    for emp in dated_employees:
        sy, sm = emp["start_year"], emp["start_month"]
        ey, em = emp["end_year"], emp["end_month"]
        effort_months = conn.execute("""
            SELECT DISTINCT e.year, e.month
            FROM efforts e
            JOIN allocation_lines al ON al.id = e.allocation_line_id
            WHERE al.employee_id = ?
            ORDER BY e.year, e.month
        """, (emp["id"],)).fetchall()

        def _fmt(year, month):
            return f"{_month_abbr[month]} {year}"

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

    conn.execute(
        "INSERT INTO audit_log (timestamp, action, details) VALUES (?, ?, ?)",
        (datetime.now(timezone.utc).isoformat(), "load",
         json.dumps({"tsv_path": str(tsv_path)})),
    )
    conn.commit()
    conn.close()


def load_tsv_as_branch(tsv_path: str | Path, main_db_path: str | Path,
                       branch_name: str | None = None) -> Path:
    """Load a TSV into a branch database for later merging.

    If the main DB does not exist (bootstrap), loads directly into it and
    returns the main DB path.  Otherwise, creates a branch and loads into that.

    Args:
        tsv_path: Path to the input TSV file.
        main_db_path: Path to the main SQLite database.
        branch_name: Name for the branch. Defaults to ``load_YYYYMMDD``.

    Returns:
        The path to the database that was loaded into (main or branch).
    """
    from sej.branch import create_branch

    main_db_path = Path(main_db_path)

    if not main_db_path.exists():
        # Bootstrap: load directly into main
        load_tsv(tsv_path, main_db_path)
        return main_db_path

    # Subsequent load: create branch, wipe it, load fresh
    if branch_name is None:
        branch_name = f"load_{datetime.now().strftime('%Y%m%d')}"
    branch_path = create_branch(main_db_path, branch_name)
    load_tsv(tsv_path, branch_path)
    return branch_path


def main():
    """CLI entry point: ``sej-load TSV_PATH [DB_PATH]``."""
    import sys

    if len(sys.argv) < 2 or len(sys.argv) > 3:
        sys.exit("Usage: sej-load TSV_PATH [DB_PATH]")
    tsv_path = Path(sys.argv[1])
    db_path = Path(sys.argv[2]) if len(sys.argv) == 3 else Path("data/sej.db")
    result = load_tsv_as_branch(tsv_path, db_path)
    if result == db_path:
        print(f"Loaded {tsv_path} → {db_path}")
    else:
        print(f"Loaded {tsv_path} → branch at {result}")
        print(f"Run 'sej-branch merge' to apply changes to main.")
