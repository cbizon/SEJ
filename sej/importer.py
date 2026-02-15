"""Import effort allocation data from a TSV file into the SEJ database.

Running the import wipes all existing data and reloads from the given file.
"""

import csv
from datetime import datetime
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
    conn = get_connection(db_path)
    create_schema(conn)
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

            if current_employee_id is None:
                raise ValueError(
                    f"Data row has no employee context: {dict(row)}"
                )

            # Resolve project
            project_code = row["Project Id"].strip()
            if not project_code or "N/A" in project_code:
                project_code = NON_PROJECT_CODE

            project_name = row["Project Name"].strip() or None
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
                if pct is None:
                    continue
                year, month = _parse_month_column(col)
                conn.execute(
                    "INSERT INTO efforts (allocation_line_id, year, month, percentage) VALUES (?, ?, ?, ?)",
                    (line_id, year, month, pct),
                )

    conn.commit()
    conn.close()
