import sqlite3
from pathlib import Path


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Open a connection to the SQLite database, enabling foreign keys."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


def _migrate_to_budget_lines(conn: sqlite3.Connection) -> None:
    """Migrate old projects-based schema to new projects + budget_lines schema.

    Old schema: projects table has project_code, name, dates, budget, pi, admin_group
    New schema: projects = aggregation layer; budget_lines = granular financial units
    """
    conn.executescript("""
        PRAGMA foreign_keys = OFF;

        ALTER TABLE projects RENAME TO old_projects;
        ALTER TABLE allocation_lines RENAME TO old_allocation_lines;

        CREATE TABLE projects (
            id              INTEGER PRIMARY KEY,
            name            TEXT    NOT NULL,
            local_pi_id     INTEGER REFERENCES employees(id),
            admin_group_id  INTEGER REFERENCES groups(id),
            is_nonproject   INTEGER NOT NULL DEFAULT 0
        );

        INSERT INTO projects (id, name, local_pi_id, admin_group_id, is_nonproject)
        SELECT id,
               COALESCE(name, project_code),
               local_pi_id,
               admin_group_id,
               CASE WHEN project_code = 'Non-Project' THEN 1 ELSE 0 END
        FROM old_projects;

        CREATE TABLE budget_lines (
            id               INTEGER PRIMARY KEY,
            project_id       INTEGER NOT NULL REFERENCES projects(id),
            budget_line_code TEXT    UNIQUE NOT NULL,
            name             TEXT,
            start_year       INTEGER,
            start_month      INTEGER CHECK (start_month BETWEEN 1 AND 12),
            end_year         INTEGER,
            end_month        INTEGER CHECK (end_month BETWEEN 1 AND 12),
            personnel_budget REAL
        );

        INSERT INTO budget_lines (id, project_id, budget_line_code, name,
                                  start_year, start_month, end_year, end_month,
                                  personnel_budget)
        SELECT id, id, project_code, name,
               start_year, start_month, end_year, end_month,
               personnel_budget
        FROM old_projects;

        CREATE TABLE allocation_lines (
            id             INTEGER PRIMARY KEY,
            employee_id    INTEGER NOT NULL REFERENCES employees(id),
            budget_line_id INTEGER NOT NULL REFERENCES budget_lines(id),
            fund_code      TEXT,
            source         TEXT,
            account        TEXT,
            cost_code_1    TEXT,
            cost_code_2    TEXT,
            cost_code_3    TEXT,
            program_code   TEXT
        );

        INSERT INTO allocation_lines (id, employee_id, budget_line_id,
                                      fund_code, source, account,
                                      cost_code_1, cost_code_2, cost_code_3,
                                      program_code)
        SELECT id, employee_id, project_id,
               fund_code, source, account,
               cost_code_1, cost_code_2, cost_code_3,
               program_code
        FROM old_allocation_lines;

        DROP TABLE old_allocation_lines;
        DROP TABLE old_projects;

        PRAGMA foreign_keys = ON;
    """)
    conn.commit()


def create_schema(conn: sqlite3.Connection) -> None:
    """Create all tables. Safe to call on an existing database (no-op if tables exist)."""
    # Detect old schema: projects table exists with project_code column, no budget_lines
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}

    if "projects" in tables and "budget_lines" not in tables:
        # Check that it's actually the old schema (has project_code column)
        old_cols = {r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()}
        if "project_code" in old_cols:
            _migrate_to_budget_lines(conn)

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS groups (
            id          INTEGER PRIMARY KEY,
            name        TEXT    UNIQUE NOT NULL,
            is_internal INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS employees (
            id          INTEGER PRIMARY KEY,
            name        TEXT    UNIQUE NOT NULL,
            group_id    INTEGER NOT NULL REFERENCES groups(id),
            salary      REAL    NOT NULL DEFAULT 120000,
            start_year  INTEGER,
            start_month INTEGER CHECK (start_month BETWEEN 1 AND 12),
            end_year    INTEGER,
            end_month   INTEGER CHECK (end_month BETWEEN 1 AND 12)
        );

        CREATE TABLE IF NOT EXISTS projects (
            id              INTEGER PRIMARY KEY,
            name            TEXT    NOT NULL,
            local_pi_id     INTEGER REFERENCES employees(id),
            admin_group_id  INTEGER REFERENCES groups(id),
            is_nonproject   INTEGER NOT NULL DEFAULT 0,
            start_year      INTEGER,
            start_month     INTEGER CHECK (start_month BETWEEN 1 AND 12),
            end_year        INTEGER,
            end_month       INTEGER CHECK (end_month BETWEEN 1 AND 12)
        );

        CREATE TABLE IF NOT EXISTS budget_lines (
            id               INTEGER PRIMARY KEY,
            project_id       INTEGER NOT NULL REFERENCES projects(id),
            budget_line_code TEXT    UNIQUE NOT NULL,
            name             TEXT,
            display_name     TEXT,
            start_year       INTEGER,
            start_month      INTEGER CHECK (start_month BETWEEN 1 AND 12),
            end_year         INTEGER,
            end_month        INTEGER CHECK (end_month BETWEEN 1 AND 12),
            personnel_budget REAL
        );

        CREATE TABLE IF NOT EXISTS allocation_lines (
            id             INTEGER PRIMARY KEY,
            employee_id    INTEGER NOT NULL REFERENCES employees(id),
            budget_line_id INTEGER NOT NULL REFERENCES budget_lines(id),
            fund_code      TEXT,
            source         TEXT,
            account        TEXT,
            cost_code_1    TEXT,
            cost_code_2    TEXT,
            cost_code_3    TEXT,
            program_code   TEXT
        );

        CREATE TABLE IF NOT EXISTS efforts (
            id                 INTEGER PRIMARY KEY,
            allocation_line_id INTEGER NOT NULL REFERENCES allocation_lines(id),
            year               INTEGER NOT NULL,
            month              INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
            percentage         REAL    NOT NULL,
            UNIQUE (allocation_line_id, year, month)
        );

        CREATE TABLE IF NOT EXISTS _meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS audit_log (
            id        INTEGER PRIMARY KEY,
            timestamp TEXT    NOT NULL,
            action    TEXT    NOT NULL,
            details   TEXT
        );

        CREATE TABLE IF NOT EXISTS change_sets (
            id          INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'open',
            created_at  TEXT NOT NULL,
            closed_at   TEXT
        );

        CREATE TABLE IF NOT EXISTS change_log (
            id             INTEGER PRIMARY KEY,
            change_set_id  INTEGER NOT NULL REFERENCES change_sets(id),
            seq            INTEGER NOT NULL,
            table_name     TEXT NOT NULL,
            operation      TEXT NOT NULL,
            row_id         INTEGER NOT NULL,
            old_values     TEXT,
            new_values     TEXT
        );
    """)

    # Migration: add is_internal to groups if it doesn't exist (for older DBs)
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(groups)").fetchall()}
    if "is_internal" not in existing_cols:
        conn.execute("ALTER TABLE groups ADD COLUMN is_internal INTEGER NOT NULL DEFAULT 1")

    # Migration: add salary to employees if it doesn't exist (for older DBs)
    employee_cols = {r[1] for r in conn.execute("PRAGMA table_info(employees)").fetchall()}
    if "salary" not in employee_cols:
        conn.execute("ALTER TABLE employees ADD COLUMN salary REAL NOT NULL DEFAULT 120000")

    # Migration: add start/end date columns to employees if they don't exist (for older DBs)
    for col, definition in [
        ("start_year",  "INTEGER"),
        ("start_month", "INTEGER CHECK (start_month BETWEEN 1 AND 12)"),
        ("end_year",    "INTEGER"),
        ("end_month",   "INTEGER CHECK (end_month BETWEEN 1 AND 12)"),
    ]:
        if col not in employee_cols:
            conn.execute(f"ALTER TABLE employees ADD COLUMN {col} {definition}")

    # Migration: add start/end date columns to projects if they don't exist (for older DBs)
    project_cols = {r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()}
    for col, definition in [
        ("start_year",  "INTEGER"),
        ("start_month", "INTEGER CHECK (start_month BETWEEN 1 AND 12)"),
        ("end_year",    "INTEGER"),
        ("end_month",   "INTEGER CHECK (end_month BETWEEN 1 AND 12)"),
    ]:
        if col not in project_cols:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col} {definition}")

    # Migration: add display_name to budget_lines if it doesn't exist (for older DBs)
    bl_cols = {r[1] for r in conn.execute("PRAGMA table_info(budget_lines)").fetchall()}
    if "display_name" not in bl_cols:
        conn.execute("ALTER TABLE budget_lines ADD COLUMN display_name TEXT")
    # Back-fill any rows that still have NULL display_name (upgrade or first migration)
    conn.execute(
        "UPDATE budget_lines SET display_name = COALESCE(name, budget_line_code)"
        " WHERE display_name IS NULL"
    )

    conn.commit()
