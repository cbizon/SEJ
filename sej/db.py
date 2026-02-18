import sqlite3
from pathlib import Path


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Open a connection to the SQLite database, enabling foreign keys."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    """Create all tables. Safe to call on an existing database (no-op if tables exist)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS groups (
            id          INTEGER PRIMARY KEY,
            name        TEXT    UNIQUE NOT NULL,
            is_internal INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS employees (
            id       INTEGER PRIMARY KEY,
            name     TEXT    UNIQUE NOT NULL,
            group_id INTEGER NOT NULL REFERENCES groups(id)
        );

        CREATE TABLE IF NOT EXISTS projects (
            id               INTEGER PRIMARY KEY,
            project_code     TEXT    UNIQUE NOT NULL,
            name             TEXT,
            start_year       INTEGER,
            start_month      INTEGER CHECK (start_month BETWEEN 1 AND 12),
            end_year         INTEGER,
            end_month        INTEGER CHECK (end_month BETWEEN 1 AND 12),
            local_pi_id      INTEGER REFERENCES employees(id),
            personnel_budget REAL,
            admin_group_id   INTEGER REFERENCES groups(id)
        );

        CREATE TABLE IF NOT EXISTS allocation_lines (
            id           INTEGER PRIMARY KEY,
            employee_id  INTEGER NOT NULL REFERENCES employees(id),
            project_id   INTEGER NOT NULL REFERENCES projects(id),
            fund_code    TEXT,
            source       TEXT,
            account      TEXT,
            cost_code_1  TEXT,
            cost_code_2  TEXT,
            cost_code_3  TEXT,
            program_code TEXT
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
    """)

    # Migration: add is_internal to groups if it doesn't exist (for older DBs)
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(groups)").fetchall()}
    if "is_internal" not in existing_cols:
        conn.execute("ALTER TABLE groups ADD COLUMN is_internal INTEGER NOT NULL DEFAULT 1")

    # Migration: add new project detail columns if they don't exist (for older DBs)
    project_cols = {r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()}
    # Rename pi_id -> local_pi_id if the old column name exists
    if "pi_id" in project_cols and "local_pi_id" not in project_cols:
        conn.execute("ALTER TABLE projects RENAME COLUMN pi_id TO local_pi_id")
        project_cols.discard("pi_id")
        project_cols.add("local_pi_id")
    for col, definition in [
        ("start_year", "INTEGER"),
        ("start_month", "INTEGER"),
        ("end_year", "INTEGER"),
        ("end_month", "INTEGER"),
        ("local_pi_id", "INTEGER REFERENCES employees(id)"),
        ("personnel_budget", "REAL"),
        ("admin_group_id", "INTEGER REFERENCES groups(id)"),
    ]:
        if col not in project_cols:
            conn.execute(f"ALTER TABLE projects ADD COLUMN {col} {definition}")

    conn.commit()
