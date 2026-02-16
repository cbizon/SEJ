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
            id   INTEGER PRIMARY KEY,
            name TEXT    UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS employees (
            id       INTEGER PRIMARY KEY,
            name     TEXT    UNIQUE NOT NULL,
            group_id INTEGER NOT NULL REFERENCES groups(id)
        );

        CREATE TABLE IF NOT EXISTS projects (
            id           INTEGER PRIMARY KEY,
            project_code TEXT    UNIQUE NOT NULL,
            name         TEXT
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
    conn.commit()
