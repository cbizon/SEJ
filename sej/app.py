"""Flask web application for viewing effort allocation data."""

from pathlib import Path

from flask import Flask, render_template

from sej.queries import get_spreadsheet_rows


def create_app(db_path=None):
    """Application factory.

    Args:
        db_path: Path to the SQLite database.  Defaults to
                 ``IET_2_8_26_anon.db`` in the current working directory.
    """
    if db_path is None:
        db_path = Path("IET_2_8_26_anon.db")

    app = Flask(__name__)
    app.config["DB_PATH"] = str(db_path)

    @app.route("/")
    def index():
        headers, rows = get_spreadsheet_rows(app.config["DB_PATH"])
        return render_template("index.html", headers=headers, rows=rows)

    return app


def main():
    """CLI entry point: ``sej-web [DB_PATH]``."""
    import sys
    from sej.db import get_connection

    db_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("IET_2_8_26_anon.db")

    if not db_path.exists():
        sys.exit(f"Error: database file not found: {db_path}\nRun load_tsv() first to populate the database.")

    conn = get_connection(db_path)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    if "efforts" not in tables:
        sys.exit(f"Error: {db_path} exists but has not been initialized.\nRun load_tsv() first to populate the database.")

    create_app(db_path).run()
