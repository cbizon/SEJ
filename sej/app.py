"""Flask web application for viewing and editing effort allocation data."""

from pathlib import Path

from flask import Flask, jsonify, render_template, request

from sej.queries import (
    get_spreadsheet_rows,
    get_spreadsheet_rows_with_ids,
    get_employees,
    get_projects,
    get_branch_info,
    update_effort,
    add_allocation_line,
)


def _parse_month_label(label: str) -> tuple[int, int]:
    """Parse 'July 2025' into (2025, 7)."""
    from datetime import datetime
    dt = datetime.strptime(label, "%B %Y")
    return dt.year, dt.month


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
        return render_template("index.html")

    @app.route("/api/data")
    def api_data():
        db = app.config["DB_PATH"]
        info = get_branch_info(db)
        is_branch = info.get("db_role") == "branch"

        if is_branch:
            headers, rows = get_spreadsheet_rows_with_ids(db)
        else:
            headers, rows = get_spreadsheet_rows(db)

        data = [dict(zip(headers, row)) for row in rows]
        return jsonify({
            "columns": headers,
            "data": data,
            "editable": is_branch,
            "branch_name": info.get("branch_name"),
        })

    @app.route("/api/branch")
    def api_branch():
        return jsonify(get_branch_info(app.config["DB_PATH"]))

    @app.route("/api/employees")
    def api_employees():
        return jsonify(get_employees(app.config["DB_PATH"]))

    @app.route("/api/projects")
    def api_projects():
        return jsonify(get_projects(app.config["DB_PATH"]))

    @app.route("/api/effort", methods=["PUT"])
    def api_update_effort():
        db = app.config["DB_PATH"]
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403

        body = request.get_json()
        allocation_line_id = body["allocation_line_id"]
        year = body["year"]
        month = body["month"]
        percentage = body.get("percentage")

        if percentage is not None:
            percentage = float(percentage)
            if percentage < 0 or percentage > 100:
                return jsonify({"error": "Percentage must be between 0 and 100"}), 400

        update_effort(db, allocation_line_id, year, month, percentage)
        return jsonify({"ok": True})

    @app.route("/api/allocation_line", methods=["POST"])
    def api_add_allocation_line():
        db = app.config["DB_PATH"]
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403

        body = request.get_json()
        employee_name = body["employee_name"]
        project_code = body["project_code"]
        project_name = body.get("project_name")

        line_id = add_allocation_line(db, employee_name, project_code, project_name)
        return jsonify({"allocation_line_id": line_id})

    return app


def main():
    """CLI entry point: ``sej-web [DB_PATH]``."""
    import sys
    from sej.db import get_connection

    # Expect at most one optional positional argument: DB_PATH
    if len(sys.argv) > 2:
        sys.exit("Usage: sej-web [DB_PATH]")
    db_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("IET_2_8_26_anon.db")

    if not db_path.exists():
        sys.exit(f"Error: database file not found: {db_path}\nRun load_tsv() first to populate the database.")

    conn = get_connection(db_path)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    if "efforts" not in tables:
        sys.exit(f"Error: {db_path} exists but has not been initialized.\nRun load_tsv() first to populate the database.")

    create_app(db_path).run()
