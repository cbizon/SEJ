"""Flask web application for viewing and editing effort allocation data."""

from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, render_template, request, send_file, abort

from sej.branch import create_branch, merge_branch, delete_branch, list_branches
from sej.queries import (
    get_spreadsheet_rows,
    get_spreadsheet_rows_with_ids,
    get_employees,
    get_projects,
    get_budget_lines,
    get_branch_info,
    update_effort,
    add_allocation_line,
    add_employee,
    add_group,
    add_project,
    update_project,
    add_budget_line,
    update_budget_line,
    update_employee,
    fix_totals,
    get_audit_log,
    get_nonproject_by_group,
    get_nonproject_by_person,
    get_groups,
    get_group_details,
    get_project_details,
    get_project_change_history,
)


def _resolve_db(app):
    """Return the active DB path: the branch DB if one exists, else main."""
    main = app.config["MAIN_DB_PATH"]
    branches = list_branches(main)
    if branches:
        return branches[0]["path"]
    return main


def create_app(db_path=None):
    """Application factory.

    Args:
        db_path: Path to the SQLite database.  Defaults to
                 ``data/sej.db`` relative to the current working directory.
    """
    if db_path is None:
        db_path = Path("data/sej.db")

    app = Flask(__name__)
    app.config["MAIN_DB_PATH"] = str(db_path)
    # Keep DB_PATH for backwards compatibility — it's the active DB
    app.config["DB_PATH"] = str(db_path)

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/api/data")
    def api_data():
        db = _resolve_db(app)
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
        return jsonify(get_branch_info(_resolve_db(app)))

    @app.route("/api/employees")
    def api_employees():
        return jsonify(get_employees(_resolve_db(app)))

    @app.route("/api/projects")
    def api_projects():
        return jsonify(get_projects(_resolve_db(app)))

    @app.route("/api/budget-lines")
    def api_budget_lines():
        return jsonify(get_budget_lines(_resolve_db(app)))

    @app.route("/api/effort", methods=["PUT"])
    def api_update_effort():
        db = _resolve_db(app)
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403

        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "Request body must be JSON"}), 400
        for key in ("allocation_line_id", "year", "month"):
            if key not in body:
                return jsonify({"error": f"Missing required field: {key}"}), 400

        allocation_line_id = int(body["allocation_line_id"])
        year = int(body["year"])
        month = int(body["month"])
        percentage = body.get("percentage")

        if percentage is not None:
            percentage = float(percentage)
            if percentage < 0 or percentage > 100:
                return jsonify({"error": "Percentage must be between 0 and 100"}), 400

        try:
            update_effort(db, allocation_line_id, year, month, percentage)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify({"ok": True})

    @app.route("/api/allocation_line", methods=["POST"])
    def api_add_allocation_line():
        db = _resolve_db(app)
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403

        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "Request body must be JSON"}), 400
        for key in ("employee_name", "budget_line_code"):
            if key not in body:
                return jsonify({"error": f"Missing required field: {key}"}), 400

        employee_name = body["employee_name"]
        budget_line_code = body["budget_line_code"]

        try:
            line_id = add_allocation_line(db, employee_name, budget_line_code)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify({"allocation_line_id": line_id})

    @app.route("/api/employee", methods=["POST"])
    def api_add_employee():
        db = _resolve_db(app)
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403

        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "Request body must be JSON"}), 400
        for key in ("first_name", "last_name", "group_name"):
            if key not in body or not str(body[key]).strip():
                return jsonify({"error": f"Missing required field: {key}"}), 400

        first_name = body["first_name"].strip()
        last_name = body["last_name"].strip()
        middle_name = body.get("middle_name", "").strip()
        group_name = body["group_name"].strip()

        salary = body.get("salary", 120000)
        try:
            emp_id = add_employee(db, last_name, first_name, middle_name, group_name, salary)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify({"employee_id": emp_id})

    @app.route("/api/employee", methods=["PUT"])
    def api_update_employee():
        db = _resolve_db(app)
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403

        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "Request body must be JSON"}), 400
        if "employee_id" not in body:
            return jsonify({"error": "Missing required field: employee_id"}), 400

        employee_id = int(body["employee_id"])
        salary = body.get("salary")
        if salary is not None:
            salary = float(salary)

        date_fields = {}
        for field in ("start_year", "start_month", "end_year", "end_month"):
            val = body.get(field)
            if val is not None:
                try:
                    val = int(val)
                except (TypeError, ValueError):
                    return jsonify({"error": f"{field} must be an integer"}), 400
            date_fields[field] = val

        try:
            update_employee(
                db,
                employee_id,
                salary=salary,
                **date_fields,
            )
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify({"ok": True})

    @app.route("/api/group", methods=["POST"])
    def api_add_group():
        db = _resolve_db(app)
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403

        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "Request body must be JSON"}), 400
        if "name" not in body or not str(body["name"]).strip():
            return jsonify({"error": "Missing required field: name"}), 400

        name = body["name"].strip()
        is_internal = bool(body.get("is_internal", True))

        try:
            group_id = add_group(db, name, is_internal)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify({"group_id": group_id})

    @app.route("/api/project", methods=["POST"])
    def api_add_project():
        db = _resolve_db(app)
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403

        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "Request body must be JSON"}), 400
        if "name" not in body or not body["name"].strip():
            return jsonify({"error": "Missing required field: name"}), 400

        try:
            project_id = add_project(
                db,
                body["name"].strip(),
                local_pi_id=body.get("local_pi_id"),
                admin_group_id=body.get("admin_group_id"),
                start_year=body.get("start_year"),
                start_month=body.get("start_month"),
                end_year=body.get("end_year"),
                end_month=body.get("end_month"),
            )
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify({"project_id": project_id})

    @app.route("/api/project", methods=["PUT"])
    def api_update_project():
        db = _resolve_db(app)
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403

        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "Request body must be JSON"}), 400
        if "project_id" not in body:
            return jsonify({"error": "Missing required field: project_id"}), 400

        name = body.get("name")
        if name is not None:
            name = name.strip() or None
        try:
            update_project(
                db,
                int(body["project_id"]),
                name=name,
                local_pi_id=body.get("local_pi_id"),
                admin_group_id=body.get("admin_group_id"),
                start_year=body.get("start_year"),
                start_month=body.get("start_month"),
                end_year=body.get("end_year"),
                end_month=body.get("end_month"),
            )
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify({"ok": True})

    @app.route("/api/budget-line", methods=["POST"])
    def api_add_budget_line():
        db = _resolve_db(app)
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403

        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "Request body must be JSON"}), 400
        if "project_id" not in body:
            return jsonify({"error": "Missing required field: project_id"}), 400

        display_name = body.get("display_name")
        if display_name is not None:
            display_name = display_name.strip() or None
        try:
            code = add_budget_line(
                db,
                int(body["project_id"]),
                display_name=display_name,
                budget_line_code=body.get("budget_line_code"),
                start_year=body.get("start_year"),
                start_month=body.get("start_month"),
                end_year=body.get("end_year"),
                end_month=body.get("end_month"),
                personnel_budget=body.get("personnel_budget"),
            )
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify({"budget_line_code": code})

    @app.route("/api/budget-line", methods=["PUT"])
    def api_update_budget_line():
        db = _resolve_db(app)
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403

        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "Request body must be JSON"}), 400
        if "budget_line_code" not in body:
            return jsonify({"error": "Missing required field: budget_line_code"}), 400

        display_name = body.get("display_name")
        if display_name is not None:
            display_name = display_name.strip() or None
        project_id = body.get("project_id")
        if project_id is not None:
            project_id = int(project_id)
        try:
            update_budget_line(
                db,
                body["budget_line_code"],
                display_name=display_name,
                start_year=body.get("start_year"),
                start_month=body.get("start_month"),
                end_year=body.get("end_year"),
                end_month=body.get("end_month"),
                personnel_budget=body.get("personnel_budget"),
                project_id=project_id,
            )
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        return jsonify({"ok": True})

    @app.route("/api/fix-totals", methods=["POST"])
    def api_fix_totals():
        db = _resolve_db(app)
        info = get_branch_info(db)
        if info.get("db_role") != "branch":
            return jsonify({"error": "Editing is only allowed on branch databases"}), 403
        changes = fix_totals(db)
        return jsonify({"changes": changes})

    @app.route("/api/branch/create", methods=["POST"])
    def api_branch_create():
        main = app.config["MAIN_DB_PATH"]
        if list_branches(main):
            return jsonify({"error": "A branch already exists. Merge or discard it first."}), 409

        name = datetime.now(timezone.utc).strftime("edit-%Y%m%d-%H%M%S")
        create_branch(main, name)
        return jsonify({"branch_name": name})

    @app.route("/api/branch/merge", methods=["POST"])
    def api_branch_merge():
        main = app.config["MAIN_DB_PATH"]
        branches = list_branches(main)
        if not branches:
            return jsonify({"error": "No active branch to merge."}), 409

        branch_name = branches[0]["name"]
        tsv_path = merge_branch(main, branch_name)
        return jsonify({
            "merged": branch_name,
            "changes_file": str(tsv_path) if tsv_path else None,
        })

    @app.route("/api/branch/discard", methods=["POST"])
    def api_branch_discard():
        main = app.config["MAIN_DB_PATH"]
        branches = list_branches(main)
        if not branches:
            return jsonify({"error": "No active branch to discard."}), 409

        branch_name = branches[0]["name"]
        delete_branch(main, branch_name)
        return jsonify({"discarded": branch_name})

    @app.route("/budget-lines")
    def budget_lines_page():
        return render_template("budget_lines.html")

    @app.route("/reports")
    def reports():
        return render_template("reports.html")

    @app.route("/reports/nonproject-by-group")
    def report_nonproject_by_group():
        return render_template("nonproject_by_group.html")

    @app.route("/api/nonproject-by-group")
    def api_nonproject_by_group():
        main = app.config["MAIN_DB_PATH"]
        return jsonify(get_nonproject_by_group(main))

    @app.route("/reports/nonproject-by-person")
    def report_nonproject_by_person():
        return render_template("nonproject_by_person.html")

    @app.route("/api/nonproject-by-person")
    def api_nonproject_by_person():
        main = app.config["MAIN_DB_PATH"]
        return jsonify(get_nonproject_by_person(main))

    @app.route("/reports/group-details")
    def report_group_details():
        return render_template("group_details.html")

    @app.route("/api/groups")
    def api_groups():
        return jsonify(get_groups(_resolve_db(app)))

    @app.route("/api/group-details")
    def api_group_details():
        main = app.config["MAIN_DB_PATH"]
        group = request.args.get("group", "")
        if not group:
            return jsonify({"error": "Missing required parameter: group"}), 400
        return jsonify(get_group_details(main, group))

    @app.route("/reports/project-details")
    def report_project_details():
        return render_template("project_details.html")

    @app.route("/api/project-details")
    def api_project_details():
        main = app.config["MAIN_DB_PATH"]
        budget_line = request.args.get("budget_line", "")
        if not budget_line:
            return jsonify({"error": "Missing required parameter: budget_line"}), 400
        return jsonify(get_project_details(main, budget_line))

    @app.route("/api/project-change-history")
    def api_project_change_history():
        main = app.config["MAIN_DB_PATH"]
        budget_line = request.args.get("budget_line", "")
        if not budget_line:
            return jsonify({"error": "Missing required parameter: budget_line"}), 400
        return jsonify(get_project_change_history(main, budget_line))

    @app.route("/history")
    def history():
        return render_template("history.html")

    @app.route("/api/history")
    def api_history():
        main = app.config["MAIN_DB_PATH"]
        entries = get_audit_log(main)
        return jsonify(entries)

    @app.route("/merges/<path:filename>")
    def serve_merge_tsv(filename):
        main = Path(app.config["MAIN_DB_PATH"])
        merges_dir = main.parent / "merges"
        tsv_path = (merges_dir / filename).resolve()
        # Ensure the resolved path is inside merges_dir (no path traversal)
        if merges_dir.resolve() not in tsv_path.parents:
            abort(404)
        if not tsv_path.exists():
            abort(404)
        return send_file(tsv_path, mimetype="text/tab-separated-values",
                         as_attachment=True, download_name=tsv_path.name)

    return app


def main():
    """CLI entry point: ``sej-web [DB_PATH]``."""
    import sys
    from sej.db import get_connection

    # Expect at most one optional positional argument: DB_PATH
    if len(sys.argv) > 2:
        sys.exit("Usage: sej-web [DB_PATH]")
    db_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/sej.db")

    if not db_path.exists():
        sys.exit(f"Error: database file not found: {db_path}\nRun load_tsv() first to populate the database.")

    conn = get_connection(db_path)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    if "efforts" not in tables:
        sys.exit(f"Error: {db_path} exists but has not been initialized.\nRun load_tsv() first to populate the database.")

    create_app(db_path).run()
