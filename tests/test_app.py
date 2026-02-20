import csv
import json
import sqlite3
import sys
from pathlib import Path

import pytest

from sej.importer import load_tsv
from sej.app import create_app, main
from sej.branch import create_branch


HEADER = [
    "EMPLOYEE", "Group", "Fund Code", "Source", "Account",
    "Cost Code 1", "Cost Code 2", "Cost Code 3", "Program Code",
    "Project Id", "Project Name",
    "July 2025", "August 2025",
]


def _project_id_for(db_path, project_name):
    """Look up the auto-generated project id by name."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT id FROM projects WHERE name = ?", (project_name,)
    ).fetchone()
    conn.close()
    return row["id"]


def _write_tsv(path: Path, rows: list[list[str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(HEADER)
        writer.writerows(rows)


@pytest.fixture
def loaded_db(tmp_path):
    """Create a database loaded with sample data and return its path."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "50.00%", "60.00%"],
        ["", "Engineering", "25210", "49000", "511120",
         "", "", "", "", "5120002", "Gadget Project",
         "50.00%", "40.00%"],
        ["Jones,Bob", "Ops", "", "", "",
         "", "", "", "", "N/A", "N/A",
         "100.00%", "100.00%"],
    ])
    load_tsv(tsv, db)
    return db


@pytest.fixture
def client(loaded_db):
    app = create_app(db_path=loaded_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


@pytest.fixture
def branch_db(loaded_db):
    """Create a branch from the loaded main DB and return its path."""
    return create_branch(loaded_db, "test_edit")


@pytest.fixture
def branch_client(branch_db):
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_index_returns_200(client):
    resp = client.get("/")
    assert resp.status_code == 200


def test_api_data_returns_200(client):
    resp = client.get("/api/data")
    assert resp.status_code == 200


def test_index_contains_column_headers(client):
    payload = client.get("/api/data").json
    cols = payload["columns"]
    for header in ["Employee", "Group", "Fund Code", "Budget Line Code",
                   "Budget Line Name", "July 2025", "August 2025"]:
        assert header in cols


def test_index_contains_employee_names(client):
    payload = client.get("/api/data").json
    employees = [row["Employee"] for row in payload["data"]]
    assert "Smith,Jane" in employees
    assert "Jones,Bob" in employees


def test_index_contains_percentages(client):
    payload = client.get("/api/data").json
    all_values = [v for row in payload["data"] for v in row.values()]
    assert "50.00%" in all_values
    assert "100.00%" in all_values


def test_non_project_shows_non_project(client):
    payload = client.get("/api/data").json
    budget_line_codes = [row["Budget Line Code"] for row in payload["data"]]
    assert "Non-Project" in budget_line_codes


def test_continuation_row_carries_employee_name(client):
    """Every allocation line carries the full employee name (no blanking)."""
    payload = client.get("/api/data").json
    smith_rows = [r for r in payload["data"] if r["Employee"] == "Smith,Jane"]
    assert len(smith_rows) == 2


def test_main_exits_on_missing_db(tmp_path, monkeypatch):
    missing = tmp_path / "no_such.db"
    monkeypatch.setattr(sys, "argv", ["sej-web", str(missing)])
    with pytest.raises(SystemExit) as exc:
        main()
    assert "not found" in str(exc.value)


def test_main_exits_on_uninitialized_db(tmp_path, monkeypatch):
    empty_db = tmp_path / "empty_anon.db"
    sqlite3.connect(empty_db).close()  # create empty file with no tables
    monkeypatch.setattr(sys, "argv", ["sej-web", str(empty_db)])
    with pytest.raises(SystemExit) as exc:
        main()
    assert "initialized" in str(exc.value)


# --- New endpoint tests ---

def test_api_data_not_editable_on_main(client):
    payload = client.get("/api/data").json
    assert payload["editable"] is False
    assert payload["branch_name"] is None


def test_api_data_editable_on_branch(branch_client):
    payload = branch_client.get("/api/data").json
    assert payload["editable"] is True
    assert payload["branch_name"] == "test_edit"
    assert "allocation_line_id" in payload["columns"]


def test_api_branch_info(branch_client):
    resp = branch_client.get("/api/branch")
    assert resp.status_code == 200
    data = resp.json
    assert data["db_role"] == "branch"
    assert data["branch_name"] == "test_edit"


def test_api_employees(client):
    resp = client.get("/api/employees")
    assert resp.status_code == 200
    names = [e["name"] for e in resp.json]
    assert "Smith,Jane" in names
    assert "Jones,Bob" in names


def test_api_employees_includes_salary(client):
    resp = client.get("/api/employees")
    assert resp.status_code == 200
    for e in resp.json:
        assert "salary" in e
        assert e["salary"] == 120000.0


def test_api_add_employee_with_salary(branch_client, branch_db):
    resp = branch_client.post("/api/employee", json={
        "first_name": "Alice",
        "last_name": "Smith",
        "middle_name": "",
        "group_name": "Engineering",
        "salary": 95000.0,
    })
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    emp = conn.execute(
        "SELECT salary FROM employees WHERE name = ?", ("Smith,Alice",)
    ).fetchone()
    conn.close()
    assert emp["salary"] == 95000.0


def test_api_add_employee_default_salary(branch_client, branch_db):
    resp = branch_client.post("/api/employee", json={
        "first_name": "Carol",
        "last_name": "Brown",
        "middle_name": "",
        "group_name": "Engineering",
    })
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    emp = conn.execute(
        "SELECT salary FROM employees WHERE name = ?", ("Brown,Carol",)
    ).fetchone()
    conn.close()
    assert emp["salary"] == 120000.0


def test_api_projects(client):
    resp = client.get("/api/projects")
    assert resp.status_code == 200
    names = [p["name"] for p in resp.json]
    assert "Widget Project" in names
    assert "Non-Project" in names


def test_api_budget_lines(client):
    resp = client.get("/api/budget-lines")
    assert resp.status_code == 200
    codes = [bl["budget_line_code"] for bl in resp.json]
    assert "5120001" in codes
    assert "Non-Project" in codes


def test_api_effort_forbidden_on_main(client):
    resp = client.put("/api/effort", json={
        "allocation_line_id": 1, "year": 2025, "month": 7, "percentage": 80.0,
    })
    assert resp.status_code == 403


def test_api_effort_update(branch_client, branch_db):
    # Get an allocation_line_id from the data
    payload = branch_client.get("/api/data").json
    row = payload["data"][0]
    line_id = row["allocation_line_id"]

    resp = branch_client.put("/api/effort", json={
        "allocation_line_id": line_id, "year": 2025, "month": 7, "percentage": 80.0,
    })
    assert resp.status_code == 200

    # Verify the value changed
    from sej.db import get_connection
    conn = get_connection(branch_db)
    effort = conn.execute(
        "SELECT percentage FROM efforts WHERE allocation_line_id = ? AND year = 2025 AND month = 7",
        (line_id,),
    ).fetchone()
    conn.close()
    assert effort["percentage"] == 80.0


def test_api_effort_missing_json(branch_client):
    resp = branch_client.put("/api/effort", data="not json",
                             content_type="text/plain")
    assert resp.status_code == 400


def test_api_effort_missing_fields(branch_client):
    resp = branch_client.put("/api/effort", json={"allocation_line_id": 1})
    assert resp.status_code == 400


def test_api_effort_invalid_percentage(branch_client):
    payload = branch_client.get("/api/data").json
    line_id = payload["data"][0]["allocation_line_id"]

    resp = branch_client.put("/api/effort", json={
        "allocation_line_id": line_id, "year": 2025, "month": 7, "percentage": 150.0,
    })
    assert resp.status_code == 400


def test_api_allocation_line_forbidden_on_main(client):
    resp = client.post("/api/allocation_line", json={
        "employee_name": "Smith,Jane", "budget_line_code": "NEW001",
    })
    assert resp.status_code == 403


def test_api_allocation_line_create(branch_client):
    resp = branch_client.post("/api/allocation_line", json={
        "employee_name": "Smith,Jane",
        "budget_line_code": "5120001",
    })
    assert resp.status_code == 200
    assert "allocation_line_id" in resp.json


def test_api_allocation_line_unknown_project(branch_client):
    resp = branch_client.post("/api/allocation_line", json={
        "employee_name": "Smith,Jane",
        "budget_line_code": "NONEXISTENT",
    })
    assert resp.status_code == 400


def test_api_add_project_forbidden_on_main(client):
    resp = client.post("/api/project", json={"name": "Test Project"})
    assert resp.status_code == 403


def test_api_add_project(branch_client, branch_db):
    resp = branch_client.post("/api/project", json={
        "name": "Brand New Project",
    })
    assert resp.status_code == 200
    project_id = resp.json["project_id"]

    from sej.db import get_connection
    conn = get_connection(branch_db)
    proj = conn.execute(
        "SELECT name FROM projects WHERE id = ?", (project_id,)
    ).fetchone()
    conn.close()
    assert proj["name"] == "Brand New Project"


def test_api_add_project_with_details(branch_client, branch_db):
    employees = branch_client.get("/api/employees").json
    internal = next(e for e in employees if e["is_internal"])
    groups = branch_client.get("/api/groups").json
    admin_group_id = groups[0]["id"]

    resp = branch_client.post("/api/project", json={
        "name": "Detailed Project",
        "local_pi_id": internal["id"],
        "admin_group_id": admin_group_id,
    })
    assert resp.status_code == 200
    project_id = resp.json["project_id"]

    from sej.db import get_connection
    conn = get_connection(branch_db)
    proj = conn.execute(
        "SELECT * FROM projects WHERE id = ?", (project_id,)
    ).fetchone()
    conn.close()
    assert proj["name"] == "Detailed Project"
    assert proj["local_pi_id"] == internal["id"]
    assert proj["admin_group_id"] == admin_group_id


def test_api_add_project_with_dates(branch_client, branch_db):
    resp = branch_client.post("/api/project", json={
        "name": "Dated Project",
        "start_year": 2025, "start_month": 7,
        "end_year": 2026, "end_month": 6,
    })
    assert resp.status_code == 200
    project_id = resp.json["project_id"]

    from sej.db import get_connection
    conn = get_connection(branch_db)
    proj = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    conn.close()
    assert proj["start_year"] == 2025
    assert proj["start_month"] == 7
    assert proj["end_year"] == 2026
    assert proj["end_month"] == 6


def test_api_add_project_unpaired_date_rejected(branch_client):
    resp = branch_client.post("/api/project", json={
        "name": "Bad Date Project",
        "start_year": 2025,
    })
    assert resp.status_code == 400


def test_api_add_project_external_pi_rejected(branch_client, branch_db):
    """Local PI must be an internal employee."""
    from sej.queries import add_group, add_employee
    add_group(branch_db, "External Org", is_internal=False)
    add_employee(branch_db, "External", "Person", "", "External Org")
    employees = branch_client.get("/api/employees").json
    external = next(e for e in employees if not e["is_internal"])

    resp = branch_client.post("/api/project", json={
        "name": "Bad PI Project",
        "local_pi_id": external["id"],
    })
    assert resp.status_code == 400
    assert "internal" in resp.json["error"]


def test_api_add_budget_line_forbidden_on_main(client):
    resp = client.post("/api/budget-line", json={"project_id": 1})
    assert resp.status_code == 403


def test_api_add_budget_line(branch_client, branch_db):
    # First create a project
    resp = branch_client.post("/api/project", json={"name": "BL Test Project"})
    project_id = resp.json["project_id"]

    resp = branch_client.post("/api/budget-line", json={
        "project_id": project_id,
        "display_name": "Budget Line A",
        "start_year": 2025,
        "start_month": 1,
        "end_year": 2027,
        "end_month": 12,
        "personnel_budget": 750000.0,
    })
    assert resp.status_code == 200
    code = resp.json["budget_line_code"]

    from sej.db import get_connection
    conn = get_connection(branch_db)
    bl = conn.execute(
        "SELECT * FROM budget_lines WHERE budget_line_code = ?", (code,)
    ).fetchone()
    conn.close()
    assert bl["display_name"] == "Budget Line A"
    assert bl["start_year"] == 2025
    assert bl["start_month"] == 1
    assert bl["end_year"] == 2027
    assert bl["end_month"] == 12
    assert bl["personnel_budget"] == 750000.0


def test_api_add_budget_line_unpaired_start_date_rejected(branch_client, branch_db):
    """start_year without start_month is rejected."""
    resp = branch_client.post("/api/project", json={"name": "Dates Test"})
    project_id = resp.json["project_id"]

    resp = branch_client.post("/api/budget-line", json={
        "project_id": project_id,
        "start_year": 2025,
    })
    assert resp.status_code == 400
    assert "start_year" in resp.json["error"]


def test_api_add_budget_line_unpaired_end_date_rejected(branch_client, branch_db):
    """end_month without end_year is rejected."""
    resp = branch_client.post("/api/project", json={"name": "Dates Test"})
    project_id = resp.json["project_id"]

    resp = branch_client.post("/api/budget-line", json={
        "project_id": project_id,
        "end_month": 6,
    })
    assert resp.status_code == 400
    assert "end_year" in resp.json["error"]


def test_api_add_budget_line_invalid_month_rejected(branch_client, branch_db):
    """Month outside 1-12 is rejected."""
    resp = branch_client.post("/api/project", json={"name": "Month Test"})
    project_id = resp.json["project_id"]

    resp = branch_client.post("/api/budget-line", json={
        "project_id": project_id,
        "start_year": 2025,
        "start_month": 13,
    })
    assert resp.status_code == 400
    assert "start_month" in resp.json["error"]


def test_api_add_budget_line_non_4digit_year_rejected(branch_client, branch_db):
    """Year that is not a 4-digit number is rejected for budget lines."""
    resp = branch_client.post("/api/project", json={"name": "Year Test"})
    project_id = resp.json["project_id"]

    resp = branch_client.post("/api/budget-line", json={
        "project_id": project_id,
        "start_year": 1,
        "start_month": 7,
    })
    assert resp.status_code == 400
    assert "4-digit" in resp.json["error"]


def test_api_add_project_non_4digit_year_rejected(branch_client):
    """Year that is not a 4-digit number is rejected for projects."""
    resp = branch_client.post("/api/project", json={
        "name": "Year Test Project",
        "start_year": 1,
        "start_month": 7,
    })
    assert resp.status_code == 400
    assert "4-digit" in resp.json["error"]


def test_api_add_budget_line_negative_budget_rejected(branch_client, branch_db):
    """Negative personnel budget is rejected."""
    resp = branch_client.post("/api/project", json={"name": "Budget Test"})
    project_id = resp.json["project_id"]

    resp = branch_client.post("/api/budget-line", json={
        "project_id": project_id,
        "personnel_budget": -100.0,
    })
    assert resp.status_code == 400
    assert "personnel_budget" in resp.json["error"]


def test_api_update_project_forbidden_on_main(client):
    resp = client.put("/api/project", json={
        "project_id": 1,
        "name": "Updated Name",
    })
    assert resp.status_code == 403


def test_api_update_project(branch_client, branch_db):
    employees = branch_client.get("/api/employees").json
    internal = next(e for e in employees if e["is_internal"])
    groups = branch_client.get("/api/groups").json
    admin_group_id = groups[0]["id"]

    # Get the project id for the project containing budget line 5120001
    budget_lines = branch_client.get("/api/budget-lines").json
    bl = next(b for b in budget_lines if b["budget_line_code"] == "5120001")
    project_id = bl["project_id"]

    resp = branch_client.put("/api/project", json={
        "project_id": project_id,
        "name": "Widget Project Updated",
        "local_pi_id": internal["id"],
        "admin_group_id": admin_group_id,
    })
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    proj = conn.execute(
        "SELECT * FROM projects WHERE id = ?", (project_id,)
    ).fetchone()
    conn.close()
    assert proj["name"] == "Widget Project Updated"
    assert proj["local_pi_id"] == internal["id"]
    assert proj["admin_group_id"] == admin_group_id


def test_api_update_project_with_dates(branch_client, branch_db):
    budget_lines = branch_client.get("/api/budget-lines").json
    bl = next(b for b in budget_lines if b["budget_line_code"] == "5120001")
    project_id = bl["project_id"]

    resp = branch_client.put("/api/project", json={
        "project_id": project_id,
        "start_year": 2025, "start_month": 1,
        "end_year": 2026, "end_month": 12,
    })
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    proj = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    conn.close()
    assert proj["start_year"] == 2025
    assert proj["start_month"] == 1
    assert proj["end_year"] == 2026
    assert proj["end_month"] == 12


def test_api_update_project_unpaired_date_rejected(branch_client, branch_db):
    budget_lines = branch_client.get("/api/budget-lines").json
    bl = next(b for b in budget_lines if b["budget_line_code"] == "5120001")
    project_id = bl["project_id"]

    resp = branch_client.put("/api/project", json={
        "project_id": project_id,
        "end_year": 2026,
    })
    assert resp.status_code == 400


def test_api_update_project_dates_cleared(branch_client, branch_db):
    """Passing null dates clears them."""
    budget_lines = branch_client.get("/api/budget-lines").json
    bl = next(b for b in budget_lines if b["budget_line_code"] == "5120001")
    project_id = bl["project_id"]

    branch_client.put("/api/project", json={
        "project_id": project_id,
        "start_year": 2025, "start_month": 1,
        "end_year": 2026, "end_month": 12,
    })
    resp = branch_client.put("/api/project", json={
        "project_id": project_id,
        "start_year": None, "start_month": None,
        "end_year": None, "end_month": None,
    })
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    proj = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    conn.close()
    assert proj["start_year"] is None
    assert proj["end_year"] is None


def test_api_projects_returns_date_fields(client):
    resp = client.get("/api/projects")
    assert resp.status_code == 200
    for p in resp.json:
        assert "start_year" in p
        assert "start_month" in p
        assert "end_year" in p
        assert "end_month" in p


def test_api_update_project_external_pi_rejected(branch_client, branch_db):
    """Local PI must be an internal employee."""
    from sej.queries import add_group, add_employee
    add_group(branch_db, "External Org", is_internal=False)
    add_employee(branch_db, "External", "Person", "", "External Org")
    employees = branch_client.get("/api/employees").json
    external = next(e for e in employees if not e["is_internal"])

    budget_lines = branch_client.get("/api/budget-lines").json
    bl = next(b for b in budget_lines if b["budget_line_code"] == "5120001")
    project_id = bl["project_id"]

    resp = branch_client.put("/api/project", json={
        "project_id": project_id,
        "local_pi_id": external["id"],
    })
    assert resp.status_code == 400
    assert "internal" in resp.json["error"]


def test_api_update_project_not_found(branch_client):
    resp = branch_client.put("/api/project", json={
        "project_id": 99999,
        "name": "Nope",
    })
    assert resp.status_code == 400


def test_api_update_project_duplicate_name_rejected(branch_client, branch_db):
    """Renaming a project to a name already used by another project is rejected."""
    from sej.queries import get_projects
    projects = get_projects(branch_db)
    non_np = [p for p in projects if not p["is_nonproject"]]
    assert len(non_np) >= 2, "Need at least two non-nonproject projects for this test"
    p1, p2 = non_np[0], non_np[1]

    resp = branch_client.put("/api/project", json={
        "project_id": p1["id"],
        "name": p2["name"],
    })
    assert resp.status_code == 400
    assert "already exists" in resp.json["error"]


def test_api_update_budget_line_forbidden_on_main(client):
    resp = client.put("/api/budget-line", json={
        "budget_line_code": "5120001",
        "name": "Updated Name",
    })
    assert resp.status_code == 403


def test_api_update_budget_line_dates_reject_early_effort(branch_client):
    """Setting start after existing effort is rejected."""
    # 5120001 has effort in Jul 2025 and Aug 2025; start after that should fail
    resp = branch_client.put("/api/budget-line", json={
        "budget_line_code": "5120001",
        "start_year": 2025,
        "start_month": 8,  # Aug 2025 — Jul 2025 is before this
    })
    assert resp.status_code == 400
    assert "Jul 2025" in resp.json["error"]


def test_api_update_budget_line_dates_reject_late_effort(branch_client):
    """Setting end before existing effort is rejected."""
    # 5120001 has effort in Aug 2025; end before that should fail
    resp = branch_client.put("/api/budget-line", json={
        "budget_line_code": "5120001",
        "end_year": 2025,
        "end_month": 7,  # Jul 2025 — Aug 2025 is after this
    })
    assert resp.status_code == 400
    assert "Aug 2025" in resp.json["error"]


def test_api_update_budget_line_dates_accept_valid_range(branch_client):
    """Setting dates that encompass all existing effort succeeds."""
    resp = branch_client.put("/api/budget-line", json={
        "budget_line_code": "5120001",
        "start_year": 2025,
        "start_month": 7,
        "end_year": 2025,
        "end_month": 8,
    })
    assert resp.status_code == 200


def test_api_budget_lines_returns_detail_fields(client):
    resp = client.get("/api/budget-lines")
    assert resp.status_code == 200
    bl = next(b for b in resp.json if b["budget_line_code"] == "5120001")
    assert "start_year" in bl
    assert "start_month" in bl
    assert "end_year" in bl
    assert "end_month" in bl
    assert "local_pi_id" in bl
    assert "local_pi_name" in bl
    assert "personnel_budget" in bl
    assert "admin_group_id" in bl
    assert "admin_group_name" in bl
    assert "display_name" in bl


def test_api_budget_lines_returns_display_name(client):
    resp = client.get("/api/budget-lines")
    bl = next(b for b in resp.json if b["budget_line_code"] == "5120001")
    assert bl["display_name"] == "Widget Project"  # initialized from finance name on import
    assert bl["name"] == "Widget Project"  # finance name from import


def test_api_update_budget_line_sets_display_name(branch_client, branch_db):
    resp = branch_client.put("/api/budget-line", json={
        "budget_line_code": "5120001",
        "display_name": "My Friendly Name",
    })
    assert resp.status_code == 200

    from sej.queries import get_budget_lines
    bls = get_budget_lines(branch_db)
    bl = next(b for b in bls if b["budget_line_code"] == "5120001")
    assert bl["display_name"] == "My Friendly Name"
    assert bl["name"] == "Widget Project"  # finance name unchanged


def test_display_name_used_in_spreadsheet_rows(branch_client, branch_db):
    """When display_name is set, it appears as Budget Line Name in the data API."""
    branch_client.put("/api/budget-line", json={
        "budget_line_code": "5120001",
        "display_name": "Friendly Widget",
    })
    payload = branch_client.get("/api/data").json
    names = [row.get("Budget Line Name") for row in payload["data"]]
    assert "Friendly Widget" in names
    assert "Widget Project" not in names  # finance name replaced


def test_display_name_initialized_from_finance_name_on_import(tmp_path):
    """On first import, display_name is set to the finance name."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "50.00%", "60.00%"],
    ])
    load_tsv(tsv, db)

    from sej.db import get_connection
    conn = get_connection(db)
    bl = conn.execute(
        "SELECT name, display_name FROM budget_lines WHERE budget_line_code = ?",
        ("5120001",),
    ).fetchone()
    conn.close()
    assert bl["name"] == "Widget Project"
    assert bl["display_name"] == "Widget Project"


def test_display_name_preserved_across_reimport(tmp_path):
    """A user-changed display_name survives a re-import; finance name is still refreshed."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "50.00%", "60.00%"],
    ])
    load_tsv(tsv, db)

    from sej.db import get_connection
    conn = get_connection(db)
    conn.execute(
        "UPDATE budget_lines SET display_name = ? WHERE budget_line_code = ?",
        ("My Friendly Name", "5120001"),
    )
    conn.commit()
    conn.close()

    # Re-import the same TSV
    load_tsv(tsv, db)

    conn = get_connection(db)
    bl = conn.execute(
        "SELECT name, display_name FROM budget_lines WHERE budget_line_code = ?",
        ("5120001",),
    ).fetchone()
    conn.close()
    assert bl["name"] == "Widget Project"         # finance name refreshed from TSV
    assert bl["display_name"] == "My Friendly Name"  # user display_name preserved


# --- Fix totals tests ---

@pytest.fixture
def violations_db(tmp_path):
    """DB where Smith,Jane's July total is 90% (violation) and has a Non-Project line."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        # Smith,Jane: 60% on real project + 30% Non-Project (20152) = 90% (violation)
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "60.00%", ""],
        ["", "Engineering", "20152", "49000", "511120",
         "", "", "", "", "N/A", "N/A",
         "30.00%", ""],
        # Jones,Bob: 100% Non-Project = no violation
        ["Jones,Bob", "Ops", "", "", "",
         "", "", "", "", "N/A", "N/A",
         "100.00%", ""],
    ])
    load_tsv(tsv, db)
    return db


@pytest.fixture
def violations_branch_db(violations_db):
    return create_branch(violations_db, "test_fix")


@pytest.fixture
def violations_branch_client(violations_branch_db):
    app = create_app(db_path=violations_branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_api_fix_totals_forbidden_on_main(violations_db):
    app = create_app(db_path=violations_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        resp = c.post("/api/fix-totals")
    assert resp.status_code == 403


def test_api_fix_totals(violations_branch_client, violations_branch_db):
    resp = violations_branch_client.post("/api/fix-totals")
    assert resp.status_code == 200
    changes = resp.json["changes"]
    # Only Smith,Jane July 2025 needs fixing
    assert len(changes) == 1
    assert changes[0]["year"] == 2025
    assert changes[0]["month"] == 7
    assert abs(changes[0]["old_percentage"] - 30.0) < 0.01
    assert abs(changes[0]["new_percentage"] - 40.0) < 0.01

    from sej.db import get_connection
    conn = get_connection(violations_branch_db)
    np_line = conn.execute("""
        SELECT al.id FROM allocation_lines al
        JOIN employees e ON e.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        JOIN projects p ON p.id = bl.project_id
        WHERE e.name = 'Smith,Jane' AND p.is_nonproject = 1
    """).fetchone()
    effort = conn.execute(
        "SELECT percentage FROM efforts WHERE allocation_line_id = ? AND year = 2025 AND month = 7",
        (np_line["id"],),
    ).fetchone()
    conn.close()
    assert abs(effort["percentage"] - 40.0) < 0.01


def test_api_fix_totals_no_changes_when_balanced(branch_client):
    """When all totals are already 100%, fix-totals returns an empty change list."""
    resp = branch_client.post("/api/fix-totals")
    assert resp.status_code == 200
    assert resp.json["changes"] == []


@pytest.fixture
def multi_nonproject_db(tmp_path):
    """DB where an employee has two Non-Project lines (27152 and 20152) and a violation."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        # Jones,Bob: 60% real project + 20% Non-Project/27152 + 10% Non-Project/20152 = 90%
        ["Jones,Bob", "Ops", "25210", "49000", "511120",
         "", "", "", "", "5120001", "Widget Project",
         "60.00%", ""],
        ["", "Ops", "27152", "49000", "511120",
         "", "", "", "", "N/A", "N/A",
         "20.00%", ""],
        ["", "Ops", "20152", "49000", "511120",
         "", "", "", "", "N/A", "N/A",
         "10.00%", ""],
    ])
    load_tsv(tsv, db)
    return db


def test_api_fix_totals_adds_to_largest_np(multi_nonproject_db):
    """When two NP lines exist, shortfall is added to the one with the most effort."""
    branch_db = create_branch(multi_nonproject_db, "test_multi")
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        resp = c.post("/api/fix-totals")
    assert resp.status_code == 200
    changes = resp.json["changes"]
    # Total is 90%, shortfall is 10%.  The 27152 line has 20% (largest), so it gets bumped.
    assert len(changes) == 1

    from sej.db import get_connection
    conn = get_connection(branch_db)
    # 27152 goes from 20% to 30%; 20152 stays at 10%.
    line_27152 = conn.execute("""
        SELECT al.id FROM allocation_lines al
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        JOIN projects p ON p.id = bl.project_id
        WHERE p.is_nonproject = 1 AND al.fund_code = '27152'
    """).fetchone()
    effort_27152 = conn.execute(
        "SELECT percentage FROM efforts WHERE allocation_line_id = ? AND year = 2025 AND month = 7",
        (line_27152["id"],),
    ).fetchone()
    assert abs(effort_27152["percentage"] - 30.0) < 0.01

    line_20152 = conn.execute("""
        SELECT al.id FROM allocation_lines al
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        JOIN projects p ON p.id = bl.project_id
        WHERE p.is_nonproject = 1 AND al.fund_code = '20152'
    """).fetchone()
    effort_20152 = conn.execute(
        "SELECT percentage FROM efforts WHERE allocation_line_id = ? AND year = 2025 AND month = 7",
        (line_20152["id"],),
    ).fetchone()
    conn.close()
    assert abs(effort_20152["percentage"] - 10.0) < 0.01


@pytest.fixture
def no_nonproject_db(tmp_path):
    """DB where Smith,Jane has effort but no Non-Project allocation line."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        # Smith,Jane: 60% on real project only — no Non-Project line at all
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "60.00%", ""],
        # Jones,Bob: balanced, no issue
        ["Jones,Bob", "Ops", "", "", "",
         "", "", "", "", "N/A", "N/A",
         "100.00%", ""],
    ])
    load_tsv(tsv, db)
    return db


def test_fix_totals_creates_nonproject_line_when_missing(no_nonproject_db):
    """fix_totals creates a Non-Project allocation line when one doesn't exist."""
    branch_db = create_branch(no_nonproject_db, "test_no_np")
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        resp = c.post("/api/fix-totals")
    assert resp.status_code == 200
    changes = resp.json["changes"]
    assert len(changes) == 1
    assert changes[0]["year"] == 2025
    assert changes[0]["month"] == 7
    assert abs(changes[0]["new_percentage"] - 40.0) < 0.01

    from sej.db import get_connection
    conn = get_connection(branch_db)
    np_line = conn.execute("""
        SELECT al.id FROM allocation_lines al
        JOIN employees e ON e.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE e.name = 'Smith,Jane' AND bl.budget_line_code = 'Non-Project'
    """).fetchone()
    assert np_line is not None
    effort = conn.execute(
        "SELECT percentage FROM efforts WHERE allocation_line_id = ? AND year = 2025 AND month = 7",
        (np_line["id"],),
    ).fetchone()
    conn.close()
    assert abs(effort["percentage"] - 40.0) < 0.01


@pytest.fixture
def abbott_db(tmp_path):
    """Abbott case: total=76%, three NP lines, two with 20152 (one empty, one 41%)."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        # Abbott,Christine: 10% NP/27152 + 20% proj + 5% proj + empty NP/20152 + 41% NP/20152 = 76%
        ["Abbott,Christine", "PM", "27152", "49000", "511120",
         "", "", "", "", "N/A", "N/A",
         "10.00%", ""],
        ["", "PM", "25210", "49000", "511120",
         "", "", "", "VRENG", "5130266", "Dashboard Project",
         "20.00%", ""],
        ["", "PM", "25210", "49000", "511120",
         "", "", "", "VRENG", "5135630", "Timesheet Project",
         "5.00%", ""],
        ["", "PM", "20152", "49000", "511120",
         "", "", "", "", "N/A", "N/A",
         "", ""],
        ["", "PM", "20152", "49000", "511120",
         "", "", "", "", "N/A", "N/A",
         "41.00%", ""],
    ])
    load_tsv(tsv, db)
    return db


def test_fix_totals_abbott_adds_to_existing_np(abbott_db):
    """Abbott: shortfall added to the 41% NP/20152 line, not the empty NP/20152 line."""
    branch_db = create_branch(abbott_db, "test_abbott")
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        resp = c.post("/api/fix-totals")
    assert resp.status_code == 200
    changes = resp.json["changes"]
    assert len(changes) == 1
    # The 41% line should become 65%, not the empty line becoming 24%
    assert abs(changes[0]["old_percentage"] - 41.0) < 0.01
    assert abs(changes[0]["new_percentage"] - 65.0) < 0.01

    from sej.db import get_connection
    conn = get_connection(branch_db)
    # The 27152 NP line should be untouched at 10%
    line_27152 = conn.execute("""
        SELECT al.id FROM allocation_lines al
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        JOIN projects p ON p.id = bl.project_id
        WHERE p.is_nonproject = 1 AND al.fund_code = '27152'
    """).fetchone()
    effort_27152 = conn.execute(
        "SELECT percentage FROM efforts WHERE allocation_line_id = ? AND year = 2025 AND month = 7",
        (line_27152["id"],),
    ).fetchone()
    conn.close()
    assert abs(effort_27152["percentage"] - 10.0) < 0.01


@pytest.fixture
def baker_multi_np_over_100_db(tmp_path):
    """Baker case: total=110%, two NP lines, preferred (20152) has no effort, other has 10%."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        # Baker: 65% + 35% real projects = 100%, plus 10% on Non-Project/27152 = 110%
        # The preferred Non-Project line (20152) has no effort for this month.
        ["Baker,Jeremy Boyd", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "65.00%", ""],
        ["", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120002", "Other Project",
         "35.00%", ""],
        ["", "Engineering", "27152", "49000", "511120",
         "", "", "", "", "N/A", "N/A",
         "10.00%", ""],
        ["", "Engineering", "20152", "49000", "511120",
         "", "", "", "", "N/A", "N/A",
         "", ""],
    ])
    load_tsv(tsv, db)
    return db


def test_fix_totals_baker_multi_np_over_100(baker_multi_np_over_100_db):
    """Baker case: two NP lines, preferred has no effort, other has 10%. Total=110%.

    fix_totals must reduce the 10% NP effort to 0%, not silently skip it.
    """
    branch_db = create_branch(baker_multi_np_over_100_db, "test_baker")
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        resp = c.post("/api/fix-totals")
    assert resp.status_code == 200
    changes = resp.json["changes"]
    # The 10% NP effort should be zeroed out
    assert len(changes) >= 1

    from sej.db import get_connection
    conn = get_connection(branch_db)
    # After fix, all NP efforts for Baker in July should be 0
    np_efforts = conn.execute("""
        SELECT e.percentage
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE emp.name = 'Baker,Jeremy Boyd'
          AND bl.budget_line_code = 'Non-Project'
          AND e.year = 2025 AND e.month = 7
    """).fetchall()
    total_np = sum(r["percentage"] for r in np_efforts)
    assert abs(total_np) < 0.01, f"Total NP effort should be 0, got {total_np}"

    # Total for Baker in July should now be 100%
    total = conn.execute("""
        SELECT SUM(e.percentage) AS total
        FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        JOIN employees emp ON emp.id = al.employee_id
        WHERE emp.name = 'Baker,Jeremy Boyd'
          AND e.year = 2025 AND e.month = 7
    """).fetchone()["total"]
    conn.close()
    assert abs(total - 100.0) < 0.01, f"Total should be 100, got {total}"

    # No negative values anywhere
    conn = get_connection(branch_db)
    negatives = conn.execute("SELECT percentage FROM efforts WHERE percentage < 0").fetchall()
    conn.close()
    assert negatives == []


@pytest.fixture
def over_100_np_other_month_db(tmp_path):
    """DB where projects total 110% in July but Non-Project only has effort in August."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        # Baker: 60% + 50% real projects in July = 110%, Non-Project only in August
        ["Baker,Jeremy Boyd", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "60.00%", "50.00%"],
        ["", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120002", "Other Project",
         "50.00%", "40.00%"],
        ["", "Engineering", "20152", "49000", "511120",
         "", "", "", "", "N/A", "N/A",
         "", "10.00%"],
    ])
    load_tsv(tsv, db)
    return db


def test_fix_totals_over_100_np_line_exists_but_no_effort_for_month(over_100_np_other_month_db):
    """When total>100, NP line exists but has no effort for that month: no negative row created."""
    branch_db = create_branch(over_100_np_other_month_db, "test_baker2")
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        resp = c.post("/api/fix-totals")
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    negatives = conn.execute("SELECT percentage FROM efforts WHERE percentage < 0").fetchall()
    conn.close()
    assert negatives == [], "No effort row should have a negative percentage"


@pytest.fixture
def over_100_with_np_db(tmp_path):
    """DB where project efforts alone exceed 100%, plus a Non-Project line."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        # Smith,Jane: 80% + 40% real projects + 10% Non-Project = 130% total.
        # Projects alone (120%) already exceed 100, so Non-Project can't fix it.
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "80.00%", ""],
        ["", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120002", "Other Project",
         "40.00%", ""],
        ["", "Engineering", "20152", "49000", "511120",
         "", "", "", "", "N/A", "N/A",
         "10.00%", ""],
    ])
    load_tsv(tsv, db)
    return db


def test_fix_totals_over_100_np_reduced_to_zero(over_100_with_np_db):
    """When project efforts alone exceed 100%, Non-Project is reduced to 0, not negative."""
    branch_db = create_branch(over_100_with_np_db, "test_over100")
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        resp = c.post("/api/fix-totals")
    assert resp.status_code == 200
    changes = resp.json["changes"]
    # other_sum = 120%, new_np = max(0, -20) = 0 — Non-Project is reduced to 0
    assert len(changes) == 1
    assert abs(changes[0]["new_percentage"] - 0.0) < 0.01

    from sej.db import get_connection
    conn = get_connection(branch_db)
    rows = conn.execute(
        "SELECT percentage FROM efforts WHERE percentage < 0"
    ).fetchall()
    conn.close()
    assert rows == [], "No effort row should have a negative percentage"


@pytest.fixture
def over_100_no_np_db(tmp_path):
    """DB where Smith,Jane's project efforts alone exceed 100%, no Non-Project line."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        # Smith,Jane: 110% on real projects only — no Non-Project line
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "60.00%", ""],
        ["", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120002", "Other Project",
         "50.00%", ""],
    ])
    load_tsv(tsv, db)
    return db


def test_fix_totals_over_100_no_np_line_not_created(over_100_no_np_db):
    """When total > 100 and no Non-Project line exists, fix_totals must not create one."""
    branch_db = create_branch(over_100_no_np_db, "test_over100_nonp")
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        resp = c.post("/api/fix-totals")
    assert resp.status_code == 200
    assert resp.json["changes"] == []

    from sej.db import get_connection
    conn = get_connection(branch_db)
    np_line = conn.execute("""
        SELECT al.id FROM allocation_lines al
        JOIN employees e ON e.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE e.name = 'Smith,Jane' AND bl.budget_line_code = 'Non-Project'
    """).fetchone()
    conn.close()
    assert np_line is None, "No Non-Project line should be created when total > 100"


def test_fix_totals_skips_external_group_employees(tmp_path):
    """fix_totals must not adjust effort for employees in external groups."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        # External,Person: 50% on a project — would be a violation if internal
        ["External,Person", "PartnerOrg", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "50.00%", ""],
    ])
    load_tsv(tsv, db)

    # Mark PartnerOrg as external
    from sej.db import get_connection
    conn = get_connection(db)
    conn.execute("UPDATE groups SET is_internal = 0 WHERE name = 'PartnerOrg'")
    conn.commit()
    conn.close()

    branch_db = create_branch(db, "test_ext")
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        resp = c.post("/api/fix-totals")
    assert resp.status_code == 200
    assert resp.json["changes"] == [], "External group employees must not be adjusted"


# --- Branch workflow API tests ---

@pytest.fixture
def main_client(loaded_db):
    """A test client pointed at the main DB (uses MAIN_DB_PATH for branch resolution)."""
    app = create_app(db_path=loaded_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_api_branch_create(main_client, loaded_db):
    resp = main_client.post("/api/branch/create")
    assert resp.status_code == 200
    data = resp.json
    assert "branch_name" in data
    assert data["branch_name"].startswith("edit-")

    # The app should now serve branch data
    resp2 = main_client.get("/api/data")
    assert resp2.json["editable"] is True


def test_api_branch_create_already_exists(main_client, loaded_db):
    main_client.post("/api/branch/create")
    resp = main_client.post("/api/branch/create")
    assert resp.status_code == 409


def test_api_branch_merge(main_client, loaded_db):
    main_client.post("/api/branch/create")
    resp = main_client.post("/api/branch/merge")
    assert resp.status_code == 200
    assert "merged" in resp.json

    # Should be back on main
    resp2 = main_client.get("/api/data")
    assert resp2.json["editable"] is False


def test_api_branch_discard(main_client, loaded_db):
    main_client.post("/api/branch/create")
    resp = main_client.post("/api/branch/discard")
    assert resp.status_code == 200
    assert "discarded" in resp.json

    # Should be back on main
    resp2 = main_client.get("/api/data")
    assert resp2.json["editable"] is False


def test_api_branch_merge_on_main(main_client):
    resp = main_client.post("/api/branch/merge")
    assert resp.status_code == 409


def test_api_branch_discard_on_main(main_client):
    resp = main_client.post("/api/branch/discard")
    assert resp.status_code == 409


def test_edit_data_button_visible_on_main(main_client):
    resp = main_client.get("/api/data")
    assert resp.json["editable"] is False


def test_edit_data_button_hidden_on_branch(main_client, loaded_db):
    main_client.post("/api/branch/create")
    resp = main_client.get("/api/data")
    assert resp.json["editable"] is True


# --- History report tests ---

def test_history_page_returns_200(main_client):
    resp = main_client.get("/history")
    assert resp.status_code == 200


def test_api_history_returns_list(main_client):
    resp = main_client.get("/api/history")
    assert resp.status_code == 200
    assert isinstance(resp.json, list)


def test_api_history_contains_load_entry(main_client):
    entries = main_client.get("/api/history").json
    actions = [e["action"] for e in entries]
    assert "load" in actions


def test_api_history_merge_entry_has_tsv_path(main_client, loaded_db):
    # Create a branch, make a change, merge it
    main_client.post("/api/branch/create")
    payload = main_client.get("/api/data").json
    line_id = payload["data"][0]["allocation_line_id"]
    main_client.put("/api/effort", json={
        "allocation_line_id": line_id, "year": 2025, "month": 7, "percentage": 42.0,
    })
    main_client.post("/api/branch/merge")

    entries = main_client.get("/api/history").json
    merge_entries = [e for e in entries if e["action"] == "merge"]
    assert merge_entries, "Expected at least one merge entry"
    assert merge_entries[0]["details"]["tsv_path"] is not None


def test_serve_merge_tsv(main_client, loaded_db):
    # Create branch, change something, merge to produce a TSV
    main_client.post("/api/branch/create")
    payload = main_client.get("/api/data").json
    line_id = payload["data"][0]["allocation_line_id"]
    main_client.put("/api/effort", json={
        "allocation_line_id": line_id, "year": 2025, "month": 7, "percentage": 42.0,
    })
    main_client.post("/api/branch/merge")

    entries = main_client.get("/api/history").json
    merge_entry = next(e for e in entries if e["action"] == "merge")
    tsv_path = merge_entry["details"]["tsv_path"]
    filename = tsv_path.replace("\\", "/").split("/")[-1]

    resp = main_client.get(f"/merges/{filename}")
    assert resp.status_code == 200
    assert b"type\t" in resp.data


def test_serve_merge_tsv_not_found(main_client):
    resp = main_client.get("/merges/nonexistent_file.tsv")
    assert resp.status_code == 404


def test_serve_merge_tsv_path_traversal(main_client):
    resp = main_client.get("/merges/../sej.db")
    assert resp.status_code == 404


# --- Reports pages ---

def test_reports_page_returns_200(main_client):
    resp = main_client.get("/reports")
    assert resp.status_code == 200


def test_report_nonproject_by_group_page_returns_200(main_client):
    resp = main_client.get("/reports/nonproject-by-group")
    assert resp.status_code == 200


def test_api_nonproject_by_group_structure(main_client):
    resp = main_client.get("/api/nonproject-by-group")
    assert resp.status_code == 200
    data = resp.json
    assert "months" in data
    assert "rows" in data
    assert isinstance(data["months"], list)
    assert isinstance(data["rows"], list)


def test_api_nonproject_by_group_contains_groups(main_client):
    data = main_client.get("/api/nonproject-by-group").json
    groups = [r["group"] for r in data["rows"]]
    assert "Engineering" in groups
    assert "Ops" in groups


def test_api_nonproject_by_group_percentages(main_client):
    data = main_client.get("/api/nonproject-by-group").json
    # Jones,Bob (Ops) is 100% Non-Project every month, so Ops should be 100%
    ops_row = next(r for r in data["rows"] if r["group"] == "Ops")
    for month in data["months"]:
        assert abs(ops_row[month] - 100.0) < 0.1, f"Ops {month}: expected 100%, got {ops_row[month]}"


def test_api_nonproject_by_group_engineering_partial(main_client):
    # Smith,Jane (Engineering) has 50%+50%=100% total, none on Non-Project
    data = main_client.get("/api/nonproject-by-group").json
    eng_row = next(r for r in data["rows"] if r["group"] == "Engineering")
    for month in data["months"]:
        assert eng_row[month] == 0.0, f"Engineering {month}: expected 0%, got {eng_row[month]}"


def test_api_nonproject_by_group_has_total_row(main_client):
    data = main_client.get("/api/nonproject-by-group").json
    assert data["rows"][-1]["group"] == "Total"


def test_api_nonproject_by_group_total_correct(main_client):
    # Jones,Bob: 100% NP; Smith,Jane: 0% NP; combined = 50% NP
    data = main_client.get("/api/nonproject-by-group").json
    total_row = data["rows"][-1]
    for month in data["months"]:
        assert abs(total_row[month] - 50.0) < 0.1, (
            f"Total {month}: expected 50%, got {total_row[month]}"
        )


def test_api_nonproject_by_group_has_fte_rows(main_client):
    data = main_client.get("/api/nonproject-by-group").json
    assert "fte_rows" in data
    assert isinstance(data["fte_rows"], list)


def test_api_nonproject_by_group_fte_total_row(main_client):
    data = main_client.get("/api/nonproject-by-group").json
    assert data["fte_rows"][-1]["group"] == "Total"


def test_api_nonproject_by_group_fte_ops(main_client):
    # Jones,Bob (Ops) is 1 employee at 100% NP → FTE = 1.0
    data = main_client.get("/api/nonproject-by-group").json
    ops_row = next(r for r in data["fte_rows"] if r["group"] == "Ops")
    for month in data["months"]:
        assert abs(ops_row[month] - 1.0) < 0.01, f"Ops FTE {month}: expected 1.0, got {ops_row[month]}"


def test_api_nonproject_by_group_fte_engineering(main_client):
    # Smith,Jane (Engineering) has 0% NP → FTE = 0.0
    data = main_client.get("/api/nonproject-by-group").json
    eng_row = next(r for r in data["fte_rows"] if r["group"] == "Engineering")
    for month in data["months"]:
        assert eng_row[month] == 0.0


def test_api_nonproject_by_group_fte_total(main_client):
    # 2 employees total: 1 fully NP (Jones,Bob), 1 fully project (Smith,Jane) → total FTE = 0.5 * 2 = 1.0
    data = main_client.get("/api/nonproject-by-group").json
    total_row = data["fte_rows"][-1]
    for month in data["months"]:
        assert abs(total_row[month] - 1.0) < 0.01, f"Total FTE {month}: expected 1.0, got {total_row[month]}"


# --- Group Details report tests ---

def test_report_group_details_page_returns_200(main_client):
    resp = main_client.get("/reports/group-details")
    assert resp.status_code == 200


def test_api_groups_returns_list(main_client):
    resp = main_client.get("/api/groups")
    assert resp.status_code == 200
    groups = resp.json
    names = [g["name"] for g in groups]
    assert "Engineering" in names
    assert "Ops" in names
    assert all(g["is_internal"] is True for g in groups)


def test_api_groups_includes_branch_groups(branch_client):
    """Groups added in the branch session should appear in the groups list."""
    branch_client.post("/api/group", json={"name": "Partner Org", "is_internal": False})
    resp = branch_client.get("/api/groups")
    names = [g["name"] for g in resp.json]
    assert "Partner Org" in names


def test_api_group_details_missing_param(main_client):
    resp = main_client.get("/api/group-details")
    assert resp.status_code == 400


def test_api_group_details_structure(main_client):
    resp = main_client.get("/api/group-details?group=Engineering")
    assert resp.status_code == 200
    data = resp.json
    assert "months" in data
    assert "people" in data
    assert "projects" in data


def test_api_group_details_people_names(main_client):
    data = main_client.get("/api/group-details?group=Engineering").json
    names = [r["name"] for r in data["people"]]
    assert "Smith,Jane" in names
    assert "Total" in names
    assert names[-1] == "Total"


def test_api_group_details_people_np_percentage(main_client):
    # Smith,Jane has no Non-Project effort, so all months should be 0%
    data = main_client.get("/api/group-details?group=Engineering").json
    jane = next(r for r in data["people"] if r["name"] == "Smith,Jane")
    for month in data["months"]:
        assert jane[month] == 0.0


def test_api_group_details_ops_person_np(main_client):
    # Jones,Bob is 100% Non-Project
    data = main_client.get("/api/group-details?group=Ops").json
    bob = next(r for r in data["people"] if r["name"] == "Jones,Bob")
    for month in data["months"]:
        assert abs(bob[month] - 100.0) < 0.1


def test_api_group_details_projects(main_client):
    data = main_client.get("/api/group-details?group=Engineering").json
    names = [r["project_name"] for r in data["projects"]]
    assert "Widget Project" in names
    assert "Gadget Project" in names


def test_api_group_details_project_effort(main_client):
    # Smith,Jane: 50% on Widget Project in July — that's the only Engineering member, so total = 50%
    data = main_client.get("/api/group-details?group=Engineering").json
    proj = next(r for r in data["projects"] if r["project_name"] == "Widget Project")
    assert abs(proj["July 2025"] - 50.0) < 0.1


def test_api_group_details_total_row_matches_group_np(main_client):
    # Engineering has 0% NP, so the Total row should also be 0%
    data = main_client.get("/api/group-details?group=Engineering").json
    total = next(r for r in data["people"] if r["name"] == "Total")
    for month in data["months"]:
        assert total[month] == 0.0


def test_api_group_details_no_children_for_single_budget_line(main_client):
    # Each project in test data has only one budget line, so no _children
    data = main_client.get("/api/group-details?group=Engineering").json
    for proj in data["projects"]:
        assert "_children" not in proj


def test_api_group_details_children_for_multi_budget_line(tmp_path):
    # Create data where one project has two budget lines
    tsv = tmp_path / "multi_anon.tsv"
    db = tmp_path / "multi_anon.db"
    _write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "30.00%", "40.00%"],
        ["", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120003", "Widget Project",
         "20.00%", "10.00%"],
        ["", "Engineering", "25210", "49000", "511120",
         "", "", "", "", "5120002", "Gadget Project",
         "50.00%", "50.00%"],
    ])
    load_tsv(tsv, db)
    # Merge the two Widget budget lines under one project
    import sqlite3
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    widget_proj = conn.execute(
        "SELECT id FROM projects WHERE name = 'Widget Project' LIMIT 1"
    ).fetchone()["id"]
    conn.execute(
        "UPDATE budget_lines SET project_id = ? WHERE budget_line_code IN ('5120001', '5120003')",
        (widget_proj,),
    )
    conn.commit()
    conn.close()

    app = create_app(db_path=db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        data = c.get("/api/group-details?group=Engineering").json

    widget = next(r for r in data["projects"] if r["project_name"] == "Widget Project")
    # Project-level totals are sum of budget lines
    assert abs(widget["July 2025"] - 50.0) < 0.1
    assert abs(widget["August 2025"] - 50.0) < 0.1

    # Should have _children with the two budget lines
    assert "_children" in widget
    assert len(widget["_children"]) == 2
    child_names = [ch["project_name"] for ch in widget["_children"]]
    assert len(child_names) == 2

    # Each child should have month data
    for child in widget["_children"]:
        assert "July 2025" in child
        assert "August 2025" in child

    # Gadget has only one budget line — no _children
    gadget = next(r for r in data["projects"] if r["project_name"] == "Gadget Project")
    assert "_children" not in gadget


# --- Non-Project by Person report tests ---

def test_report_nonproject_by_person_page_returns_200(main_client):
    resp = main_client.get("/reports/nonproject-by-person")
    assert resp.status_code == 200


def test_api_nonproject_by_person_structure(main_client):
    resp = main_client.get("/api/nonproject-by-person")
    assert resp.status_code == 200
    data = resp.json
    assert "months" in data
    assert "rows" in data
    assert isinstance(data["months"], list)
    assert isinstance(data["rows"], list)


def test_api_nonproject_by_person_contains_people(main_client):
    data = main_client.get("/api/nonproject-by-person").json
    names = [r["name"] for r in data["rows"]]
    assert "Smith,Jane" in names
    assert "Jones,Bob" in names


def test_api_nonproject_by_person_includes_group(main_client):
    data = main_client.get("/api/nonproject-by-person").json
    jane = next(r for r in data["rows"] if r["name"] == "Smith,Jane")
    assert jane["group"] == "Engineering"
    bob = next(r for r in data["rows"] if r["name"] == "Jones,Bob")
    assert bob["group"] == "Ops"


def test_api_nonproject_by_person_bob_is_100pct(main_client):
    # Jones,Bob (Ops) is 100% Non-Project every month
    data = main_client.get("/api/nonproject-by-person").json
    bob = next(r for r in data["rows"] if r["name"] == "Jones,Bob")
    for month in data["months"]:
        assert abs(bob[month] - 100.0) < 0.1, f"Jones,Bob {month}: expected 100%, got {bob[month]}"


def test_api_nonproject_by_person_jane_is_0pct(main_client):
    # Smith,Jane (Engineering) has no Non-Project effort
    data = main_client.get("/api/nonproject-by-person").json
    jane = next(r for r in data["rows"] if r["name"] == "Smith,Jane")
    for month in data["months"]:
        assert jane[month] == 0.0, f"Smith,Jane {month}: expected 0%, got {jane[month]}"


def test_api_nonproject_by_person_has_total_row(main_client):
    data = main_client.get("/api/nonproject-by-person").json
    assert data["rows"][-1]["name"] == "Total"


def test_api_nonproject_by_person_total_correct(main_client):
    # Jones,Bob: 100% NP; Smith,Jane: 0% NP; combined = 50% NP
    data = main_client.get("/api/nonproject-by-person").json
    total = data["rows"][-1]
    for month in data["months"]:
        assert abs(total[month] - 50.0) < 0.1, (
            f"Total {month}: expected 50%, got {total[month]}"
        )


# --- Project Details report tests ---

def test_report_project_details_page_returns_200(main_client):
    resp = main_client.get("/reports/project-details")
    assert resp.status_code == 200


def test_api_project_details_missing_param(main_client):
    resp = main_client.get("/api/project-details")
    assert resp.status_code == 400


def test_api_project_details_structure(main_client, loaded_db):
    pid = _project_id_for(loaded_db, "Widget Project")
    resp = main_client.get(f"/api/project-details?project_id={pid}")
    assert resp.status_code == 200
    data = resp.json
    assert "months" in data
    assert "fte_rows" in data
    assert "people" in data
    assert "budget_line_spending" in data


def test_api_project_details_months(main_client, loaded_db):
    pid = _project_id_for(loaded_db, "Widget Project")
    data = main_client.get(f"/api/project-details?project_id={pid}").json
    assert "July 2025" in data["months"]
    assert "August 2025" in data["months"]


def test_api_project_details_start_date_filters_months(tmp_path):
    """Months before the project start date are excluded from the display."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "0.00%", "60.00%"],
        ["Jones,Bob", "Ops", "", "", "",
         "", "", "", "", "N/A", "N/A",
         "100.00%", "100.00%"],
    ])
    load_tsv(tsv, db)
    from sej.queries import update_project
    pid = _project_id_for(db, "Widget Project")
    update_project(db, pid, start_year=2025, start_month=8)
    app = create_app(db_path=db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        data = c.get(f"/api/project-details?project_id={pid}").json
    assert "July 2025" not in data["months"]
    assert "August 2025" in data["months"]


def test_api_project_details_end_date_filters_months(tmp_path):
    """Months after the project end date are excluded from the display."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "50.00%", "0.00%"],
        ["Jones,Bob", "Ops", "", "", "",
         "", "", "", "", "N/A", "N/A",
         "100.00%", "100.00%"],
    ])
    load_tsv(tsv, db)
    from sej.queries import update_project
    pid = _project_id_for(db, "Widget Project")
    update_project(db, pid, end_year=2025, end_month=7)
    app = create_app(db_path=db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        data = c.get(f"/api/project-details?project_id={pid}").json
    assert "July 2025" in data["months"]
    assert "August 2025" not in data["months"]


def test_api_project_details_fte_label(main_client, loaded_db):
    pid = _project_id_for(loaded_db, "Widget Project")
    data = main_client.get(f"/api/project-details?project_id={pid}").json
    assert data["fte_rows"][0]["label"] == "Internal FTE"


def test_api_project_details_fte_values(main_client, loaded_db):
    # Smith,Jane: 50% on Widget Project in July → FTE = 0.50; 60% in August → FTE = 0.60
    pid = _project_id_for(loaded_db, "Widget Project")
    data = main_client.get(f"/api/project-details?project_id={pid}").json
    internal = data["fte_rows"][0]
    assert abs(internal["July 2025"] - 0.50) < 0.01
    assert abs(internal["August 2025"] - 0.60) < 0.01


def test_api_project_details_no_external_row_when_none(main_client, loaded_db):
    # No external employees in the base fixture → only one FTE row
    pid = _project_id_for(loaded_db, "Widget Project")
    data = main_client.get(f"/api/project-details?project_id={pid}").json
    assert len(data["fte_rows"]) == 1


def test_api_project_details_people(main_client, loaded_db):
    # Only Smith,Jane works on Widget Project
    pid = _project_id_for(loaded_db, "Widget Project")
    data = main_client.get(f"/api/project-details?project_id={pid}").json
    names = [r["name"] for r in data["people"]]
    assert "Smith,Jane" in names
    assert "Jones,Bob" not in names


def test_api_project_details_person_effort(main_client, loaded_db):
    # Smith,Jane: 50% in July, 60% in August on Widget Project
    pid = _project_id_for(loaded_db, "Widget Project")
    data = main_client.get(f"/api/project-details?project_id={pid}").json
    jane = next(r for r in data["people"] if r["name"] == "Smith,Jane")
    assert abs(jane["July 2025"] - 50.0) < 0.1
    assert abs(jane["August 2025"] - 60.0) < 0.1


def test_api_project_details_person_group(main_client, loaded_db):
    pid = _project_id_for(loaded_db, "Widget Project")
    data = main_client.get(f"/api/project-details?project_id={pid}").json
    jane = next(r for r in data["people"] if r["name"] == "Smith,Jane")
    assert jane["group"] == "Engineering"


def test_api_add_employee_forbidden_on_main(client):
    resp = client.post("/api/employee", json={
        "first_name": "Jane", "last_name": "Smith", "group_name": "Engineering",
    })
    assert resp.status_code == 403


def test_api_add_employee_missing_field(branch_client):
    resp = branch_client.post("/api/employee", json={
        "first_name": "Jane", "last_name": "Smith",
    })
    assert resp.status_code == 400


def test_api_add_employee_unknown_group(branch_client):
    resp = branch_client.post("/api/employee", json={
        "first_name": "Jane", "last_name": "Doe", "group_name": "NoSuchGroup",
    })
    assert resp.status_code == 400


def test_api_add_employee_success(branch_client, branch_db):
    resp = branch_client.post("/api/employee", json={
        "first_name": "Jane", "last_name": "Doe", "group_name": "Engineering",
    })
    assert resp.status_code == 200
    assert "employee_id" in resp.json

    from sej.db import get_connection
    conn = get_connection(branch_db)
    emp = conn.execute(
        "SELECT name FROM employees WHERE id = ?", (resp.json["employee_id"],)
    ).fetchone()
    conn.close()
    assert emp["name"] == "Doe,Jane"


def test_api_add_employee_with_middle_name(branch_client, branch_db):
    resp = branch_client.post("/api/employee", json={
        "first_name": "Jane", "last_name": "Doe",
        "middle_name": "Marie", "group_name": "Engineering",
    })
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    emp = conn.execute(
        "SELECT name FROM employees WHERE id = ?", (resp.json["employee_id"],)
    ).fetchone()
    conn.close()
    assert emp["name"] == "Doe,Jane Marie"


def test_api_add_employee_without_middle_name(branch_client, branch_db):
    resp = branch_client.post("/api/employee", json={
        "first_name": "Bob", "last_name": "Doe",
        "middle_name": "", "group_name": "Ops",
    })
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    emp = conn.execute(
        "SELECT name FROM employees WHERE id = ?", (resp.json["employee_id"],)
    ).fetchone()
    conn.close()
    assert emp["name"] == "Doe,Bob"


def test_api_add_employee_missing_json(branch_client):
    resp = branch_client.post("/api/employee", data="not json",
                              content_type="text/plain")
    assert resp.status_code == 400


def test_api_project_details_nonproject(main_client, loaded_db):
    # Jones,Bob is 100% Non-Project → FTE = 1.0 per month
    pid = _project_id_for(loaded_db, "Non-Project")
    data = main_client.get(f"/api/project-details?project_id={pid}").json
    internal = data["fte_rows"][0]
    assert abs(internal["July 2025"] - 1.0) < 0.01
    bob = next(r for r in data["people"] if r["name"] == "Jones,Bob")
    assert abs(bob["July 2025"] - 100.0) < 0.1


# --- Spending analysis tests ---

@pytest.fixture
def spending_client(branch_db):
    """Branch DB with budget on budget line 5120001 and end date on its project."""
    from sej.queries import update_budget_line, update_project
    # Smith,Jane: salary=120000, 50% Jul → $5000, 60% Aug → $6000
    update_budget_line(branch_db, "5120001",
                       personnel_budget=100000.0,
                       end_year=2025, end_month=9)
    pid = _project_id_for(branch_db, "Widget Project")
    update_project(branch_db, pid, end_year=2025, end_month=9)
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


@pytest.fixture
def spending_project_id(branch_db):
    """Return the Widget Project id from the branch DB."""
    return _project_id_for(branch_db, "Widget Project")


def test_spending_analysis_present(spending_client, spending_project_id):
    data = spending_client.get(f"/api/project-details?project_id={spending_project_id}").json
    assert data["spending_analysis"] is not None
    assert len(data["spending_analysis"]) > 0


def test_spending_analysis_absent_without_budget(main_client, loaded_db):
    pid = _project_id_for(loaded_db, "Widget Project")
    data = main_client.get(f"/api/project-details?project_id={pid}").json
    assert data["spending_analysis"] is None


def test_spending_analysis_values(spending_client, spending_project_id):
    # Jul: 120000/12 * 0.50 = 5000 → remaining 95000
    # Aug: 120000/12 * 0.60 = 6000 → remaining 89000
    # Sep: extrapolated at 6000  → remaining 83000
    data = spending_client.get(f"/api/project-details?project_id={spending_project_id}").json
    points = {p["month"]: p["remaining"] for p in data["spending_analysis"]}
    assert abs(points["July 2025"] - 95000.0) < 1.0
    assert abs(points["August 2025"] - 89000.0) < 1.0
    assert abs(points["September 2025"] - 83000.0) < 1.0


def test_spending_analysis_covers_to_end(spending_client, spending_project_id):
    data = spending_client.get(f"/api/project-details?project_id={spending_project_id}").json
    months = [p["month"] for p in data["spending_analysis"]]
    assert months[-1] == "September 2025"


def test_spending_analysis_start_date_respected(branch_db):
    """Chart starts at project start date with $0 spend for pre-effort months."""
    from sej.queries import update_budget_line, update_project
    pid = _project_id_for(branch_db, "Widget Project")
    # Start in May 2025 — two months before the first effort (July 2025)
    update_budget_line(branch_db, "5120001",
                       personnel_budget=100000.0,
                       end_year=2025, end_month=8)
    update_project(branch_db, pid, start_year=2025, start_month=5,
                   end_year=2025, end_month=8)
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        data = c.get(f"/api/project-details?project_id={pid}").json
    points = {p["month"]: p["remaining"] for p in data["spending_analysis"]}
    # May and June have no effort → $0 spend → budget stays at 100000
    assert abs(points["May 2025"] - 100000.0) < 1.0
    assert abs(points["June 2025"] - 100000.0) < 1.0
    # July: 5000 spent → 95000
    assert abs(points["July 2025"] - 95000.0) < 1.0


def test_spending_analysis_zero_spend_within_range(branch_db):
    """Months within the data range with no effort for this project contribute $0."""
    from sej.queries import update_budget_line, update_project
    pid = _project_id_for(branch_db, "Gadget Project")
    update_budget_line(branch_db, "5120002",
                       personnel_budget=80000.0,
                       end_year=2025, end_month=9)
    update_project(branch_db, pid, end_year=2025, end_month=9)
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        data = c.get(f"/api/project-details?project_id={pid}").json
    points = {p["month"]: p["remaining"] for p in data["spending_analysis"]}
    assert abs(points["July 2025"] - 75000.0) < 1.0   # 80000 - 5000
    assert abs(points["August 2025"] - 71000.0) < 1.0  # 75000 - 4000
    assert abs(points["September 2025"] - 67000.0) < 1.0  # extrapolate Aug rate


def test_budget_line_spending_in_project_details(branch_db):
    """Per-budget-line spending is returned alongside project-level spending."""
    from sej.queries import update_budget_line, update_project
    pid = _project_id_for(branch_db, "Widget Project")
    update_budget_line(branch_db, "5120001",
                       personnel_budget=100000.0,
                       end_year=2025, end_month=9)
    update_project(branch_db, pid, end_year=2025, end_month=9)
    app = create_app(db_path=branch_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        data = c.get(f"/api/project-details?project_id={pid}").json
    assert "budget_line_spending" in data
    assert len(data["budget_line_spending"]) >= 1
    bl = data["budget_line_spending"][0]
    assert "name" in bl
    assert bl["spending_analysis"] is not None
    assert len(bl["spending_analysis"]) > 0


# --- Add group tests ---

def test_api_add_group_forbidden_on_main(client):
    resp = client.post("/api/group", json={"name": "New Group"})
    assert resp.status_code == 403


def test_api_add_group_missing_name(branch_client):
    resp = branch_client.post("/api/group", json={"is_internal": True})
    assert resp.status_code == 400


def test_api_add_group_missing_json(branch_client):
    resp = branch_client.post("/api/group", data="not json", content_type="text/plain")
    assert resp.status_code == 400


def test_api_add_group_internal(branch_client, branch_db):
    resp = branch_client.post("/api/group", json={"name": "New Internal Group", "is_internal": True})
    assert resp.status_code == 200
    assert "group_id" in resp.json

    from sej.db import get_connection
    conn = get_connection(branch_db)
    row = conn.execute("SELECT is_internal FROM groups WHERE name = 'New Internal Group'").fetchone()
    conn.close()
    assert row["is_internal"] == 1


def test_api_add_group_external(branch_client, branch_db):
    resp = branch_client.post("/api/group", json={"name": "Partner Org", "is_internal": False})
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    row = conn.execute("SELECT is_internal FROM groups WHERE name = 'Partner Org'").fetchone()
    conn.close()
    assert row["is_internal"] == 0


def test_api_add_group_duplicate_name(branch_client):
    resp = branch_client.post("/api/group", json={"name": "Engineering"})
    assert resp.status_code == 400
    assert "already exists" in resp.json["error"]


def test_api_add_group_defaults_to_internal(branch_client, branch_db):
    resp = branch_client.post("/api/group", json={"name": "Default Group"})
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    row = conn.execute("SELECT is_internal FROM groups WHERE name = 'Default Group'").fetchone()
    conn.close()
    assert row["is_internal"] == 1


# --- External group exclusion from reports ---

@pytest.fixture
def external_group_db(tmp_path):
    """DB with one internal employee and one external employee."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "100.00%", ""],
        ["External,Person", "PartnerOrg", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "50.00%", ""],
    ])
    load_tsv(tsv, db)
    from sej.db import get_connection
    conn = get_connection(db)
    conn.execute("UPDATE groups SET is_internal = 0 WHERE name = 'PartnerOrg'")
    conn.commit()
    conn.close()
    return db


@pytest.fixture
def external_group_client(external_group_db):
    app = create_app(db_path=external_group_db)
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_nonproject_by_group_excludes_external(external_group_client):
    data = external_group_client.get("/api/nonproject-by-group").json
    groups = [r["group"] for r in data["rows"]]
    assert "PartnerOrg" not in groups
    assert "Engineering" in groups


def test_nonproject_by_group_total_excludes_external(external_group_client):
    # Only Smith,Jane (Engineering, 0% NP) counts — total should be 0%
    data = external_group_client.get("/api/nonproject-by-group").json
    total_row = data["rows"][-1]
    assert total_row["group"] == "Total"
    for month in data["months"]:
        assert total_row[month] == 0.0


def test_nonproject_by_person_excludes_external(external_group_client):
    data = external_group_client.get("/api/nonproject-by-person").json
    names = [r["name"] for r in data["rows"]]
    assert "External,Person" not in names
    assert "Smith,Jane" in names


def test_nonproject_by_person_total_excludes_external(external_group_client):
    # Only Smith,Jane (0% NP) counts — total should be 0%
    data = external_group_client.get("/api/nonproject-by-person").json
    total_row = data["rows"][-1]
    assert total_row["name"] == "Total"
    for month in data["months"]:
        assert total_row[month] == 0.0


# --- Project details external FTE row ---

def test_api_project_details_external_fte_row_present(external_group_client, external_group_db):
    # external_group_db: Smith,Jane (internal, 100%) and External,Person (external, 50%)
    # both on Widget Project in July 2025
    pid = _project_id_for(external_group_db, "Widget Project")
    data = external_group_client.get(f"/api/project-details?project_id={pid}").json
    labels = [r["label"] for r in data["fte_rows"]]
    assert "Internal FTE" in labels
    assert "External FTE" in labels


def test_api_project_details_external_fte_values(external_group_client, external_group_db):
    pid = _project_id_for(external_group_db, "Widget Project")
    data = external_group_client.get(f"/api/project-details?project_id={pid}").json
    internal = next(r for r in data["fte_rows"] if r["label"] == "Internal FTE")
    external = next(r for r in data["fte_rows"] if r["label"] == "External FTE")
    assert abs(internal["July 2025"] - 1.0) < 0.01
    assert abs(external["July 2025"] - 0.50) < 0.01


def test_api_project_details_external_fte_row_absent_when_no_external(main_client, loaded_db):
    # Base fixture has no external employees
    pid = _project_id_for(loaded_db, "Widget Project")
    data = main_client.get(f"/api/project-details?project_id={pid}").json
    labels = [r["label"] for r in data["fte_rows"]]
    assert "External FTE" not in labels


# --- Employee start/end date tests ---

def test_get_employees_includes_date_fields(client):
    """GET /api/employees includes start/end date fields."""
    resp = client.get("/api/employees")
    assert resp.status_code == 200
    for emp in resp.json:
        assert "start_year" in emp
        assert "start_month" in emp
        assert "end_year" in emp
        assert "end_month" in emp


def test_api_update_employee_forbidden_on_main(client):
    """PUT /api/employee returns 403 on main DB."""
    resp = client.put("/api/employee", json={"employee_id": 1})
    assert resp.status_code == 403


def test_api_update_employee_sets_dates(branch_client, branch_db):
    """PUT /api/employee sets start/end date fields."""
    employees = branch_client.get("/api/employees").json
    emp_id = next(e["id"] for e in employees if e["name"] == "Smith,Jane")

    resp = branch_client.put("/api/employee", json={
        "employee_id": emp_id,
        "start_year": 2025,
        "start_month": 7,
        "end_year": 2025,
        "end_month": 8,
    })
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    emp = conn.execute("SELECT * FROM employees WHERE id = ?", (emp_id,)).fetchone()
    conn.close()
    assert emp["start_year"] == 2025
    assert emp["start_month"] == 7
    assert emp["end_year"] == 2025
    assert emp["end_month"] == 8


def test_api_update_employee_clears_dates(branch_client, branch_db):
    """Sending no date fields clears previously set dates."""
    employees = branch_client.get("/api/employees").json
    emp_id = next(e["id"] for e in employees if e["name"] == "Smith,Jane")

    # First set dates (Jul–Aug 2025 encompasses all existing effort)
    branch_client.put("/api/employee", json={
        "employee_id": emp_id,
        "start_year": 2025,
        "start_month": 7,
        "end_year": 2025,
        "end_month": 8,
    })

    # Clear by omitting date fields
    resp = branch_client.put("/api/employee", json={"employee_id": emp_id})
    assert resp.status_code == 200

    from sej.db import get_connection
    conn = get_connection(branch_db)
    emp = conn.execute("SELECT * FROM employees WHERE id = ?", (emp_id,)).fetchone()
    conn.close()
    assert emp["start_year"] is None
    assert emp["start_month"] is None
    assert emp["end_year"] is None
    assert emp["end_month"] is None


def test_api_update_employee_rejects_start_without_month(branch_client):
    """start_year without start_month is rejected."""
    employees = branch_client.get("/api/employees").json
    emp_id = employees[0]["id"]

    resp = branch_client.put("/api/employee", json={
        "employee_id": emp_id,
        "start_year": 2025,
    })
    assert resp.status_code == 400
    assert "start_year" in resp.json["error"]


def test_api_update_employee_rejects_conflicting_start(branch_client):
    """Setting start after existing effort is rejected."""
    employees = branch_client.get("/api/employees").json
    emp_id = next(e["id"] for e in employees if e["name"] == "Smith,Jane")

    # Smith,Jane has effort in Jul 2025; start=Aug 2025 would conflict
    resp = branch_client.put("/api/employee", json={
        "employee_id": emp_id,
        "start_year": 2025,
        "start_month": 8,
    })
    assert resp.status_code == 400
    assert "Jul 2025" in resp.json["error"]


def test_api_update_employee_rejects_conflicting_end(branch_client):
    """Setting end before existing effort is rejected."""
    employees = branch_client.get("/api/employees").json
    emp_id = next(e["id"] for e in employees if e["name"] == "Smith,Jane")

    # Smith,Jane has effort in Aug 2025; end=Jul 2025 would conflict
    resp = branch_client.put("/api/employee", json={
        "employee_id": emp_id,
        "end_year": 2025,
        "end_month": 7,
    })
    assert resp.status_code == 400
    assert "Aug 2025" in resp.json["error"]


# --- fix_totals respects employee date bounds ---

def test_fix_totals_skips_months_before_employee_start(tmp_path):
    """Employee with start=Aug 2025; July shortfall is left alone by fix_totals."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        # Smith,Jane: 90% in July (shortfall), 100% in August
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "90.00%", "100.00%"],
        # Jones,Bob: always 100%
        ["Jones,Bob", "Ops", "", "", "",
         "", "", "", "", "N/A", "N/A",
         "100.00%", "100.00%"],
    ])
    load_tsv(tsv, db)

    # Set Smith,Jane start = Aug 2025 directly (bypassing conflict check since
    # we want to test that fix_totals skips, not that update_employee validates)
    from sej.db import get_connection
    conn = get_connection(db)
    conn.execute(
        "UPDATE employees SET start_year=2025, start_month=8 WHERE name='Smith,Jane'"
    )
    conn.commit()
    conn.close()

    branch_db = create_branch(db, "test_fix_start")
    app_inst = create_app(db_path=branch_db)
    app_inst.config["TESTING"] = True
    with app_inst.test_client() as c:
        resp = c.post("/api/fix-totals")
    assert resp.status_code == 200
    # Smith,Jane's July shortfall should NOT be fixed (not active in July)
    assert resp.json["changes"] == [], f"Expected no changes, got {resp.json['changes']}"

    from sej.db import get_connection
    conn = get_connection(branch_db)
    al = conn.execute("""
        SELECT al.id FROM allocation_lines al
        JOIN employees e ON e.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE e.name = 'Smith,Jane' AND bl.budget_line_code = '5120001'
    """).fetchone()
    effort = conn.execute(
        "SELECT percentage FROM efforts WHERE allocation_line_id = ? AND year=2025 AND month=7",
        (al["id"],),
    ).fetchone()
    conn.close()
    assert abs(effort["percentage"] - 90.0) < 0.01


def test_fix_totals_skips_months_after_employee_end(tmp_path):
    """Employee with end=Jul 2025; August shortfall is left alone by fix_totals."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    _write_tsv(tsv, [
        # Smith,Jane: 100% in July, 90% in August (shortfall)
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "100.00%", "90.00%"],
        # Jones,Bob: always 100%
        ["Jones,Bob", "Ops", "", "", "",
         "", "", "", "", "N/A", "N/A",
         "100.00%", "100.00%"],
    ])
    load_tsv(tsv, db)

    from sej.db import get_connection
    conn = get_connection(db)
    conn.execute(
        "UPDATE employees SET end_year=2025, end_month=7 WHERE name='Smith,Jane'"
    )
    conn.commit()
    conn.close()

    branch_db = create_branch(db, "test_fix_end")
    app_inst = create_app(db_path=branch_db)
    app_inst.config["TESTING"] = True
    with app_inst.test_client() as c:
        resp = c.post("/api/fix-totals")
    assert resp.status_code == 200
    # Smith,Jane's August shortfall should NOT be fixed (not active in August)
    assert resp.json["changes"] == [], f"Expected no changes, got {resp.json['changes']}"

    from sej.db import get_connection
    conn = get_connection(branch_db)
    al = conn.execute("""
        SELECT al.id FROM allocation_lines al
        JOIN employees e ON e.id = al.employee_id
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        WHERE e.name = 'Smith,Jane' AND bl.budget_line_code = '5120001'
    """).fetchone()
    effort = conn.execute(
        "SELECT percentage FROM efforts WHERE allocation_line_id = ? AND year=2025 AND month=8",
        (al["id"],),
    ).fetchone()
    conn.close()
    assert abs(effort["percentage"] - 90.0) < 0.01


# --- update_effort respects employee date bounds ---

def test_update_effort_rejects_before_employee_start(branch_db):
    """update_effort raises ValueError when month is before employee start."""
    from sej.queries import update_effort
    from sej.db import get_connection

    conn = get_connection(branch_db)
    emp = conn.execute(
        "SELECT id FROM employees WHERE name = 'Smith,Jane'"
    ).fetchone()
    conn.execute(
        "UPDATE employees SET start_year=2025, start_month=8 WHERE id=?", (emp["id"],)
    )
    al = conn.execute(
        "SELECT id FROM allocation_lines WHERE employee_id = ? LIMIT 1", (emp["id"],)
    ).fetchone()
    conn.commit()
    conn.close()

    with pytest.raises(ValueError, match="before"):
        update_effort(branch_db, al["id"], 2025, 7, 50.0)


def test_update_effort_rejects_after_employee_end(branch_db):
    """update_effort raises ValueError when month is after employee end."""
    from sej.queries import update_effort
    from sej.db import get_connection

    conn = get_connection(branch_db)
    emp = conn.execute(
        "SELECT id FROM employees WHERE name = 'Smith,Jane'"
    ).fetchone()
    conn.execute(
        "UPDATE employees SET end_year=2025, end_month=7 WHERE id=?", (emp["id"],)
    )
    al = conn.execute(
        "SELECT id FROM allocation_lines WHERE employee_id = ? LIMIT 1", (emp["id"],)
    ).fetchone()
    conn.commit()
    conn.close()

    with pytest.raises(ValueError, match="after"):
        update_effort(branch_db, al["id"], 2025, 8, 50.0)


def test_update_effort_allows_deletion_outside_range(branch_db):
    """Deleting effort (percentage=None) is allowed even outside employee's range."""
    from sej.queries import update_effort
    from sej.db import get_connection

    conn = get_connection(branch_db)
    emp = conn.execute(
        "SELECT id FROM employees WHERE name = 'Smith,Jane'"
    ).fetchone()
    conn.execute(
        "UPDATE employees SET start_year=2025, start_month=8 WHERE id=?", (emp["id"],)
    )
    al = conn.execute(
        "SELECT id FROM allocation_lines WHERE employee_id = ? LIMIT 1", (emp["id"],)
    ).fetchone()
    conn.commit()
    conn.close()

    # Deleting July effort should not raise even though July < Aug 2025 start
    update_effort(branch_db, al["id"], 2025, 7, None)


# --- Importer preserves employee dates across reload ---

def test_importer_preserves_employee_dates_across_reload(tmp_path):
    """Employee start/end dates survive a TSV reload."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    # Only August effort so start=Aug 2025 won't conflict
    _write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "", "50.00%"],
        ["Jones,Bob", "Ops", "", "", "",
         "", "", "", "", "N/A", "N/A",
         "", "100.00%"],
    ])
    load_tsv(tsv, db)

    from sej.db import get_connection
    conn = get_connection(db)
    conn.execute(
        "UPDATE employees SET start_year=2025, start_month=8 WHERE name='Smith,Jane'"
    )
    conn.commit()
    conn.close()

    # Reload the same TSV — dates should be preserved
    load_tsv(tsv, db)

    conn = get_connection(db)
    emp = conn.execute(
        "SELECT start_year, start_month FROM employees WHERE name='Smith,Jane'"
    ).fetchone()
    conn.close()
    assert emp["start_year"] == 2025
    assert emp["start_month"] == 8


def test_importer_raises_on_effort_outside_employee_start(tmp_path):
    """Loading TSV with effort before employee start raises ValueError."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    # TSV has July effort
    _write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "50.00%", ""],
    ])
    load_tsv(tsv, db)

    # Set start=Aug 2025 directly
    from sej.db import get_connection
    conn = get_connection(db)
    conn.execute(
        "UPDATE employees SET start_year=2025, start_month=8 WHERE name='Smith,Jane'"
    )
    conn.commit()
    conn.close()

    # Reload should fail: July effort is before Aug 2025 start
    with pytest.raises(ValueError, match="before"):
        load_tsv(tsv, db)


def test_importer_raises_on_effort_outside_employee_end(tmp_path):
    """Loading TSV with effort after employee end raises ValueError."""
    tsv = tmp_path / "data_anon.tsv"
    db = tmp_path / "test_anon.db"
    # TSV has August effort
    _write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project",
         "", "50.00%"],
    ])
    load_tsv(tsv, db)

    # Set end=Jul 2025 directly
    from sej.db import get_connection
    conn = get_connection(db)
    conn.execute(
        "UPDATE employees SET end_year=2025, end_month=7 WHERE name='Smith,Jane'"
    )
    conn.commit()
    conn.close()

    # Reload should fail: August effort is after Jul 2025 end
    with pytest.raises(ValueError, match="after"):
        load_tsv(tsv, db)


# --- Project change history tests ---

def test_api_project_change_history_missing_param(main_client):
    resp = main_client.get("/api/project-change-history")
    assert resp.status_code == 400


def test_api_project_change_history_no_merges(main_client, loaded_db):
    """A project with no merges returns an empty list."""
    pid = _project_id_for(loaded_db, "Widget Project")
    resp = main_client.get(f"/api/project-change-history?project_id={pid}")
    assert resp.status_code == 200
    assert resp.json == []


def test_api_project_change_history_with_merge(main_client, loaded_db):
    """After a merge with changes, the change history endpoint returns them."""
    pid = _project_id_for(loaded_db, "Widget Project")
    # Create branch, make a change on budget line 5120001, merge
    main_client.post("/api/branch/create")
    payload = main_client.get("/api/data").json
    # Find a line for budget line 5120001
    line = next(r for r in payload["data"]
                if r["Budget Line Code"] == "5120001")
    line_id = line["allocation_line_id"]

    main_client.put("/api/effort", json={
        "allocation_line_id": line_id, "year": 2025, "month": 7, "percentage": 42.0,
    })
    main_client.post("/api/branch/merge")

    resp = main_client.get(f"/api/project-change-history?project_id={pid}")
    assert resp.status_code == 200
    groups = resp.json
    assert len(groups) >= 1
    group = groups[0]
    assert "timestamp" in group
    assert "branch_name" in group
    assert len(group["changes"]) >= 1
    change = group["changes"][0]
    assert change["year"] == "2025"
    assert change["month"] == "7"
    assert "employee" in change
    assert "type" in change


def test_api_project_change_history_filters_by_project(main_client, loaded_db):
    """Change history only returns changes for the requested project."""
    pid_widget = _project_id_for(loaded_db, "Widget Project")
    pid_gadget = _project_id_for(loaded_db, "Gadget Project")
    # Create branch, change budget line 5120001 (Widget Project), merge
    main_client.post("/api/branch/create")
    payload = main_client.get("/api/data").json
    line = next(r for r in payload["data"]
                if r["Budget Line Code"] == "5120001")
    main_client.put("/api/effort", json={
        "allocation_line_id": line["allocation_line_id"],
        "year": 2025, "month": 7, "percentage": 42.0,
    })
    main_client.post("/api/branch/merge")

    # Gadget Project should have no changes from this merge
    resp = main_client.get(f"/api/project-change-history?project_id={pid_gadget}")
    assert resp.status_code == 200
    assert resp.json == []


def test_budget_lines_page_returns_200(client):
    resp = client.get("/budget-lines")
    assert resp.status_code == 200


def test_budget_lines_page_returns_200_on_main(main_client):
    resp = main_client.get("/budget-lines")
    assert resp.status_code == 200


def test_api_update_budget_line_project_reassignment(branch_client, branch_db):
    """Budget line can be reassigned to a different project."""
    from sej.queries import get_budget_lines, get_projects

    # Get current state
    projects = get_projects(branch_db)
    budget_lines = get_budget_lines(branch_db)
    bl = next(b for b in budget_lines if b["budget_line_code"] == "5120001")
    original_project_id = bl["project_id"]

    # Find a different project to reassign to
    other_project = next(p for p in projects if p["id"] != original_project_id)

    resp = branch_client.put("/api/budget-line", json={
        "budget_line_code": "5120001",
        "display_name": bl["display_name"],
        "start_year": bl["start_year"],
        "start_month": bl["start_month"],
        "end_year": bl["end_year"],
        "end_month": bl["end_month"],
        "personnel_budget": bl["personnel_budget"],
        "project_id": other_project["id"],
    })
    assert resp.status_code == 200

    # Verify the reassignment persisted
    updated = get_budget_lines(branch_db)
    updated_bl = next(b for b in updated if b["budget_line_code"] == "5120001")
    assert updated_bl["project_id"] == other_project["id"]


def test_api_update_budget_line_invalid_project_id(branch_client):
    """Reassigning to a nonexistent project returns 400."""
    resp = branch_client.put("/api/budget-line", json={
        "budget_line_code": "5120001",
        "project_id": 99999,
    })
    assert resp.status_code == 400
    assert "Project not found" in resp.json["error"]


def test_api_update_budget_line_without_project_id_preserves_project(branch_client, branch_db):
    """Updating without project_id does not change the project assignment."""
    from sej.queries import get_budget_lines

    budget_lines = get_budget_lines(branch_db)
    bl = next(b for b in budget_lines if b["budget_line_code"] == "5120001")
    original_project_id = bl["project_id"]

    resp = branch_client.put("/api/budget-line", json={
        "budget_line_code": "5120001",
        "display_name": "Updated Display Name",
    })
    assert resp.status_code == 200

    updated = get_budget_lines(branch_db)
    updated_bl = next(b for b in updated if b["budget_line_code"] == "5120001")
    assert updated_bl["project_id"] == original_project_id
    assert updated_bl["display_name"] == "Updated Display Name"
