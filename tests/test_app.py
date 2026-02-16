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
        ["Jones,Bob", "Ops", "20152", "12001", "512120",
         "", "", "", "VROPS", "N/A", "N/A",
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
    for header in ["Employee", "Group", "Fund Code", "Project Id",
                   "Project Name", "July 2025", "August 2025"]:
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
    project_ids = [row["Project Id"] for row in payload["data"]]
    assert "Non-Project" in project_ids


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


def test_api_projects(client):
    resp = client.get("/api/projects")
    assert resp.status_code == 200
    codes = [p["project_code"] for p in resp.json]
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
        "employee_name": "Smith,Jane", "project_code": "NEW001",
    })
    assert resp.status_code == 403


def test_api_allocation_line_create(branch_client):
    resp = branch_client.post("/api/allocation_line", json={
        "employee_name": "Smith,Jane",
        "project_code": "NEW001",
        "project_name": "New Project",
    })
    assert resp.status_code == 200
    assert "allocation_line_id" in resp.json


def test_api_allocation_line_new_project(branch_client, branch_db):
    branch_client.post("/api/allocation_line", json={
        "employee_name": "Jones,Bob",
        "project_code": "BRAND_NEW",
        "project_name": "Brand New Project",
    })

    from sej.db import get_connection
    conn = get_connection(branch_db)
    proj = conn.execute(
        "SELECT name FROM projects WHERE project_code = 'BRAND_NEW'"
    ).fetchone()
    conn.close()
    assert proj["name"] == "Brand New Project"
