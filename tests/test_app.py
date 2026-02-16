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
        "project_code": "5120001",
    })
    assert resp.status_code == 200
    assert "allocation_line_id" in resp.json


def test_api_allocation_line_unknown_project(branch_client):
    resp = branch_client.post("/api/allocation_line", json={
        "employee_name": "Smith,Jane",
        "project_code": "NONEXISTENT",
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
    code = resp.json["project_code"]

    from sej.db import get_connection
    conn = get_connection(branch_db)
    proj = conn.execute(
        "SELECT name FROM projects WHERE project_code = ?", (code,)
    ).fetchone()
    conn.close()
    assert proj["name"] == "Brand New Project"


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
        ["Jones,Bob", "Ops", "20152", "12001", "512120",
         "", "", "", "VROPS", "N/A", "N/A",
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
        JOIN projects p ON p.id = al.project_id
        WHERE e.name = 'Smith,Jane' AND p.project_code = 'Non-Project'
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
        JOIN projects p ON p.id = al.project_id
        WHERE p.project_code = 'Non-Project' AND al.fund_code = '27152'
    """).fetchone()
    effort_27152 = conn.execute(
        "SELECT percentage FROM efforts WHERE allocation_line_id = ? AND year = 2025 AND month = 7",
        (line_27152["id"],),
    ).fetchone()
    assert abs(effort_27152["percentage"] - 30.0) < 0.01

    line_20152 = conn.execute("""
        SELECT al.id FROM allocation_lines al
        JOIN projects p ON p.id = al.project_id
        WHERE p.project_code = 'Non-Project' AND al.fund_code = '20152'
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
        ["Jones,Bob", "Ops", "20152", "12001", "512120",
         "", "", "", "VROPS", "N/A", "N/A",
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
        JOIN projects p ON p.id = al.project_id
        WHERE e.name = 'Smith,Jane' AND p.project_code = 'Non-Project'
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
        JOIN projects p ON p.id = al.project_id
        WHERE p.project_code = 'Non-Project' AND al.fund_code = '27152'
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
        JOIN projects p ON p.id = al.project_id
        WHERE emp.name = 'Baker,Jeremy Boyd'
          AND p.project_code = 'Non-Project'
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
        JOIN projects p ON p.id = al.project_id
        WHERE e.name = 'Smith,Jane' AND p.project_code = 'Non-Project'
    """).fetchone()
    conn.close()
    assert np_line is None, "No Non-Project line should be created when total > 100"


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
    assert "Engineering" in groups
    assert "Ops" in groups


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
    codes = [r["project_code"] for r in data["projects"]]
    assert "5120001" in codes
    assert "5120002" in codes


def test_api_group_details_project_effort(main_client):
    # Smith,Jane: 50% on 5120001 in July — that's the only Engineering member, so total = 50%
    data = main_client.get("/api/group-details?group=Engineering").json
    proj = next(r for r in data["projects"] if r["project_code"] == "5120001")
    assert abs(proj["July 2025"] - 50.0) < 0.1


def test_api_group_details_total_row_matches_group_np(main_client):
    # Engineering has 0% NP, so the Total row should also be 0%
    data = main_client.get("/api/group-details?group=Engineering").json
    total = next(r for r in data["people"] if r["name"] == "Total")
    for month in data["months"]:
        assert total[month] == 0.0


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


def test_api_project_details_structure(main_client):
    resp = main_client.get("/api/project-details?project=5120001")
    assert resp.status_code == 200
    data = resp.json
    assert "months" in data
    assert "fte" in data
    assert "people" in data


def test_api_project_details_months(main_client):
    data = main_client.get("/api/project-details?project=5120001").json
    assert "July 2025" in data["months"]
    assert "August 2025" in data["months"]


def test_api_project_details_fte_label(main_client):
    data = main_client.get("/api/project-details?project=5120001").json
    assert data["fte"]["label"] == "Total FTE"


def test_api_project_details_fte_values(main_client):
    # Smith,Jane: 50% on 5120001 in July → FTE = 0.50; 60% in August → FTE = 0.60
    data = main_client.get("/api/project-details?project=5120001").json
    assert abs(data["fte"]["July 2025"] - 0.50) < 0.01
    assert abs(data["fte"]["August 2025"] - 0.60) < 0.01


def test_api_project_details_people(main_client):
    # Only Smith,Jane works on 5120001
    data = main_client.get("/api/project-details?project=5120001").json
    names = [r["name"] for r in data["people"]]
    assert "Smith,Jane" in names
    assert "Jones,Bob" not in names


def test_api_project_details_person_effort(main_client):
    # Smith,Jane: 50% in July, 60% in August on 5120001
    data = main_client.get("/api/project-details?project=5120001").json
    jane = next(r for r in data["people"] if r["name"] == "Smith,Jane")
    assert abs(jane["July 2025"] - 50.0) < 0.1
    assert abs(jane["August 2025"] - 60.0) < 0.1


def test_api_project_details_person_group(main_client):
    data = main_client.get("/api/project-details?project=5120001").json
    jane = next(r for r in data["people"] if r["name"] == "Smith,Jane")
    assert jane["group"] == "Engineering"


def test_api_project_details_nonproject(main_client):
    # Jones,Bob is 100% Non-Project → FTE = 1.0 per month
    data = main_client.get("/api/project-details?project=Non-Project").json
    assert abs(data["fte"]["July 2025"] - 1.0) < 0.01
    bob = next(r for r in data["people"] if r["name"] == "Jones,Bob")
    assert abs(bob["July 2025"] - 100.0) < 0.1
