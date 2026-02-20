import csv
import pytest
import tempfile
from pathlib import Path

from sej.importer import load_tsv, load_tsv_as_branch, augment_sample_data, NON_PROJECT_CODE


HEADER = [
    "EMPLOYEE", "Group", "Fund Code", "Source", "Account",
    "Cost Code 1", "Cost Code 2", "Cost Code 3", "Program Code",
    "Project Id", "Project Name",
    "July 2025", "August 2025",
]

# Extended header covering all 12 months for augmentation tests
HEADER_FULL = [
    "EMPLOYEE", "Group", "Fund Code", "Source", "Account",
    "Cost Code 1", "Cost Code 2", "Cost Code 3", "Program Code",
    "Project Id", "Project Name",
    "July 2025", "August 2025", "September 2025", "October 2025",
    "November 2025", "December 2025", "January 2026", "February 2026",
    "March 2026", "April 2026", "May 2026", "June 2026",
]


def write_tsv(path: Path, rows: list[list[str]]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(HEADER)
        writer.writerows(rows)


@pytest.fixture
def tmp(tmp_path):
    return tmp_path


def _db(tmp):
    return tmp / "test_anon.db"


def _tsv(tmp):
    return tmp / "data_anon.tsv"


def test_accepts_any_filename(tmp):
    p = tmp / "data.tsv"
    write_tsv(p, [])
    load_tsv(p, _db(tmp))  # should not raise


def test_basic_import(tmp):
    tsv = _tsv(tmp)
    write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120", "", "", "", "VRENG", "5120001", "Widget Project", "50.00%", "60.00%"],
        ["",           "Engineering", "25210", "49000", "511120", "", "", "", "",      "5120002", "Gadget Project", "50.00%", "40.00%"],
    ])
    load_tsv(tsv, _db(tmp))

    from sej.db import get_connection
    conn = get_connection(_db(tmp))

    employees = conn.execute("SELECT * FROM employees").fetchall()
    assert len(employees) == 1
    assert employees[0]["name"] == "Smith,Jane"

    budget_lines = conn.execute(
        "SELECT budget_line_code FROM budget_lines WHERE budget_line_code != ?", (NON_PROJECT_CODE,)
    ).fetchall()
    assert {r["budget_line_code"] for r in budget_lines} == {"5120001", "5120002"}

    efforts = conn.execute("SELECT * FROM efforts").fetchall()
    assert len(efforts) == 4  # 2 lines × 2 months


def test_na_project_becomes_imputed_budget_line(tmp):
    tsv = _tsv(tmp)
    write_tsv(tsv, [
        ["Jones,Bob", "Ops", "20152", "12001", "512120", "", "", "", "VROPS", "N/A", "N/A", "100.00%", ""],
    ])
    load_tsv(tsv, _db(tmp))

    from sej.db import get_connection
    conn = get_connection(_db(tmp))
    line = conn.execute(
        """
        SELECT bl.budget_line_code, bl.project_id FROM allocation_lines al
        JOIN budget_lines bl ON bl.id = al.budget_line_id
        """
    ).fetchone()
    # Should construct imputed code from accounting fields
    assert line["budget_line_code"] == "I:20152:12001:512120::::VROPS"

    # Should belong to the Non-Project project
    np_project = conn.execute(
        "SELECT id FROM projects WHERE is_nonproject = 1"
    ).fetchone()
    assert line["project_id"] == np_project["id"]


def test_blank_effort_cells_skipped(tmp):
    tsv = _tsv(tmp)
    write_tsv(tsv, [
        ["Lee,Carol", "Analytics", "25210", "49000", "511120", "", "", "", "", "5130001", "Some Project", "25.00%", ""],
    ])
    load_tsv(tsv, _db(tmp))

    from sej.db import get_connection
    conn = get_connection(_db(tmp))
    efforts = conn.execute("SELECT * FROM efforts").fetchall()
    assert len(efforts) == 1
    assert efforts[0]["month"] == 7


def test_wipe_and_reload(tmp):
    tsv = _tsv(tmp)
    write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120", "", "", "", "", "5120001", "Widget Project", "50.00%", "50.00%"],
    ])
    load_tsv(tsv, _db(tmp))

    # Second import with different data
    write_tsv(tsv, [
        ["Brown,Tom", "Research", "", "", "", "", "", "", "VRRES", "N/A", "N/A", "100.00%", "100.00%"],
    ])
    load_tsv(tsv, _db(tmp))

    from sej.db import get_connection
    conn = get_connection(_db(tmp))
    employees = conn.execute("SELECT name FROM employees").fetchall()
    assert len(employees) == 1
    assert employees[0]["name"] == "Brown,Tom"


def test_project_name_populated(tmp):
    tsv = _tsv(tmp)
    write_tsv(tsv, [
        ["Adams,Eve", "Data", "25210", "49000", "511120", "", "", "", "", "5199999", "The Big Project", "40.00%", "40.00%"],
        ["",          "Data", "25210", "49000", "511120", "", "", "", "", "5199999", "The Big Project", "60.00%", "60.00%"],
    ])
    load_tsv(tsv, _db(tmp))

    from sej.db import get_connection
    conn = get_connection(_db(tmp))
    bl = conn.execute(
        "SELECT name FROM budget_lines WHERE budget_line_code = '5199999'"
    ).fetchone()
    assert bl["name"] == "The Big Project"


def test_load_logs_audit(tmp):
    tsv = _tsv(tmp)
    write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project", "50.00%", "60.00%"],
    ])
    load_tsv(tsv, _db(tmp))

    from sej.db import get_connection
    conn = get_connection(_db(tmp))
    log = conn.execute("SELECT * FROM audit_log WHERE action='load'").fetchone()
    conn.close()
    assert log is not None


def test_duplicate_project_name_reuses_project(tmp):
    """Two budget lines with the same project name share a single project record."""
    tsv = _tsv(tmp)
    write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120", "", "", "", "", "5120001", "Shared Project", "50.00%", ""],
        ["",           "Engineering", "25210", "49000", "511120", "", "", "", "", "5120002", "Shared Project", "50.00%", ""],
    ])
    load_tsv(tsv, _db(tmp))

    from sej.db import get_connection
    conn = get_connection(_db(tmp))
    projects = conn.execute(
        "SELECT id FROM projects WHERE name = 'Shared Project'"
    ).fetchall()
    assert len(projects) == 1

    bls = conn.execute(
        "SELECT project_id FROM budget_lines WHERE budget_line_code IN ('5120001', '5120002')"
    ).fetchall()
    assert len({r["project_id"] for r in bls}) == 1
    conn.close()


def test_load_as_branch_bootstrap(tmp):
    """First load goes directly into main when DB doesn't exist."""
    tsv = _tsv(tmp)
    db = _db(tmp)
    write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project", "50.00%", "60.00%"],
    ])
    result = load_tsv_as_branch(tsv, db)
    assert result == db
    assert db.exists()

    from sej.db import get_connection
    conn = get_connection(db)
    employees = conn.execute("SELECT * FROM employees").fetchall()
    assert len(employees) == 1


def test_load_as_branch_creates_change_set(tmp):
    """Subsequent load creates a change_set and reloads in the same DB."""
    tsv = _tsv(tmp)
    db = _db(tmp)
    write_tsv(tsv, [
        ["Smith,Jane", "Engineering", "25210", "49000", "511120",
         "", "", "", "VRENG", "5120001", "Widget Project", "50.00%", "60.00%"],
    ])
    load_tsv(tsv, db)  # bootstrap

    # Now do a second load via change_set
    write_tsv(tsv, [
        ["Jones,Bob", "Ops", "20152", "12001", "512120",
         "", "", "", "VROPS", "N/A", "N/A", "100.00%", "100.00%"],
    ])
    result = load_tsv_as_branch(tsv, db)
    assert result == db

    # DB should have the new data (load_tsv wipes and reloads)
    from sej.db import get_connection
    conn = get_connection(db)
    employees = conn.execute("SELECT name FROM employees").fetchall()
    assert len(employees) == 1
    assert employees[0]["name"] == "Jones,Bob"

    # A change_set should be open
    from sej.changelog import get_open_change_set
    assert get_open_change_set(conn) is not None
    conn.close()


# --- Augmentation tests ---

def write_tsv_full(path: Path, rows: list[list[str]]) -> None:
    """Write a TSV with the full 12-month header."""
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(HEADER_FULL)
        writer.writerows(rows)


def _make_augment_db(tmp):
    """Create a DB with two projects (one with 2 employees, one with 1) spanning all 12 months."""
    tsv = tmp / "augment_anon.tsv"
    db = tmp / "augment_anon.db"
    # Project A (5140001): 2 employees — should be picked as candidate
    # Project B (5140002): 1 employee
    pct = "10.00%"
    write_tsv_full(tsv, [
        # Employee 1, project A
        ["Alpha,Ann", "TeamA", "25210", "49000", "511120", "", "", "", "VRENG",
         "5140001", "Project Alpha",
         pct, pct, pct, pct, pct, pct, pct, pct, pct, pct, pct, pct],
        # Employee 2, project A (continuation row — blank employee = same employee)
        ["Beta,Bob", "TeamA", "25210", "49000", "511120", "", "", "", "VRENG",
         "5140001", "Project Alpha",
         pct, pct, pct, pct, pct, pct, pct, pct, pct, pct, pct, pct],
        # Employee 3, project B
        ["Gamma,Gail", "TeamB", "30100", "50000", "522220", "", "", "", "VROPS",
         "5140002", "Project Beta",
         "20.00%", "20.00%", "", "", "", "", "", "", "", "", "", ""],
    ])
    load_tsv(tsv, db)
    return db


def test_augment_picks_project_and_sets_metadata(tmp):
    db = _make_augment_db(tmp)
    result = augment_sample_data(db)
    assert result == "Project Alpha"

    from sej.db import get_connection
    conn = get_connection(db)
    proj = conn.execute("""
        SELECT * FROM projects
        WHERE is_nonproject = 0 AND start_year IS NOT NULL
    """).fetchone()
    assert proj is not None
    assert proj["start_year"] == 2025
    assert proj["start_month"] == 7
    assert proj["end_year"] == 2026
    assert proj["end_month"] == 6
    assert proj["local_pi_id"] is not None
    assert proj["admin_group_id"] is not None

    # The picked project should be the one with most employees (Project Alpha)
    assert proj["name"] == "Project Alpha"
    conn.close()


def test_augment_splits_budget_line_into_y1_y2(tmp):
    db = _make_augment_db(tmp)
    augment_sample_data(db)

    from sej.db import get_connection
    conn = get_connection(db)

    y1 = conn.execute(
        "SELECT * FROM budget_lines WHERE budget_line_code = '5140001'"
    ).fetchone()
    assert y1["start_year"] == 2025
    assert y1["start_month"] == 7
    assert y1["end_year"] == 2026
    assert y1["end_month"] == 2
    assert "Y1" in y1["display_name"]

    y2 = conn.execute("""
        SELECT * FROM budget_lines
        WHERE project_id = ? AND budget_line_code != '5140001'
    """, (y1["project_id"],)).fetchone()
    assert y2 is not None
    assert y2["budget_line_code"].isdigit()
    assert len(y2["budget_line_code"]) == 7
    assert y2["start_year"] == 2026
    assert y2["start_month"] == 3
    assert y2["end_year"] == 2026
    assert y2["end_month"] == 6
    assert "Y2" in y2["display_name"]

    # Both belong to the same project
    assert y1["project_id"] == y2["project_id"]
    conn.close()


def test_augment_efforts_split_correctly(tmp):
    db = _make_augment_db(tmp)
    augment_sample_data(db)

    from sej.db import get_connection
    conn = get_connection(db)

    y1_bl = conn.execute(
        "SELECT id FROM budget_lines WHERE budget_line_code = '5140001'"
    ).fetchone()
    y2_bl = conn.execute("""
        SELECT id FROM budget_lines
        WHERE project_id = (
            SELECT project_id FROM budget_lines WHERE budget_line_code = '5140001'
        ) AND budget_line_code != '5140001'
    """).fetchone()

    # Y1 efforts: July 2025 through Feb 2026 (months 7-12 of 2025, 1-2 of 2026)
    y1_efforts = conn.execute("""
        SELECT e.year, e.month FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        WHERE al.budget_line_id = ?
        ORDER BY e.year, e.month
    """, (y1_bl["id"],)).fetchall()

    y1_months = [(r["year"], r["month"]) for r in y1_efforts]
    for ym in y1_months:
        # All Y1 efforts should be before March 2026
        assert ym < (2026, 3), f"Y1 has effort in {ym} which should be on Y2"

    # Y2 efforts: March 2026 through June 2026
    y2_efforts = conn.execute("""
        SELECT e.year, e.month FROM efforts e
        JOIN allocation_lines al ON al.id = e.allocation_line_id
        WHERE al.budget_line_id = ?
        ORDER BY e.year, e.month
    """, (y2_bl["id"],)).fetchall()

    y2_months = [(r["year"], r["month"]) for r in y2_efforts]
    for ym in y2_months:
        assert ym >= (2026, 3), f"Y2 has effort in {ym} which should be on Y1"

    # Should have efforts on both sides (2 employees × 8 months on Y1, 2 × 4 on Y2)
    assert len(y1_months) == 16  # 2 employees × 8 months (Jul-Feb)
    assert len(y2_months) == 8   # 2 employees × 4 months (Mar-Jun)
    conn.close()


def test_augment_sets_personnel_budgets(tmp):
    db = _make_augment_db(tmp)
    augment_sample_data(db)

    from sej.db import get_connection
    conn = get_connection(db)

    y1 = conn.execute(
        "SELECT personnel_budget FROM budget_lines WHERE budget_line_code = '5140001'"
    ).fetchone()
    y2 = conn.execute("""
        SELECT personnel_budget FROM budget_lines
        WHERE project_id = (
            SELECT project_id FROM budget_lines WHERE budget_line_code = '5140001'
        ) AND budget_line_code != '5140001'
    """).fetchone()

    assert y1["personnel_budget"] is not None
    assert y1["personnel_budget"] > 0
    assert y2["personnel_budget"] is not None
    assert y2["personnel_budget"] > 0
    conn.close()


def test_augment_noop_when_no_projects(tmp):
    """Augmentation gracefully does nothing if there are no non-sentinel projects."""
    tsv = tmp / "empty_anon.tsv"
    db = tmp / "empty_anon.db"
    write_tsv_full(tsv, [
        ["Solo,Sam", "TeamX", "20152", "12001", "512120", "", "", "", "VROPS",
         "N/A", "N/A", "100.00%", "", "", "", "", "", "", "", "", "", "", ""],
    ])
    load_tsv(tsv, db)
    assert augment_sample_data(db) is None  # should not raise, returns None
