import csv
import pytest
import tempfile
from pathlib import Path

from sej.importer import load_tsv, NON_PROJECT_CODE


HEADER = [
    "EMPLOYEE", "Group", "Fund Code", "Source", "Account",
    "Cost Code 1", "Cost Code 2", "Cost Code 3", "Program Code",
    "Project Id", "Project Name",
    "July 2025", "August 2025",
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

    projects = conn.execute(
        "SELECT project_code FROM projects WHERE project_code != ?", (NON_PROJECT_CODE,)
    ).fetchall()
    assert {r["project_code"] for r in projects} == {"5120001", "5120002"}

    efforts = conn.execute("SELECT * FROM efforts").fetchall()
    assert len(efforts) == 4  # 2 lines × 2 months


def test_na_project_becomes_non_project(tmp):
    tsv = _tsv(tmp)
    write_tsv(tsv, [
        ["Jones,Bob", "Ops", "20152", "12001", "512120", "", "", "", "VROPS", "N/A", "N/A", "100.00%", ""],
    ])
    load_tsv(tsv, _db(tmp))

    from sej.db import get_connection
    conn = get_connection(_db(tmp))
    line = conn.execute(
        """
        SELECT p.project_code FROM allocation_lines al
        JOIN projects p ON p.id = al.project_id
        """
    ).fetchone()
    assert line["project_code"] == NON_PROJECT_CODE


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
    project = conn.execute(
        "SELECT name FROM projects WHERE project_code = '5199999'"
    ).fetchone()
    assert project["name"] == "The Big Project"
