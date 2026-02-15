import csv
import sqlite3
import sys
from pathlib import Path

import pytest

from sej.importer import load_tsv
from sej.app import create_app, main


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


def test_index_returns_200(client):
    resp = client.get("/")
    assert resp.status_code == 200


def test_index_contains_column_headers(client):
    html = client.get("/").data.decode()
    for header in ["Employee", "Group", "Fund Code", "Project Id",
                    "Project Name", "July 2025", "August 2025"]:
        assert header in html


def test_index_contains_employee_names(client):
    html = client.get("/").data.decode()
    assert "Smith,Jane" in html
    assert "Jones,Bob" in html


def test_index_contains_percentages(client):
    html = client.get("/").data.decode()
    assert "50.00%" in html
    assert "100.00%" in html


def test_non_project_shows_na(client):
    html = client.get("/").data.decode()
    assert "N/A" in html


def test_continuation_row_blanks_employee(client):
    """The second allocation line for Smith,Jane should not repeat the name."""
    html = client.get("/").data.decode()
    # Smith,Jane should appear exactly once in the table body
    # (the header row doesn't contain it, so count in the whole page)
    assert html.count("Smith,Jane") == 1


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
