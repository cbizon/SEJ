# SEJ

Keeping track of project effort levels.

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

## Loading the Database

The database is populated from a TSV file exported from the effort-tracking spreadsheet.

```python
from sej.importer import load_tsv

load_tsv("path/to/data_anon.tsv", "sej.db")
```

This wipes any existing data and reloads from the given file. The database file is created if it does not exist.

**Input file requirements:**

- Tab-separated values (TSV), exported from the effort spreadsheet
- Must include columns: `EMPLOYEE`, `Group`, `Project Id`, `Project Name`, `Fund Code`, `Source`, `Account`, `Cost Code 1`, `Cost Code 2`, `Cost Code 3`, `Program Code`, and one column per month (e.g. `July 2025`, `August 2025`, …)
- Rows where `Project Id` is blank or `N/A` are recorded under the sentinel project code `Non-Project`

## Web App

A Flask web app displays the effort data in a spreadsheet-style table.

```bash
uv run sej-web sej.db
```

Then open http://127.0.0.1:5000 in your browser. The database path argument is optional and defaults to `IET_2_8_26_anon.db`.

## Running Tests

```bash
uv run pytest --cov=sej
```
