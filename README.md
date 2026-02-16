# SEJ

Keeping track of project effort levels.

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

## Loading the Database

The database is populated from a TSV file exported from the effort-tracking spreadsheet.

```
uv run sej-load data.tsv sej.db
```

The first load creates the database directly. Subsequent loads create a **branch** that can be reviewed and merged (see [Branching](#branching) below).

**Input file requirements:**

- Tab-separated values (TSV), exported from the effort spreadsheet
- Must include columns: `EMPLOYEE`, `Group`, `Project Id`, `Project Name`, `Fund Code`, `Source`, `Account`, `Cost Code 1`, `Cost Code 2`, `Cost Code 3`, `Program Code`, and one column per month (e.g. `July 2025`, `August 2025`, ...)
- Rows where `Project Id` is blank or `N/A` are recorded under the sentinel project code `Non-Project`

## Web App

A Flask web app displays the effort data in a spreadsheet-style table.

```bash
uv run sej-web sej.db
```

Then open http://127.0.0.1:5000 in your browser. The database path argument is optional and defaults to `IET_2_8_26_anon.db`.

When viewing a **branch** database, the app enters edit mode:

- A banner shows the active branch name
- Month cells become editable (click to type a new percentage)
- **Selection mode** lets you select multiple cells and apply a single value to all of them
- **Add allocation line** lets you create a new row for an employee/project combination

Edits on main are rejected — you must be on a branch to make changes.

## Branching

All changes go through branches. A branch is a full copy of the main database that you can edit freely, then merge back when ready. Every merge automatically backs up the previous main so you can revert if needed.

### Create a branch

```bash
uv run sej-branch create my-edits --from-db sej.db
```

This copies `sej.db` to `sej_branch_my-edits.db`. Open the branch in the web app to edit:

```bash
uv run sej-web sej_branch_my-edits.db
```

### List branches

```bash
uv run sej-branch list --db sej.db
```

### Merge a branch

```bash
uv run sej-branch merge my-edits --db sej.db
```

This:
1. Computes a diff between the branch and main
2. Writes a change-log TSV (`merge_my-edits_<timestamp>.tsv`)
3. Backs up the current main (`sej_backup_<timestamp>.db`)
4. Replaces main with the branch
5. Records the merge in the audit log

### Delete a branch (without merging)

```bash
uv run sej-branch delete my-edits --db sej.db
```

### Revert to a previous main

Every merge creates a backup. To roll back:

```bash
# Revert to the most recent backup
uv run sej-branch revert --db sej.db

# Revert to a specific backup
uv run sej-branch revert sej_backup_20260215_143000_123456.db --db sej.db
```

### List available backups

```bash
uv run sej-branch backups --db sej.db
```

Backups are listed most-recent-first.

### Clean up old backups

```bash
# Keep only the 5 most recent (default)
uv run sej-branch prune-backups --db sej.db

# Keep only the 2 most recent
uv run sej-branch prune-backups --keep 2 --db sej.db
```

## Running Tests

```bash
uv run pytest --cov=sej
```
