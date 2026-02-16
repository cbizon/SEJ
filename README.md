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
uv run sej-load IET_2_8_26_anon.tsv
```

This creates `data/sej.db`. The first load creates the database directly. Subsequent loads create a **branch** that can be reviewed and merged (see [Branching](#branching) below).

You can specify an explicit output path as a second argument:

```
uv run sej-load mydata.tsv path/to/custom.db
```

**Input file requirements:**

- Tab-separated values (TSV), exported from the effort spreadsheet
- Must include columns: `EMPLOYEE`, `Group`, `Project Id`, `Project Name`, `Fund Code`, `Source`, `Account`, `Cost Code 1`, `Cost Code 2`, `Cost Code 3`, `Program Code`, and one column per month (e.g. `July 2025`, `August 2025`, ...)
- Rows where `Project Id` is blank or `N/A` are recorded under the sentinel project code `Non-Project`

## Web App

A Flask web app displays the effort data in a spreadsheet-style table.

```bash
uv run sej-web
```

The database path argument is optional and defaults to `data/sej.db`. To use a different path:

```bash
uv run sej-web path/to/custom.db
```

Then open http://127.0.0.1:5000 in your browser.

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
uv run sej-branch create my-edits
```

This copies `data/sej.db` to `data/sej_branch_my-edits.db`. The web app picks up the branch automatically — just refresh.

### List branches

```bash
uv run sej-branch list
```

### Merge a branch

```bash
uv run sej-branch merge my-edits
```

This:
1. Computes a diff between the branch and main
2. Writes a change-log TSV (`data/merges/merge_my-edits_<timestamp>.tsv`)
3. Backs up the current main (`data/backups/sej_backup_<timestamp>.db`)
4. Replaces main with the branch
5. Records the merge in the audit log

### Delete a branch (without merging)

```bash
uv run sej-branch delete my-edits
```

### Revert to a previous main

Every merge creates a backup. To roll back:

```bash
# Revert to the most recent backup
uv run sej-branch revert

# Revert to a specific backup
uv run sej-branch revert data/backups/sej_backup_20260215_143000_123456.db
```

### List available backups

```bash
uv run sej-branch backups
```

Backups are listed most-recent-first.

### Clean up old backups

```bash
# Keep only the 5 most recent (default)
uv run sej-branch prune-backups

# Keep only the 2 most recent
uv run sej-branch prune-backups --keep 2
```

## Running Tests

```bash
uv run pytest --cov=sej
```
