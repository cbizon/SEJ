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

### Effort Table

The main page shows a spreadsheet-style table of effort allocations. The table supports filtering, sorting, and column show/hide. Rows for employees in **external groups** are excluded from the 100% totals check (see [Groups](#groups) below).

An **Export TSV** button downloads the current table contents as a TSV file, respecting any active filters and visible columns.

### Edit Mode (Branch)

When viewing a **branch** database, the app enters edit mode:

- A banner shows the active branch name
- Month cells become editable (click to type a new percentage)
- **Selection mode** lets you select multiple cells and apply a single value to all of them
- **Add allocation line** lets you create a new row for an employee/project combination
- **Add employee** opens a dialog to create a new employee with name, group, and salary
- **Add/Edit project** opens a dialog to create a new project or update an existing project's metadata (name, start/end dates, local PI, admin group, personnel budget)
- **Fix totals** automatically adjusts effort percentages so that each internal employee's monthly allocations sum to 100%
- Branch controls (create, merge, discard) are available directly in the UI

Edits on main are rejected — you must be on a branch to make changes.

## Groups

Groups can be flagged as **internal** or **external**. Internal groups are subject to the 100% monthly effort constraint; external groups are not. New groups can be added from the Add employee dialog on a branch.

## Employees

Each employee record stores:

- **Salary** — used to estimate personnel spending against project budgets
- **Start date / End date** — optional month-level date range; FTE calculations only count an employee during their active period

Employee salary and dates can be updated from the Add/Edit project dialog area or via the employee API on a branch.

## Projects and Budget Lines

The system uses a two-level hierarchy for organizing financial data:

### Budget Lines

**Budget lines** are the granular financial units that come from the accounting system. Each budget line represents a specific combination of:

- A unique budget line code (e.g., from the `Project Id` column in the input TSV)
- Accounting codes (fund code, source, account, cost codes, program code)
- Optional start/end dates
- Optional personnel budget amount

Budget lines are what employees actually charge their time against. Each allocation line in the database connects an employee to a specific budget line with specific accounting codes.

### Projects

**Projects** are an aggregation layer that groups related budget lines together. A project can contain one or more budget lines, allowing you to see the total effort and spending across all funding sources for a logical project.

Each project record stores optional metadata:

- **Start / End** — the project's active date range (month and year)
- **Local PI** — the primary investigator from within the organization
- **Admin Group** — the group responsible for administering the project
- **Personnel Budget** — total personnel spending budget across all budget lines (dollars); used to compute a remaining-budget chart in the Project Details report

**Initial Import:** When data is first loaded from a TSV file, the system creates a 1:1 mapping — one project per budget line. Each project is initially named after its budget line.

**Grouping Budget Lines:** After import, you can use the **Add/Edit project** dialog (on a branch) to group multiple related budget lines under a single project. For example, if you have budget lines "ProjectA-Year1", "ProjectA-Year2", and "ProjectA-Supplement", you can create or update a project called "Project A" and assign all three budget lines to it. This allows reports and budgets to roll up across all funding sources for that logical project.

## Reports

The web app includes several built-in reports, accessible from the **Reports** page (`/reports`):

| Report | Description |
|--------|-------------|
| Non-Project by Group | Average non-project effort percentage per group, by month, with a breakdown table below |
| Non-Project by Person | Per-person non-project effort percentages, by month |
| Group Details | For a selected group, shows each employee's effort across all projects by month |
| Project Details | For a selected project, shows project metadata, effort tables, a budget chart, and change history |

### Project Details

The Project Details report includes:

- **Project info panel** — shows local PI, admin group, date range, and personnel budget (when set)
- **Total FTE by Month** — total full-time equivalents allocated to the project each month
- **Individual Effort by Month** — per-person percentage allocations, filterable by name and group
- **Remaining Personnel Budget** — a line chart tracking how much of the personnel budget remains month by month (only shown when a budget is set)
- **Change History** — all allocation changes from past merges, grouped by merge event

## History

The **History** page (`/history`) shows an audit log of all merges, including when they happened and what changed. Each merge entry links to a downloadable TSV of the specific changes applied.

## Future Ideas

See [docs/todo.md](docs/todo.md) for planned enhancements and longer-term ideas.

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
