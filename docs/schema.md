# Database Schema

## Overview

The database tracks employee effort allocations across projects on a monthly basis.
Each employee is assigned to one group and can have multiple allocation lines, each
representing a specific project/accounting code combination. Monthly effort percentages
are stored against those lines.

---

## Tables

### `groups`

Organizational teams or units. Groups loaded from the TSV are always internal.
Externally-created groups (people from partner organizations) are marked as
external so their effort is not expected to sum to 100%.

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | integer | PK |
| `name` | text | UNIQUE NOT NULL |
| `is_internal` | integer | NOT NULL DEFAULT 1 — 1 = internal, 0 = external |

---

### `employees`

People whose effort is being tracked. Each employee belongs to exactly one group.

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | integer | PK |
| `name` | text | UNIQUE NOT NULL |
| `group_id` | integer | FK → groups.id, NOT NULL |

---

### `projects`

Projects that employees can allocate effort to. The special sentinel record
`project_code = "Non-Project"` represents overhead or administrative time that
is charged to a program code rather than a specific project (sourced from rows
where the original data has `Project Id = N/A`).

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | integer | PK |
| `project_code` | text | UNIQUE NOT NULL |
| `name` | text | |
| `start_year` | integer | optional — must be paired with `start_month` |
| `start_month` | integer | optional, 1–12 — must be paired with `start_year` |
| `end_year` | integer | optional — must be paired with `end_month` |
| `end_month` | integer | optional, 1–12 — must be paired with `end_year` |
| `local_pi_id` | integer | FK → employees.id, optional — must be an internal employee |
| `personnel_budget` | real | optional |
| `admin_group_id` | integer | FK → groups.id, optional — may be internal or external |

---

### `allocation_lines`

One row per employee × project × accounting code combination. Accounting codes
are stored here (not on `projects`) because the same project can be charged under
different fund codes, accounts, and cost codes by different employees or groups.

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | integer | PK |
| `employee_id` | integer | FK → employees.id, NOT NULL |
| `project_id` | integer | FK → projects.id, NOT NULL |
| `fund_code` | text | |
| `source` | text | |
| `account` | text | |
| `cost_code_1` | text | |
| `cost_code_2` | text | |
| `cost_code_3` | text | |
| `program_code` | text | e.g. "VRCOM", "VRCYB" |

---

### `efforts`

Monthly effort percentages for each allocation line. Values are expressed as
percentages (0.0–100.0). An employee's efforts across all their allocation lines
should sum to approximately 100% for any given month.

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | integer | PK |
| `allocation_line_id` | integer | FK → allocation_lines.id, NOT NULL |
| `year` | integer | NOT NULL |
| `month` | integer | NOT NULL, 1–12 |
| `percentage` | real | NOT NULL |

**Unique constraint:** `(allocation_line_id, year, month)`

---

### `_meta`

Key-value metadata about this database file. Used by the branching system to
track whether a database is the main copy or a branch.

| Column | Type | Constraints |
|--------|------|-------------|
| `key` | text | PK |
| `value` | text | |

Known keys:
- `db_role`: `"main"` or `"branch"`
- `branch_name`: name of the branch (null for main)
- `source_db`: path to the main DB this was branched from

---

### `audit_log`

Records significant operations: loads, branch creation, merges, and deletions.

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | integer | PK |
| `timestamp` | text | NOT NULL, ISO-8601 |
| `action` | text | NOT NULL — `load`, `branch_create`, `merge`, `branch_delete`, `revert` |
| `details` | text | JSON blob with context (branch name, TSV path, etc.) |

---

## Entity Relationships

```
groups
  └── employees (many employees per group)
        └── allocation_lines (many lines per employee)
              ├── projects (many lines may reference the same project)
              └── efforts (one row per month per allocation line)
```
