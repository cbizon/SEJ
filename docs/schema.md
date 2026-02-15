# Database Schema

## Overview

The database tracks employee effort allocations across projects on a monthly basis.
Each employee is assigned to one group and can have multiple allocation lines, each
representing a specific project/accounting code combination. Monthly effort percentages
are stored against those lines.

---

## Tables

### `groups`

Organizational teams or units.

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | integer | PK |
| `name` | text | UNIQUE NOT NULL |

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

## Entity Relationships

```
groups
  └── employees (many employees per group)
        └── allocation_lines (many lines per employee)
              ├── projects (many lines may reference the same project)
              └── efforts (one row per month per allocation line)
```
