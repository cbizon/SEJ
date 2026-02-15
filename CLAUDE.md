# SEJ

## Goal

A simple app for keeping track of project effort levels

## Basic Setup

* github: This project has a github repo at https://github.com/cbizon/SEJ
* uv: we are using uv for package and environment management and an isolated environment
* tests: we are using pytest, and want to maintain high code coverage

### Environment Management - CRITICAL
**NEVER EVER INSTALL ANYTHING INTO SYSTEM LIBRARIES OR ANACONDA BASE ENVIRONMENT**
- ALWAYS use the isolated virtual environment at `.venv/`
- ALWAYS use `uv run` to execute commands, which automatically uses the isolated environment
- The virtual environment is sacred. System packages are not your garbage dump.

## Project Structure

- `sej/` — main package
- `tests/` — pytest tests
- `docs/` — design documentation

## Database

The database is SQLite. See `docs/schema.md` for the full schema.

The project code `"Non-Project"` is a sentinel value in the `projects` table representing overhead or administrative effort (rows where the source data has no project ID). All code that works with projects must handle this case.

## Input

The input data is a tsv file downloaded from a spreadsheet.  For development, we will always be using an anonymized version of the input file. CLAUDE may NOT read any non-anonymized input data.  Do not attempt to open an input file that does not contain "anon" as part of the file name.

Furthermore, do not check the test data into git ever.  

## ***RULES OF THE ROAD***

- Don't use mocks. They obscure problems

- Ask clarifying questions

- Don't make classes just to group code. It is non-pythonic and hard to test.

- Do not implement bandaids - treat the root cause of problems

- Don't use try/except as a way to hide problems.  It is often good just to let something fail and figure out why.

- Once we have a test, do not delete it without explicit permission.  

- Do not return made up results if an API fails.  Let it fail.

- When changing code, don't make duplicate functions - just change the function. We can always roll back changes if needed.

- Keep the directories clean, don't leave a bunch of junk laying around.

- When making pull requests, NEVER ever mention a `co-authored-by` or similar aspects. In particular, never mention the tool used to create the commit message or PR.

- Check git status before commits

