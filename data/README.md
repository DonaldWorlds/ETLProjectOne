data/

This directory contains **runtime data only**.

Nothing in this directory is committed to version control.

## Subdirectories

- raw/ Immutable source snapshots
- staging/ Cleaned and standardized data
- temp/ Temporary downloads
- archive/ Compressed historical snapshots

## Rules

- Raw data is never modified
- Staging data can be regenerated
- All data is reproducible via the ETL system