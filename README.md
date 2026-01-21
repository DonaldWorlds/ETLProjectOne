***

# NBA Dataset ETL — Systems Thinking Project

## Overview (Business First)

This project is a **systems‑thinking, back‑end data engineering project** built around the open Kaggle dataset:

> **Historical NBA Data and Player Box Scores** (eoinamoore/historical-nba-data-and-player-box-scores)

Goal: design and implement a **production‑style ETL system** that mirrors how real analytics platforms ingest, validate, audit, and serve data over time.

This project intentionally emphasizes:
- Deterministic batch processing
- Auditability and recoverability
- Separation of concerns (ingestion vs transformation vs analytics)
- Long‑lived systems over one‑off scripts

***

## What Problem This Project Solves

Real businesses rely on **continuously updating external data sources** that are:

- Large
- Imperfect or noisy
- Occasionally corrected retroactively
- Outside their control

This project is designed to answer core operational questions:

- Has the source data changed since the last run?
- What exactly changed?
- When did it change?
- Can we prove what we loaded and why?
- Can we safely re‑run or recover the pipeline without corrupting state?

The NBA dataset acts as a **stand‑in for real enterprise feeds**, such as:

- Sportsbook or odds feeds
- Financial market data
- E‑commerce transaction exports
- Vendor‑delivered CSV drops (SFTP, S3, email attachments, etc.)

***

## System‑Level Architecture (Macro View)

This is a **batch‑first ETL system** designed to later evolve into a dynamic platform.

```text
Kaggle Dataset (External Source)
        ↓
Smart Ingestion (Change Detection)
        ↓
Raw Zone (Immutable Snapshots under data/temp/)
        ↓
Staging / Cleaning (Standardization & Validation)
        ↓
Analytical Warehouse (BigQuery / Postgres - planned)
        ↓
Downstream Systems (APIs, ML, BI - planned)
```

Key architectural ideas:

- **No silent overwrites** of raw data
- **Append‑only facts** and logs
- **Explicit state and metadata** (hashes, row counts, versions)
- **Every run is explainable** via a human‑readable ingestion log

***

## Core Concepts

### 1. Batch Thinking

All processing happens in **discrete, repeatable batches** with:

- A defined input (a versioned snapshot under `data/temp/`)
- A defined output (decision + updated metadata)
- A recorded decision in `metadata/ingestion_log.md`

### 2. Cron as the Heartbeat

Cron does **one job**:

> Trigger the system on a schedule.

All intelligence lives in Python:

- Downloading the latest dataset from Kaggle
- Detecting whether data changed vs the previous baseline
- Deciding whether to ingest, skip, or alert
- Recording outcomes in an append‑only Markdown log

This converts scripts into a **living system** rather than a one‑off notebook.

### 3. Smart Ingestion (Change Detection)

Because the dataset is large, the system **never blindly reloads** it.

Instead, it:

- Establishes an initial baseline snapshot
- Computes file‑level hashes and row counts
- Stores metadata about prior runs under `metadata/`
- Compares **current** vs **previous** state

The change detection engine can return one of three decisions:

- `ingest` → new or corrected data detected; safe to proceed
- `skip` → no meaningful change; cheap exit
- `alert` → schema changes, missing files, or suspicious conditions

The system behaves like a real data platform:

- **Stateful**: remembers previous state via hashes & row counts
- **Auditable**: each run is logged in Markdown with comparison details
- **Recoverable**: re‑runs don’t corrupt baselines
- **Extensible**: later phases can add APIs, ML, and BI downstream

### 4. Future Phases (Planned)

Built intentionally as a foundation for:

- FastAPI for manual triggers and run observability
- Dimensional modeling for analytics in a warehouse
- ML systems consuming cleaned, conformed data

***

## Tooling Philosophy (Why These Choices)

- **Python** — control plane and business logic
- **Polars** — fast, memory‑efficient dataframe operations
- **Postgres / BigQuery** — analytical storage (planned targets)
- **Cron** — deterministic scheduling and simple orchestration
- **Metadata files and tables** — the system’s “memory” (hashes, row counts, logs)
***

## Repository Structure

```text
ETL-ProjectOne/
├── src/                     # Python package: etl_project_package (core ETL logic)
│   └── etl_project_package/
│       ├── compare.py       # compare_and_decide, state comparison logic
│       ├── main.py          # cron_run entrypoint
│       ├── kaggle_connect.py# Kaggle download + dotenv auth
│       └── ...              # other helpers (versioning, logging)
├── data/
│   └── temp/                # Versioned raw snapshots (e.g. v1_nbadataset_temp_data/)
├── metadata/
│   ├── hashes/              # Per‑file hash baselines
│   ├── row_counts/          # Per‑file row count baselines
│   └── ingestion_log.md     # Append‑only Markdown audit log
├── tests/                   # Pytest suite for functions and pipeline
├── .env                     # Kaggle credentials (KAGGLE_USERNAME, KAGGLE_KEY)
├── pyproject.toml           # Poetry configuration
└── README.md                # This document
```

***

## Project Status

### What is Implemented

- **Kaggle Connectivity**
  - Downloading the NBA dataset using the Kaggle Python API
  - Authentication via `python-dotenv` and `.env` (no `kaggle.json` required)

- **Change Detection Core**
  - `compare_and_decide(project_root, version, data_path)`:
    - Computes current file hashes, row counts, and schema
    - Reads previous state from `metadata/`
    - Applies rules to decide `ingest` / `skip` / `alert`
    - Produces a rich `comparison_details` dictionary

- **Logging and Audit Trail**
  - `log_decision(decision, reason, comparison_details, project_root, version)`:
    - Appends a human‑readable Markdown section to `metadata/ingestion_log.md`
    - Includes file‑level comparison tables and run metadata
    │   └── last_hash.txt Store hash for comparison


- **Manual Testing**
  - Manual runs via:
    - `poetry run python -m etl_project_package.main`
  - Verified:
    - First run creates baseline and ingests
    - Subsequent runs detect changes and write log entries

***

## What Still Needs Improvement / To‑Do

### Data & Version Management

- **Cron‑style Entrypoint**
  - `cron_run(project_root)`:
    - Generates a new version name (e.g. `v2_nbadataset_temp_data`)
    - Downloads the dataset into `data/temp/<version>/`
    - Runs `compare_and_decide`
    - Logs the decision
    - Optionally cleans up older snapshot folders

- **Persist Previous State Correctly**
  - Ensure `compare_and_decide` writes baseline hashes and row counts into `metadata/` after an ingest.
  - Verify `read_previous_state` correctly reloads this metadata on the next run.

- **Version Folder Lifecycle**
  - Finalize a clear policy for:
    - When to delete old `data/temp/v*/` folders
    - How many complete versions to retain
    - How to handle incomplete or failed downloads

### Error Handling & Robustness

- **Kaggle Download Reliability**
  - Harden `download_kaggle_dataset_to` with:
    - Retries on transient failures
    - Validation that expected CSV files exist (e.g. `Games.csv`, `Players.csv`, etc.)
    - Clean removal of partially downloaded version folders on failure

- **Compare & Ingest Logic**
  - Refine rules so that:
    - Purely expected seasonal updates can optionally be treated as `skip`
    - Case differences or harmless file renames don’t cause false `ingest` results
  - Add duplicate/unchanged snapshot detection to avoid unnecessary ingests.

### Testing & Observability

- **Automated Tests**
  - Unit tests for:
    - `next_local_version_name`
    - `compare_and_decide` in different scenarios (baseline, no change, missing files, schema changes)
    - `log_decision` (structure of Markdown output)
  - Integration test:
    - End‑to‑end `cron_run` using temporary directories and mocked Kaggle downloads.
    - Data Snapshot update

- **Developer Experience**
  - Add example commands and expected output to help new contributors run:
    - `poetry install`
    - `poetry run pytest`
    - `poetry run python -m etl_project_package.main`

***

## Later in the Process (Roadmap)

- **Dockerization**
  - Containerize the entire ETL system for reproducible deployments.

- **Warehouse Modeling**
  - Design and build dimensional or star schemas in Postgres/BigQuery using the ingested NBA data.

- **APIs and Dynamic Orchestration**
  - Introduce FastAPI or similar to:
    - Trigger runs on demand
    - Expose status and recent decisions
    - Integrate with external schedulers beyond cron.

- **ML & Analytics**
  - Use the cleaned and versioned dataset as a source for:
    - Predictive models
    - Interactive dashboards
    - Historical analyses that rely on trustworthy, repeatable ETL.

***
