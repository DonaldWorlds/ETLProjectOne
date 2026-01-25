
### NBA Dataset ETL — Systems Thinking Project

#### Overview (Business First)

This project is a **systems‑thinking, back‑end data engineering project** built around the Kaggle dataset:

> Historical NBA Data and Player Box Scores (eoinamoore/historical-nba-data-and-player-box-scores) [github](https://github.com/nogibjj/Rmr62DataBricksEtl)

The goal is to implement a **production‑style ETL system** that behaves like a real analytics platform: it ingests, validates, audits, and reuses data over time instead of just running a one‑off script. [neptune](https://neptune.ai/blog/build-etl-data-pipeline-in-ml)

The system emphasizes:

- Deterministic batch processing
- Auditability and recoverability
- Separation of concerns (ingestion vs comparison vs logging)
- Long‑lived, self‑managing runs instead of ad‑hoc notebooks

***

### What Problem This Project Solves

Real systems depend on **continuously updating external data sources** that are large, noisy, and occasionally corrected after the fact. This project answers operational questions such as: [neptune](https://neptune.ai/blog/build-etl-data-pipeline-in-ml)

- Has the source data changed since the last run?
- Exactly which files changed (hash or row count)?
- When did they change?
- Can we prove what we loaded and why?
- Can we safely re‑run without corrupting the baseline?

The NBA dataset stands in for feeds like:

- Sportsbook / odds feeds
- Market data exports
- Vendor CSV drops (S3, SFTP, email)
- Any recurring external CSV delivery

***

### System‑Level Architecture (Macro View)

This is a **batch‑first ETL system** with versioned snapshots and explicit metadata.

```text
Kaggle Dataset (External Source)
        ↓
Cron / CLI Trigger
        ↓
Download into data/temp/vN_nbadataset_temp_data/
        ↓
compare_and_decide (hash + row_count comparison)
        ↓
Decision: ingest / skip / alert
        ↓
On ingest:
  - Save hashes + row_counts → metadata/
  - Append run details → ingestion_log.md
  - Delete oldest temp version
On skip:
  - Log the run, keep current temp version
```

Key ideas:

- No silent overwrites of raw snapshots under `data/temp/`
- Append‑only, human‑readable log (`metadata/ingestion_log.md`)
- Explicit state (`metadata/hashes/`, `metadata/row_counts/`)
- Every run is explainable from metadata alone

***

### Core Concepts

#### 1. Batch Thinking

Each run is a **batch** with:

- Input: one versioned folder under `data/temp/` (for example `v10_nbadataset_temp_data/`)
- Output: a decision (`ingest`, `skip`, or `alert`) plus updated metadata
- A logged Markdown section with a per‑file comparison table

#### 2. Cron as Heartbeat (or Manual CLI)

The main entrypoint is:

```bash
poetry run python -m src.etl_project_package.main
```

`cron_run(project_root)`:

- Picks the next version name (vN)
- Downloads the Kaggle dataset into `data/temp/vN_nbadataset_temp_data/`
- Calls `compare_and_decide(...)`
- Logs the decision
- On `ingest`, saves state + deletes the oldest complete version

You can wire this to cron, e.g. every 6 hours:

```bash
0 */6 * * * cd /path/to/ETL-ProjectOne && poetry run python -m src.etl_project_package.main
```

#### 3. Smart Ingestion (Change Detection)

On each run, the system:

- Reads previous hashes + row counts from `metadata/hashes/` and `metadata/row_counts/`
- Computes current hashes + row counts from the new temp folder
- Compares previous vs current per file
- Applies clear rules:

- `ingest`  
  - First‑ever run (no previous state), or  
  - At least one file’s content changed (hash mismatch) or row count changed safely
- `skip`  
  - All 7 core CSVs have identical hashes and row counts to the baseline
- `alert`  
  - Missing or unexpected files, schema differences, or suspicious conditions

On `ingest`:

- `save_current_state(...)` writes 2 metadata files per CSV:
  - `metadata/hashes/<file>.md5`: timestamp + hash
  - `metadata/row_counts/<file>.rows`: total rows + data rows
- `cleanup_temp_on_ingest(...)` deletes the oldest complete `data/temp/v*/` folder, keeping only recent versions.

On `skip`:

- No metadata is changed.
- The new version folder stays in `data/temp/` as an auditable copy.
- The log records that all files were identical to the baseline.

***

### Tooling

- Python — core logic and orchestration
- Kaggle API — dataset download + auth [kaggle](https://www.kaggle.com/docs/api)
- dotenv — to manage `KAGGLE_USERNAME` / `KAGGLE_KEY`
- Standard library (hashes, paths, logging) for reliability

(Polars / Postgres / BigQuery are planned but not yet wired in.)

***

### Repository Structure

```text
ETL-ProjectOne/
├── src/
│   └── etl_project_package/
│       ├── main.py            # cron_run entrypoint
│       ├── compare.py         # compare_and_decide, state logic
│       ├── kaggle_connect.py  # Kaggle download + auth helpers
│       └── ...                # helpers (versioning, logging)
├── data/
│   └── temp/                  # Versioned raw Kaggle snapshots (vN_nbadataset_temp_data/)
├── metadata/
│   ├── hashes/                # Per-file hash baseline (.md5)
│   ├── row_counts/            # Per-file row count baseline (.rows)
│   └── ingestion_log.md       # Markdown audit log per run
├── .env                       # KAGGLE_USERNAME / KAGGLE_KEY
├── pyproject.toml             # Poetry config
└── README.md
```

***

### Current Status (What’s Implemented)

- Reliable Kaggle download with:
  - Auth via `.env` (`KAGGLE_USERNAME`, `KAGGLE_KEY`)
  - Retry + basic validation (must find 7 CSVs)
- Change detection:
  - Per‑file MD5 hashes
  - Per‑file row counts
  - Optional schema comparison
- Decision engine:
  - `compare_and_decide(project_root, version, data_path)` returns `(decision, reason, details)`
- Persistence:
  - `save_current_state(...)` writes hashes + row counts into `metadata/`
  - `read_previous_state(...)` reloads them on the next run
- Logging:
  - `log_decision(...)` appends a Markdown section for every run with:
    - Summary (decision, reason, timestamp, version)
    - Per‑file comparison table (previous vs current hash/rows)
- Temp lifecycle:
  - On `ingest`, delete the oldest complete version folder
  - On `skip`, keep the new folder (for audit) but do not update metadata

***

### To‑Do / Roadmap

Short‑term:
- cron job
- Add automated tests for:
  - `next_local_version_name`
  - `compare_and_decide` scenarios (baseline / skip / ingest / alert)
  - `read_previous_*` and `save_current_state`
- Add a simple CLI flag / config to control how many versions to retain under `data/temp/`.

Medium‑term:

- Package a Docker image for the ETL process
- Add Airflow or another orchestrator instead of cron
- Add a warehouse target (BigQuery) and model NBA facts/dimensions

Long‑term:

- FastAPI service for:
  - Triggering runs
  - Viewing latest decision/log
  - Health checks
- Use this dataset as a reliable source for downstream analytics / ML.

***


