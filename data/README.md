***
## Updated `data/` README

### `data/` Directory



- `data/temp/` — versioned raw Kaggle snapshots created per run:
  - `v8_nbadataset_temp_data/`
  - `v9_nbadataset_temp_data/`
  - `v10_nbadataset_temp_data/`
- Older versions are removed automatically on `ingest` by `cleanup_temp_on_ingest(...)`.

Rules:

- Raw snapshots in `data/temp/` are treated as immutable.
- The same Kaggle dataset downloaded on different days may appear as multiple versions; the ETL compares them using hashes and row counts rather than trusting filenames or timestamps.
- Everything in `data/` is reproducible by rerunning the ETL with the same source dataset.

(Namespaces like `raw/`, `staging/`, `archive/` are planned for later phases; for now only `temp/` is used.)

***